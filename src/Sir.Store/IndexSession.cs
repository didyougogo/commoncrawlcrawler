﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Sir.Core;

namespace Sir.Store
{
    /// <summary>
    /// Indexing session targeting a single collection.
    /// </summary>
    public class IndexSession : CollectionSession
    {
        private readonly ValueWriter _vals;
        private readonly ValueWriter _keys;
        private readonly DocWriter _docs;
        private readonly ValueIndexWriter _valIx;
        private readonly ValueIndexWriter _keyIx;
        private readonly DocIndexWriter _docIx;
        private readonly PagedPostingsWriter _postingsWriter;
        private readonly Stopwatch _timer;
        private readonly ProducerConsumerQueue<(long keyId, VectorNode index, IList<VectorNode> nodes)> _buildQueue;
        private readonly ITokenizer _tokenizer;
        private readonly StreamWriter _log;
        private readonly Dictionary<long, VectorNode> _dirty;
        private bool _completed;

        public IndexSession(
            ulong collectionId, 
            LocalStorageSessionFactory sessionFactory, 
            ITokenizer tokenizer) : base(collectionId, sessionFactory)
        {
            _tokenizer = tokenizer;
            _log = Logging.CreateWriter("session");
            _buildQueue = new ProducerConsumerQueue<(long keyId, VectorNode index, IList<VectorNode> nodes)>(Build);
            _dirty = new Dictionary<long, VectorNode>();

            ValueStream = sessionFactory.CreateAppendStream(Path.Combine(sessionFactory.Dir, string.Format("{0}.val", collectionId)));
            KeyStream = sessionFactory.CreateAppendStream(Path.Combine(sessionFactory.Dir, string.Format("{0}.key", collectionId)));
            DocStream = sessionFactory.CreateAppendStream(Path.Combine(sessionFactory.Dir, string.Format("{0}.docs", collectionId)));
            ValueIndexStream = sessionFactory.CreateAppendStream(Path.Combine(sessionFactory.Dir, string.Format("{0}.vix", collectionId)));
            KeyIndexStream = sessionFactory.CreateAppendStream(Path.Combine(sessionFactory.Dir, string.Format("{0}.kix", collectionId)));
            DocIndexStream = sessionFactory.CreateAppendStream(Path.Combine(sessionFactory.Dir, string.Format("{0}.dix", collectionId)));
            PostingsStream = sessionFactory.CreateReadWriteStream(Path.Combine(sessionFactory.Dir, string.Format("{0}.pos", collectionId)));
            VectorStream = sessionFactory.CreateAppendStream(Path.Combine(sessionFactory.Dir, string.Format("{0}.vec", collectionId)));
            Index = sessionFactory.GetCollectionIndex(collectionId);

            _vals = new ValueWriter(ValueStream);
            _keys = new ValueWriter(KeyStream);
            _docs = new DocWriter(DocStream);
            _valIx = new ValueIndexWriter(ValueIndexStream);
            _keyIx = new ValueIndexWriter(KeyIndexStream);
            _docIx = new DocIndexWriter(DocIndexStream);
            _postingsWriter = new PagedPostingsWriter(PostingsStream);
            _timer = new Stopwatch();
        }

        public void Write(AnalyzeJob job)
        {
            try
            {
                var timer = new Stopwatch();
                timer.Start();

                var docCount = 0;
                var columns = new Dictionary<long, HashSet<VectorNode>>();

                foreach(var doc in job.Documents)
                {
                    docCount++;

                    var docId = (ulong)doc["__docid"];

                    foreach (var obj in doc.Keys)
                    {
                        var key = (string)obj;

                        if (key.StartsWith("__"))
                            continue;

                        var keyHash = key.ToHash();
                        var keyId = SessionFactory.GetKeyId(keyHash);

                        HashSet<VectorNode> column;
                        if (!columns.TryGetValue(keyId, out column))
                        {
                            column = new HashSet<VectorNode>();
                            columns.Add(keyId, column);
                        }

                        var val = (IComparable)doc[obj];
                        var str = val as string;

                        if (str == null || key[0] == '_')
                        {
                            var v = val.ToString();

                            if (!string.IsNullOrWhiteSpace(v))
                            {
                                var node = new VectorNode(v, docId);
                                column.Add(node);
                            }
                        }
                        else
                        {
                            foreach (var token in _tokenizer.Tokenize(str))
                            {
                                if (!string.IsNullOrWhiteSpace(token))
                                {
                                    var node = new VectorNode(token, docId);
                                    column.Add(node);
                                }
                            }
                        }
                    }
                }

                _log.Log(string.Format("analyzed {0} docs in {1}", docCount, timer.Elapsed));

                timer.Restart();

                foreach (var column in columns)
                {
                    var keyId = column.Key;

                    VectorNode ix;
                    if (!_dirty.TryGetValue(keyId, out ix))
                    {
                        ix = GetIndex(keyId);

                        if (ix == null)
                        {
                            ix = new VectorNode();
                            SessionFactory.AddIndex(CollectionId, keyId, ix);
                        }
                        _dirty.Add(keyId, ix);
                    }
                }

                Parallel.ForEach(columns, column =>
                {
                    var keyId = column.Key;
                    var nodes = column.Value.ToArray();
                    var ix = _dirty[keyId];

                    Build(keyId, ix, nodes);
                });
            }
            catch (Exception ex)
            {
                _log.Log(ex);

                throw;
            }
        }

        private void Build((long keyId, VectorNode index, IList<VectorNode> nodes) job)
        {
            Build(job.keyId, job.index, job.nodes);
        }

        private void Build(long keyId, VectorNode index, IList<VectorNode> nodes)
        {
            var timer = new Stopwatch();
            timer.Start();

            foreach (var node in nodes)
            {
                index.Add(node);
            }

            _log.Log(string.Format("added {0} nodes to column {1}.{2} in {3}. {4}",
                nodes.Count, CollectionId, keyId, timer.Elapsed, index.Size()));
        }

        private void Serialize((long keyId, VectorNode node) job)
        {
            Serialize(job.keyId, job.node);
        }

        private void Serialize(long keyId, VectorNode node)
        {
            try
            {
                using (var ixFile = CreateIndexStream(keyId))
                {
                    node.SerializeTreeAndPayload(
                                            ixFile,
                                            VectorStream,
                                            _postingsWriter);
                }
                _log.Log(string.Format("serialized column {0}", keyId));
            }
            catch (Exception ex)
            {
                _log.Log(ex);

                throw;
            }
        }

        private Stream CreateIndexStream(long keyId)
        {
            var fileName = Path.Combine(SessionFactory.Dir, string.Format("{0}.{1}.ix", CollectionId, keyId));
            return new FileStream(fileName, FileMode.Create, FileAccess.Write, FileShare.None);
        }

        public void Serialize()
        {
            if (_completed)
                return;

            try
            {
                _buildQueue.Dispose();

                _log.Log("build queue completed.");

                foreach (var x in _dirty)
                {
                    Serialize(x.Key, x.Value);
                }

                _log.Log("serialization completed.");

                _completed = true;
            }
            catch (Exception ex)
            {
                _log.Log(ex);

                throw;
            }
        }

        public override void Dispose()
        {
            if (!_completed)
            {
                Serialize();
                _completed = true;
            }

            base.Dispose();
        }
    }
}