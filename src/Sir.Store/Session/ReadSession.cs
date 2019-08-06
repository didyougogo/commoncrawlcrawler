﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;

namespace Sir.Store
{
    /// <summary>
    /// Read session targeting a single collection.
    /// </summary>
    public class ReadSession : CollectionSession, ILogger, IDisposable
    {
        private readonly IConfigurationProvider _config;
        private readonly IStringModel _model;
        private readonly Stream _postings;
        private readonly PostingsReader _postingsReader;
        private readonly DocumentStreamReader _streamReader;
        private readonly ConcurrentDictionary<long, NodeReader> _nodeReaders;

        public ReadSession(ulong collectionId,
            SessionFactory sessionFactory, 
            IConfigurationProvider config,
            IStringModel model,
            DocumentStreamReader streamReader) 
            : base(collectionId, sessionFactory)
        {
            _config = config;
            _model = model;
            _streamReader = streamReader;

            var posFileName = Path.Combine(SessionFactory.Dir, $"{CollectionId}.pos");

            _postings = SessionFactory.CreateReadStream(posFileName);
            _postingsReader = new PostingsReader(_postings);
            _nodeReaders = new ConcurrentDictionary<long, NodeReader>();
        }

        public void Dispose()
        {
            if (_postings != null)
                _postings.Dispose();

            _streamReader.Dispose();

            foreach (var reader in _nodeReaders.Values)
                reader.Dispose();
        }
        public ReadResult Read(Query query)
        {
            this.Log("begin read session for query {0}", query);

            if (SessionFactory.CollectionExists(query.Collection))
            {
                var result = MapReduce(query);

                if (result != null)
                {
                    var docs = ReadDocs(result.SortedDocuments);

                    this.Log("end read session for query {0}", query);

                    return new ReadResult { Total = result.Total, Docs = docs };
                }
            }

            this.Log("zero results for query {0}", query);

            return new ReadResult { Total = 0, Docs = new IDictionary<string, object>[0] };
        }

        private ScoredResult MapReduce(Query query)
        {
            Map(query);

            var timer = Stopwatch.StartNew();

            var result = new Dictionary<long, double>();

            _postingsReader.Reduce(query.ToClauses(), result);

            this.Log("reducing took {0}", timer.Elapsed);

            timer.Restart();

            var sorted = Sort(result, query.Skip, query.Take);

            this.Log("sorting took {0}", timer.Elapsed);

            return sorted;
        }


        private static ScoredResult Sort(IDictionary<long, double> documents, int skip, int take)
        {
            var sortedByScore = new List<KeyValuePair<long, double>>(documents);

            sortedByScore.Sort(
                delegate (KeyValuePair<long, double> pair1,
                KeyValuePair<long, double> pair2)
                {
                    return pair2.Value.CompareTo(pair1.Value);
                }
            );

            var index = skip > 0 ? skip : 0;
            var count = Math.Min(sortedByScore.Count, take > 0 ? take : sortedByScore.Count);

            return new ScoredResult { SortedDocuments = sortedByScore.GetRange(index, count), Total = sortedByScore.Count };
        }

        /// <summary>
        /// Map query terms to index IDs.
        /// </summary>
        /// <param name="query">An un-mapped query</param>
        public void Map(Query query)
        {
            var timer = Stopwatch.StartNew();
            var clauses = query.ToClauses();

            //Parallel.ForEach(clauses, q =>
            foreach (var q in clauses)
            {
                var cursor = q;

                while (cursor != null)
                {
                    Hit hit = null;

                    var indexReader = cursor.Term.KeyId.HasValue ?
                        CreateIndexReader(cursor.Term.KeyId.Value) :
                        CreateIndexReader(cursor.Term.KeyHash);

                    if (indexReader != null)
                    {
                        hit = indexReader.ClosestMatch(cursor.Term.Vector, _model);
                    }

                    if (hit != null && hit.Score > 0)
                    {
                        cursor.Score = hit.Score;

                        foreach (var offs in hit.Node.PostingsOffsets)
                        {
                            cursor.PostingsOffsets.Add(offs);
                        }
                    }

                    cursor = cursor.NextTermInClause;
                }
            }//);

            this.Log("mapping {0} took {1}", query, timer.Elapsed);
        }

        public NodeReader CreateIndexReader(long keyId)
        {
            var ixFileName = Path.Combine(SessionFactory.Dir, string.Format("{0}.{1}.ix", CollectionId, keyId));

            if (!File.Exists(ixFileName))
                return null;

            return _nodeReaders.GetOrAdd(keyId, new NodeReader(CollectionId, keyId, SessionFactory, _config));
        }

        public NodeReader CreateIndexReader(ulong keyHash)
        {
            long keyId;
            if (!SessionFactory.TryGetKeyId(CollectionId, keyHash, out keyId))
            {
                return null;
            }

            return CreateIndexReader(keyId);
        }

        public IList<IDictionary<string, object>> ReadDocs(IEnumerable<KeyValuePair<long, double>> docs)
        {
            var result = new List<IDictionary<string, object>>();

            foreach (var d in docs)
            {
                var docInfo = _streamReader.GetDocumentAddress(d.Key);

                if (docInfo.offset < 0)
                {
                    continue;
                }

                var docMap = _streamReader.GetDocumentMap(docInfo.offset, docInfo.length);
                var doc = new Dictionary<string, object>();

                for (int i = 0; i < docMap.Count; i++)
                {
                    var kvp = docMap[i];
                    var kInfo = _streamReader.GetAddressOfKey(kvp.keyId);
                    var vInfo = _streamReader.GetAddressOfValue(kvp.valId);
                    var key = _streamReader.GetKey(kInfo.offset, kInfo.len, kInfo.dataType);
                    var val = _streamReader.GetValue(vInfo.offset, vInfo.len, vInfo.dataType);

                    doc[key.ToString()] = val;
                }

                doc["___docid"] = d.Key;
                doc["___score"] = d.Value;

                result.Add(doc);
            }

            return result;
        }
    }
}
