using Sir.Core;
using Sir.Document;
using System;
using System.Collections.Generic;

namespace Sir.Search
{
    /// <summary>
    /// Write session targeting a single collection.
    /// </summary>
    public class WriteSession : IDisposable
    {
        private readonly ulong _collectionId;
        private readonly DocumentWriter _streamWriter;
        private readonly ProducerConsumerQueue<(IDictionary<string, object> document, HashSet<string> storedFieldNames)> _queue;
        private readonly SessionFactory _sessionFactory;

        public int QueueCount { get { return _queue.Count; } }

        public WriteSession(
            ulong collectionId,
            DocumentWriter streamWriter,
            SessionFactory sessionFactory)
        {
            _collectionId = collectionId;
            _streamWriter = streamWriter;
            _queue = new ProducerConsumerQueue<(IDictionary<string, object>, HashSet<string>)>(1, DoWrite);
            _sessionFactory = sessionFactory;
        }

        public void Put(IDictionary<string, object> document, HashSet<string> storedFieldNames)
        {
            document["created"] = DateTime.Now.ToBinary();
            document["collectionid"] = _collectionId;

            var docId = _streamWriter.GetNextDocId();

            document["___docid"] = docId;

            foreach (var key in document.Keys)
            {
                _streamWriter.EnsureKeyExists(key);
            }

            _queue.Enqueue((document, storedFieldNames));
        }

        private void DoWrite((IDictionary<string, object> document, HashSet<string> storedFieldNames) work)
        {
            var docMap = new List<(long keyId, long valId)>();

            foreach (var key in work.document.Keys)
            {
                if (key != "collectionid" && !work.storedFieldNames.Contains(key))
                {
                    continue;
                }

                var val = work.document[key];

                if (val == null)
                {
                    continue;
                }

                // store k/v
                var keyId = _sessionFactory.GetKeyId(_collectionId, key.ToHash());
                var kvmap = _streamWriter.Put(keyId, val, out _);

                // store refs to k/v pair
                docMap.Add(kvmap);
            }

            var docMeta = _streamWriter.PutDocumentMap(docMap);

            _streamWriter.PutDocumentAddress((long)work.document["___docid"], docMeta.offset, docMeta.length);
        }

        public void Dispose()
        {
            _queue.Dispose();
            _streamWriter.Dispose();
        }
    }
}