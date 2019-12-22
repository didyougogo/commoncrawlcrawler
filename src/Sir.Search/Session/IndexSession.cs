using Microsoft.Extensions.Logging;
using Sir.VectorSpace;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading;

namespace Sir.Search
{
    /// <summary>
    /// Indexing session targeting a single collection.
    /// </summary>
    public class IndexSession : IDisposable, IIndexSession
    {
        private readonly ulong _collectionId;
        private readonly SessionFactory _sessionFactory;
        private readonly IConfigurationProvider _config;
        private readonly Stream _postingsStream;
        private readonly Stream _vectorStream;
        private bool _flushed;
        public IStringModel Model { get; }
        public ConcurrentDictionary<long, VectorNode> Index { get; }

        private long _merges;
        private readonly ILogger _logger;

        public IndexSession(
            ulong collectionId,
            SessionFactory sessionFactory,
            IStringModel model,
            IConfigurationProvider config,
            ILogger logger)
        {
            _collectionId = collectionId;
            _sessionFactory = sessionFactory;
            _config = config;
            _postingsStream = sessionFactory.OpenAppendStream($"{collectionId}.pos");
            _vectorStream = sessionFactory.OpenAppendStream($"{collectionId}.vec");
            Model = model;
            Index = new ConcurrentDictionary<long, VectorNode>();
            _logger = logger;
        }

        public void Put(long docId, long keyId, string value)
        {
            var tokens = Model.Tokenize(value.ToCharArray());
            var column = Index.GetOrAdd(keyId, new VectorNode());

            foreach (var token in tokens)
            {
                if (GraphBuilder.MergeOrAdd(
                    column,
                    new VectorNode(token, docId),
                    Model,
                    Model.FoldAngle,
                    Model.IdenticalAngle))
                {
                    _merges++;
                }
            }
        }

        public void Put(long docId, IVector vector, VectorNode column)
        {
            GraphBuilder.MergeOrAdd(
                column,
                new VectorNode(vector, docId),
                Model,
                Model.FoldAngle,
                Model.IdenticalAngle);
        }

        public IndexInfo GetIndexInfo()
        {
            return new IndexInfo(GetGraphInfo(), _merges);
        }

        private IEnumerable<GraphInfo> GetGraphInfo()
        {
            foreach (var ix in Index)
            {
                yield return new GraphInfo(ix.Key, ix.Value);
            }

            yield break;
        }

        public void Flush()
        {
            if (_flushed)
                return;

            _flushed = true;

            foreach (var column in Index)
            {
                using (var indexStream = _sessionFactory.OpenAppendStream($"{_collectionId}.{column.Key}.ix"))
                using (var columnWriter = new ColumnWriter(indexStream))
                using (var pageIndexWriter = new PageIndexWriter(_sessionFactory.OpenAppendStream($"{_collectionId}.{column.Key}.ixtp")))
                {
                    var size = columnWriter.CreatePage(column.Value, _vectorStream, _postingsStream, pageIndexWriter);

                    _logger.LogInformation($"serialized column {column.Key} weight {column.Value.Weight} {size}");
                }
            }
        }

        public void Dispose()
        {
            if (!_flushed)
                Flush();

            _postingsStream.Dispose();
            _vectorStream.Dispose();
        }
    }
}