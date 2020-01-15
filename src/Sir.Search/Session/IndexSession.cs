using Microsoft.Extensions.Logging;
using Sir.VectorSpace;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Sir.Search
{
    public class IndexSession : IDisposable
    {
        private readonly IStringModel _model;
        private readonly IConfigurationProvider _config;
        private readonly ulong _collectionId;
        private readonly SessionFactory _sessionFactory;
        private readonly ILogger _logger;
        private readonly ConcurrentDictionary<long, VectorNode> _index;
        private bool _flushed;
        private long _merges;
        private readonly Stream _postingsStream;
        private readonly Stream _vectorStream;

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
            _model = model;
            _index = new ConcurrentDictionary<long, VectorNode>();
            _logger = logger;
            _postingsStream = sessionFactory.OpenAppendStream($"{collectionId}.pos");
            _vectorStream = sessionFactory.OpenAppendStream($"{collectionId}.vec");
        }

        public void Put(long docId, long keyId, string value)
        {
            _flushed = false;

            var tokens = _model.Tokenize(value.ToCharArray());
            var column = _index.GetOrAdd(keyId, new VectorNode());

            foreach (var token in tokens)
            {
                var node = new VectorNode(token, docId);

                if (GraphBuilder.MergeOrAdd(
                    column,
                    node,
                    _model,
                    _model.FoldAngle,
                    _model.IdenticalAngle))
                {
                    _merges++;
                }
            }
        }

        public void Flush()
        {
            if (_flushed)
                return;

            _flushed = true;

            foreach (var column in _index)
            {
                using (var indexStream = _sessionFactory.OpenAppendStream($"{_collectionId}.{column.Key}.ix"))
                using (var pageIndexWriter = new PageIndexWriter(_sessionFactory.OpenAppendStream($"{_collectionId}.{column.Key}.ixtp")))
                using (var columnWriter = new ColumnWriter(indexStream, pageIndexWriter))
                {
                    var size = columnWriter.CreatePage(column.Value, _vectorStream, _postingsStream, pageIndexWriter);

                    _logger.LogInformation($"serialized column {column.Key} weight {column.Value.Weight} {size}");
                }
            }
        }

        public IndexInfo GetIndexInfo()
        {
            return new IndexInfo(GetGraphInfo(), _merges);
        }

        private IEnumerable<GraphInfo> GetGraphInfo()
        {
            foreach (var ix in _index)
            {
                yield return new GraphInfo(ix.Key, (int)ix.Value.Weight);
            }

            yield break;
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