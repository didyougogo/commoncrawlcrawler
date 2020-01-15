using Microsoft.Extensions.Logging;
using Sir.VectorSpace;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace Sir.Search
{
    public class TrainSession : IDisposable
    {
        private readonly IStringModel _model;
        private readonly IConfigurationProvider _config;
        private readonly ulong _collectionId;
        private readonly SessionFactory _sessionFactory;
        private readonly ILogger _logger;
        private readonly ConcurrentDictionary<long, ConcurrentDictionary<double, VectorNode>> _index;
        private bool _flushed;
        private long _merges;

        public TrainSession(
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
            _index = new ConcurrentDictionary<long, ConcurrentDictionary<double, VectorNode>>();
            _logger = logger;
        }

        public void Put(long keyId, string value)
        {
            _flushed = false;

            var tokens = _model.Tokenize(value.ToCharArray());
            var column = _index.GetOrAdd(keyId, new ConcurrentDictionary<double, VectorNode>());

            foreach (var token in tokens)
            {
                var angle = _model.CosAngle(_model.SortingVector, token);
                var node = new VectorNode(token);

                if (!column.TryAdd(angle, node))
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

            var time = Stopwatch.StartNew();

            foreach (var column in _index)
            {
                using (var sortedListStream = _sessionFactory.OpenAppendStream($"{_collectionId}.{column.Key}.sl"))
                using (var indexStream = _sessionFactory.OpenAppendStream($"{_collectionId}.{column.Key}.uix"))
                using (var vectorStream = _sessionFactory.OpenAppendStream($"{_collectionId}.vec"))
                using (var sortedListPage = new PageIndexWriter(_sessionFactory.OpenAppendStream($"{_collectionId}.{column.Key}.slp")))
                using (var indexPage = new PageIndexWriter(_sessionFactory.OpenAppendStream($"{_collectionId}.{column.Key}.uixp")))
                using (var columnWriter = new ColumnWriter(sortedListStream, indexStream, sortedListPage, indexPage))
                {
                    var sorted = new SortedList<double, VectorNode>(column.Value);

                    _logger.LogInformation($"sorted {sorted.Count} angles in {time.Elapsed}");

                    time.Restart();

                    var size = columnWriter.CreatePage(
                        sorted,
                        vectorStream);

                    _logger.LogInformation($"serialized segment {size} in {time.Elapsed}");

#if DEBUG
                    File.WriteAllLines($"train.{column.Key}.log", sorted.Select(x => x.ToString()));
#endif
                }
            }

            _index.Clear();
            _merges = 0;
        }

        public void Dispose()
        {
            if (!_flushed)
                Flush();
        }
    }
}