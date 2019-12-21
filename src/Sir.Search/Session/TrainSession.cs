using Microsoft.Extensions.Logging;
using Sir.VectorSpace;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Sir.Search
{
    /// <summary>
    /// Indexing session targeting a single collection.
    /// </summary>
    public class TrainSession : IDisposable
    {
        private readonly ulong _collectionId;
        private readonly SessionFactory _sessionFactory;
        private readonly IConfigurationProvider _config;
        private readonly Stream _vectorStream;
        private readonly IStringModel _model;
        private readonly ILogger _logger;
        private bool _flushed;
        private int _merges;

        public int Merges { get { return _merges; } }

        public ConcurrentDictionary<double, VectorNode> Space { get; }

        public TrainSession(
            ulong collectionId,
            SessionFactory sessionFactory,
            IStringModel model,
            IConfigurationProvider config,
            ILogger logger,
            SortedList<double, VectorNode> space)
        {
            _collectionId = collectionId;
            _sessionFactory = sessionFactory;
            _config = config;
            _vectorStream = sessionFactory.CreateAppendStream($"{_collectionId}.vec");
            _model = model;
            _logger = logger;
            Space = new ConcurrentDictionary<double, VectorNode>(space);
        }

        public void Put(string value)
        {
            var tokens = _model.Tokenize(value.ToCharArray());

            Parallel.ForEach(tokens, token =>
            //foreach (var token in tokens)
            {
                var angle = _model.CosAngle(_model.SortingVector, token);

                if (!Space.TryAdd(angle, new VectorNode(token)))
                {
                    Interlocked.Increment(ref _merges);
                }
            });
        }

        public void Flush()
        {
            if (_flushed)
                return;

            _flushed = true;

            var time = Stopwatch.StartNew();

            using (var indexStream = _sessionFactory.CreateAppendStream($"{_collectionId}.ix"))
            using (var columnWriter = new ColumnWriter(indexStream))
            using (var pageIndexWriter = new PageIndexWriter(_sessionFactory.CreateAppendStream($"{_collectionId}.ixtp")))
            {
                var sorted = new SortedList<double, VectorNode>(Space);

                _logger.LogInformation($"sorted angles in {time.Elapsed}");

                time.Restart();

                var size = columnWriter.CreateSortedPage(sorted, _vectorStream, pageIndexWriter);

                time.Stop();

                File.WriteAllText("train.log", $"{sorted.Count} sorted words\r\n{string.Join("\r\n", sorted)}");

                _logger.LogInformation($"serialized lexicon segment with size {size} in {time.Elapsed}");
            }
        }

        public void Dispose()
        {
            if (!_flushed)
                Flush();

            _vectorStream.Dispose();
        }
    }
}