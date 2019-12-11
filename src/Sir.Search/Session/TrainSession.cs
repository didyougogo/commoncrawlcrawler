using Microsoft.Extensions.Logging;
using Sir.VectorSpace;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
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
        private readonly Stream _postingsStream;
        private readonly Stream _vectorStream;
        private readonly IStringModel _model;
        private readonly ILogger _logger;
        private bool _flushed;
        public int _merges;

        public int Merges { get { return _merges; } }

        public ConcurrentDictionary<double, VectorNode> Lexicon { get; }

        public TrainSession(
            SessionFactory sessionFactory,
            IStringModel model,
            IConfigurationProvider config,
            ILogger logger)
        {
            _collectionId = "lexicon".ToHash();
            _sessionFactory = sessionFactory;
            _config = config;
            _postingsStream = sessionFactory.CreateAppendStream(Path.Combine(sessionFactory.Dir, $"{_collectionId}.pos"));
            _vectorStream = sessionFactory.CreateAppendStream(Path.Combine(sessionFactory.Dir, $"{_collectionId}.vec"));
            _model = model;
            _logger = logger;
            Lexicon = new ConcurrentDictionary<double, VectorNode>();
        }

        public void Put(string value)
        {
            var tokens = _model.Tokenize(value.ToCharArray());

            Parallel.ForEach(tokens, token =>
            //foreach (var token in tokens)
            {
                var angle = _model.CosAngle(_model.SortingVector, token);

                if (!Lexicon.TryAdd(angle, new VectorNode(token)))
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

            using (var indexStream = _sessionFactory.CreateAppendStream(Path.Combine(_sessionFactory.Dir, $"{_collectionId}.ix")))
            using (var columnWriter = new ColumnWriter(indexStream))
            using (var pageIndexWriter = new PageIndexWriter(_sessionFactory.CreateAppendStream(Path.Combine(_sessionFactory.Dir, $"{_collectionId}.ixtp"))))
            {
                var sorted = new SortedList<double, VectorNode>(Lexicon);

                _logger.LogInformation($"sorted list of angles in {time.Elapsed}");

                time.Restart();

                var size = columnWriter.CreateSortedPage(sorted, _vectorStream, _postingsStream, pageIndexWriter);

                time.Stop();

                File.WriteAllText("train.log", $"{sorted.Count} sorted words\r\n{string.Join("\r\n", sorted)}");

                _logger.LogInformation($"serialized lexicon segment with node count {size.depth} in {time.Elapsed}");
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