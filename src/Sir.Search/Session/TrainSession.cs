using Microsoft.Extensions.Logging;
using Sir.Core;
using Sir.VectorSpace;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;

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
        private readonly IStringModel _model;
        private readonly ILogger _logger;
        private bool _flushed;
        private int _merges;
        private int _numOfDocs;

        public int Merges { get { return _merges; } }

        public ConcurrentDictionary<double, VectorNode> Space { get; }

        public TrainSession(
            ulong collectionId,
            SessionFactory sessionFactory,
            IStringModel model,
            IConfigurationProvider config,
            ILogger logger,
            Memory<double> space)
        {
            _collectionId = collectionId;
            _sessionFactory = sessionFactory;
            _config = config;
            _model = model;
            _logger = logger;

            Space = new ConcurrentDictionary<double, VectorNode>(
                space.ToArray().ToDictionary(x=>x, x=>(VectorNode)null));
        }

        public void Put(IEnumerable<IDictionary<string, object>> documents, HashSet<string> trainingFieldNames)
        {
            var count = 0;
            var time = Stopwatch.StartNew();

            foreach (var doc in documents)
            {
                Put(doc, trainingFieldNames);

                count++;
            }

            var t = time.Elapsed.TotalMilliseconds;
            var docsPerSecond = (int)(count / t * 1000);

            _numOfDocs += count;

            _logger.LogInformation($"processed {_numOfDocs} docs. {_merges} merges. weight {Space.Count}. {docsPerSecond} docs/s");
        }

        public void Put(IDictionary<string, object> document, HashSet<string> trainingFieldNames)
        {
            foreach (var kv in document)
            {
                if (trainingFieldNames.Contains(kv.Key) && kv.Value != null)
                {
                    var tokens = _model.Tokenize(((string)kv.Value).ToCharArray());

                    foreach (var token in tokens)
                    {
                        var angle = _model.CosAngle(_model.SortingVector, token);

                        if (!Space.TryAdd(angle, new VectorNode(token)))
                        {
                            _merges++;
                        }
                    }
                }
            }
        }

        public void Flush()
        {
            if (_flushed)
                return;

            _flushed = true;

            var time = Stopwatch.StartNew();

            using (var indexStream = _sessionFactory.OpenTruncatedStream($"{_collectionId}.ix"))
            using (var columnWriter = new ColumnWriter(indexStream))
            {
                var sorted = new SortedList<double, VectorNode>(Space);

                _logger.LogInformation($"sorted {sorted.Count} angles in {time.Elapsed}");

                time.Restart();

                var size = columnWriter.CreateSortedPage(sorted);

                _logger.LogInformation($"serialized segment with size {size} in {time.Elapsed}");


                time.Restart();
                File.WriteAllLines("train.log", sorted.Select(x=>x.ToString()));
                _logger.LogInformation($"wrote train.log in {time.Elapsed}");
            }
        }

        public void Dispose()
        {
            if (!_flushed)
            {
                Flush();
            }
        }
    }
}