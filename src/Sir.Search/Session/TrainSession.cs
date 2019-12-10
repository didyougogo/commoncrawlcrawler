using Microsoft.Extensions.Logging;
using Sir.VectorSpace;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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
        private readonly ILogger<TrainSession> _logger;
        private readonly VectorNode _lexicon;
        private bool _flushed;

        public VectorNode Lexicon => _lexicon;

        public TrainSession(
            SessionFactory sessionFactory,
            IStringModel model,
            VectorNode lexicon,
            IConfigurationProvider config,
            ILogger<TrainSession> logger)
        {
            _collectionId = "lexicon".ToHash();
            _sessionFactory = sessionFactory;
            _config = config;
            _postingsStream = sessionFactory.CreateAppendStream(Path.Combine(sessionFactory.Dir, $"{_collectionId}.pos"));
            _vectorStream = sessionFactory.CreateAppendStream(Path.Combine(sessionFactory.Dir, $"{_collectionId}.vec"));
            _model = model;
            _lexicon = lexicon;
            _logger = logger;
            _lexicon = new VectorNode();
        }

        public void Put(string value)
        {
            var tokens = _model.Tokenize(value).ToList();
            Parallel.ForEach(tokens, token =>
            //foreach (var token in tokens)
            {
                GraphBuilder.IncrementId(
                    _lexicon,
                    new VectorNode(token),
                    _model,
                    _model.FoldAngle,
                    _model.IdenticalAngle);
            });
        }


        public void Flush()
        {
            if (_flushed)
                return;

            _flushed = true;

            using (var indexStream = _sessionFactory.CreateAppendStream(Path.Combine(_sessionFactory.Dir, $"{_collectionId}.ix")))
            using (var columnWriter = new ColumnWriter(indexStream))
            using (var pageIndexWriter = new PageIndexWriter(_sessionFactory.CreateAppendStream(Path.Combine(_sessionFactory.Dir, $"{_collectionId}.ixtp"))))
            {
                var size = columnWriter.CreatePage(_lexicon, _vectorStream, _postingsStream, pageIndexWriter);

                _logger.LogInformation($"serialized lexicon segment with weight {_lexicon.Weight} and size {size}");
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