using Microsoft.Extensions.Logging;
using Sir.Core;
using Sir.Document;
using Sir.KeyValue;
using Sir.VectorSpace;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace Sir.Search
{
    /// <summary>
    /// Dispatcher of sessions.
    /// </summary>
    public class SessionFactory : IDisposable, ISessionFactory
    {
        private readonly ConcurrentDictionary<string, MemoryMappedFile> _mmfs;

        private ConcurrentDictionary<ulong, ulong> _collectionAliases;
        private ConcurrentDictionary<ulong, ConcurrentDictionary<ulong, long>> _keys;
        private Memory<double> _lexicon;

        private readonly ILogger _logger;

        public string Dir { get; }
        public IConfigurationProvider Config { get; }
        public IStringModel Model { get; }

        public SessionFactory(IConfigurationProvider config, IStringModel model, ILogger logger)
        {
            var time = Stopwatch.StartNew();

            Dir = config.Get("data_dir");
            Config = config;
            Model = model;

            if (!Directory.Exists(Dir))
            {
                Directory.CreateDirectory(Dir);
            }

            _mmfs = new ConcurrentDictionary<string, MemoryMappedFile>();
            _logger = logger;
            _keys = LoadKeys();

            _logger.LogInformation($"loaded keys in {time.Elapsed}");

            time.Restart();

            _collectionAliases = LoadCollectionAliases();

            _logger.LogInformation($"loaded aliases in {time.Elapsed}");

            time.Restart();

            _lexicon = LoadLexicon();

            _logger.LogInformation($"loaded lexicon ({_lexicon.Length/sizeof(double)}) in {time.Elapsed}");

            _logger.LogInformation($"initiated");
        }

        public IEnumerable<IVector> CreateDocumentEmbeddings(
            IEnumerable<IVector> tokens,
            IStringModel model)
        {
            var bufIndex = 0;
            var buf = new double[2];
            //var debug = new string[2];
            var count = 0;

            foreach (var token in tokens)
            {
                var angle = model.CosAngle(model.SortingVector, token);

                if (bufIndex == 2)
                {
                    yield return new IndexedVector(
                        new double[2] { buf[0], angle },
                        model.VectorWidth);

                    //_logger.LogInformation($"{debug[0]}{token.Data}");

                    buf[0] = buf[1];
                    buf[1] = angle;

                    //debug[0] = debug[1];
                    //debug[1] = token.Data.ToString();
                }
                else
                {
                    buf[bufIndex] = angle;

                    //debug[bufIndex] = token.Data.ToString();

                    bufIndex++;
                }

                count++;
            }

            if (count == 2) 
            {
                yield return new IndexedVector(
                        new double[2] { buf[0], 0 },
                        model.VectorWidth);

                yield return new IndexedVector(
                        new double[2] { buf[1], 0 },
                        model.VectorWidth);

                //_logger.LogInformation($"{debug[0]}{token.Data}");
            }
            else if (count == 1)
            {
                yield return new IndexedVector(
                        new double[2] { buf[0], 0 },
                        model.VectorWidth);

                //_logger.LogInformation($"{debug[0]}{token.Data}");
            }
        }

        public MemoryMappedFile OpenMMF(string fileName)
        {
            var mapName = fileName.Replace(":", "").Replace("\\", "_");

            try
            {
                return _mmfs.GetOrAdd(mapName, x =>
                {
                    return MemoryMappedFile.CreateFromFile(fileName, FileMode.Open, mapName, 0, MemoryMappedFileAccess.ReadWrite);
                });
            }
            catch
            {
                return _mmfs.GetOrAdd(mapName, x =>
                {
                    return MemoryMappedFile.OpenExisting(mapName);
                });
            }
        }

        public long GetDocCount(string collection)
        {
            var fileName = Path.Combine(Dir, $"{collection.ToHash()}.dix");

            if (!File.Exists(fileName))
                return 0;

            return new FileInfo(fileName).Length / (sizeof(long) + sizeof(int));
        }

        public void Truncate(ulong collectionId)
        {
            if ("lexicon".ToHash() == collectionId)
            {
                _lexicon = new double[0];
            }

            var count = 0;

            foreach (var file in Directory.GetFiles(Dir, $"{collectionId}*"))
            {
                File.Delete(file);
                count++;
            }

            _keys.Clear();

            _logger.LogInformation($"truncated collection {collectionId} ({count} files)");
        }

        public void TruncateIndex(ulong collectionId)
        {
            var count = 0;

            foreach (var file in Directory.GetFiles(Dir, $"{collectionId}*.ix"))
            {
                File.Delete(file);
                count++;
            }
            foreach (var file in Directory.GetFiles(Dir, $"{collectionId}*.ixp"))
            {
                File.Delete(file);
                count++;
            }
            foreach (var file in Directory.GetFiles(Dir, $"{collectionId}*.vec"))
            {
                File.Delete(file);
                count++;
            }
            foreach (var file in Directory.GetFiles(Dir, $"{collectionId}*.pos"))
            {
                File.Delete(file);
                count++;
            }

            _logger.LogInformation($"truncated index {collectionId} ({count} files)");
        }

        private Memory<double> LoadLexicon()
        {
            var collectionId = "lexicon".ToHash();
            var ixFileName = Path.Combine(Dir, $"{collectionId}.ix");

            if (File.Exists(ixFileName))
            {
                using (var ixStream = OpenReadStream(Path.Combine(Dir, $"{collectionId}.ix")))
                using (var vecStream = OpenReadStream(Path.Combine(Dir, $"{collectionId}.vec")))
                {
                    return GraphBuilder.DeserializeSortedList(ixStream, vecStream, Model);
                }
            }

            return new double[0];
        }

        public void Train(Job job, int reportSize)
        {
            var time = Stopwatch.StartNew();

            using (var trainSession = CreateTrainLexiconSession())
            using (var queue = new ProducerConsumerQueue<IDictionary<string, object>>(
                1,
                doc =>
                {
                    trainSession.Put(doc, job.IndexedFieldNames);
                }))
            {
                var t = Stopwatch.StartNew();
                var batchNo = 0;

                foreach (var batch in job.Documents.Batch(reportSize))
                {
                    var count = 0;

                    foreach (var doc in batch)
                    {
                        queue.Enqueue(doc);

                        count++;
                    }

                    var e = t.Elapsed.TotalMilliseconds;
                    var docsPerSecond = (int)(count / e * 1000);

                    _logger.LogInformation($"enqueued batch {++batchNo} in {t.Elapsed}. queue count {queue.Count}. reading from disk at {docsPerSecond} docs/s");

                    t.Restart();
                }
            }

            _logger.LogInformation($"training ({job.CollectionId}) took {time.Elapsed}");
        }

        public IEnumerable<IDictionary<string, object>> WriteOnly(Job job, WriteSession writeSession)
        {
            //var time = Stopwatch.StartNew();
            var docCount = 0;

            foreach (var document in job.Documents)
            {
                writeSession.Write(document, job.StoredFieldNames);

                docCount++;

                yield return document;
            }

            //_logger.LogInformation($"writing {docCount} documents took {time.Elapsed}.");
        }

        public void IndexOnly(
            IDictionary<string, object> document, 
            IIndexSession indexSession, 
            HashSet<string> indexedFieldNames)
        {
            var docId = (long)document["___docid"];
            var collectionId = (ulong)document["collectionid"];

            Parallel.ForEach(document, kv =>
            //foreach (var kv in document)
            {
                if (indexedFieldNames.Contains(kv.Key) && kv.Value != null)
                {
                    var keyId = GetKeyId(collectionId, kv.Key.ToHash());

                    indexSession.Put(docId, keyId, (string)kv.Value);
                }
            });
        }

        public void IndexOnly(
            IEnumerable<IDictionary<string, object>> documents, 
            IIndexSession indexSession, 
            HashSet<string> indexedFieldNames)
        {
            var time = Stopwatch.StartNew();
            var docCount = 0;

            foreach (var document in documents)
            {
                IndexOnly(document, indexSession, indexedFieldNames);

                docCount++;
            }

            _logger.LogInformation($"indexing {docCount} documents took {time.Elapsed}.");
        }

        public IndexInfo Write(Job job, WriteSession writeSession, IIndexSession indexSession)
        {
            var docCount = 0;
            var time = Stopwatch.StartNew();

            //Parallel.ForEach(WriteOnly(job, writeSession), document=>
            foreach (var document in WriteOnly(job, writeSession))
            {
                IndexOnly(document, indexSession, job.IndexedFieldNames);

                docCount++;
            }//);

            _logger.LogInformation($"indexing {docCount} documents {job.CollectionId} took {time.Elapsed}.");

            return indexSession.GetIndexInfo();
        }

        public void Write(Job job, WriteSession writeSession, IIndexSession indexSession, int reportSize)
        {
            var time = Stopwatch.StartNew();
            var info = Write(job, writeSession, indexSession);
            var t = time.Elapsed.TotalMilliseconds;
            var docsPerSecond = (int)(reportSize / t * 1000);

            _logger.LogInformation($"{info}\n{docsPerSecond} docs/s\n");
        }

        public void Write(Job job, int reportSize)
        {
            var batchNo = 0;
            var time = Stopwatch.StartNew();

            using (var writeSession = CreateWriteSession(job.CollectionId))
            using (var indexSession = CreateIndexSession(job.CollectionId))
            {
                foreach (var batch in job.Documents.Batch(reportSize))
                {
                    Write(
                        new Job(
                            job.CollectionId,
                            batch,
                            job.Model,
                            job.StoredFieldNames,
                            job.IndexedFieldNames),
                        writeSession,
                        indexSession,
                        reportSize);

                    _logger.LogInformation($"processed batch {++batchNo}");
                }
            }

            _logger.LogInformation($"processed job ({job.CollectionId}), in total: {time.Elapsed}");
        }

        public void Write(Job job)
        {
            using (var writeSession = CreateWriteSession(job.CollectionId))
            using (var indexSession = CreateIndexSession(job.CollectionId))
            {
                Write(job, writeSession, indexSession);
            }
        }

        public void Write(Job job, IIndexSession indexSession)
        {
            using (var writeSession = CreateWriteSession(job.CollectionId))
            {
                Write(job, writeSession, indexSession);
            }
        }

        public void Write(
            IEnumerable<IDictionary<string, object>> documents, 
            IStringModel model, 
            HashSet<string> storedFieldNames,
            HashSet<string> indexedFieldNames
            )
        {
            Parallel.ForEach(documents.GroupBy(d => (string)d["___collectionid"]), group =>
            //foreach (var group in documents.GroupBy(d => (string)d["___collectionid"]))
            {
                var collectionId = group.Key.ToHash();

                using (var writeSession = CreateWriteSession(collectionId))
                using (var indexSession = CreateIndexSession(collectionId))
                {
                    Write(
                        new Job(
                            collectionId, 
                            group, 
                            model, 
                            storedFieldNames, 
                            indexedFieldNames), 
                        writeSession, 
                        indexSession);
                }
            });
        }

        public FileStream CreateLockFile(ulong collectionId)
        {
            return new FileStream(Path.Combine(Dir, collectionId + ".lock"),
                   FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None,
                   4096, FileOptions.RandomAccess | FileOptions.DeleteOnClose);
        }

        public void Refresh()
        {
            _keys = LoadKeys();
            _collectionAliases = LoadCollectionAliases();
            _lexicon = LoadLexicon();
        }

        public ConcurrentDictionary<ulong, ulong> LoadCollectionAliases()
        {
            var timer = Stopwatch.StartNew();
            var aliases = new Dictionary<ulong, ulong>();
            var fileName = Path.Combine(Dir, "aliases.cmap");

            using (var stream = new FileStream(fileName, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite))
            {
                Span<byte> buf = new byte[sizeof(ulong)*2];
                var read = stream.Read(buf);

                while (read > 0)
                {
                    var data = MemoryMarshal.Cast<byte, ulong>(buf);

                    aliases.Add(data[0], data[1]);

                    read = stream.Read(buf);
                }
            }

            _logger.LogInformation($"loaded {aliases.Count} collection ID -> original collection ID mappings into memory in {timer.Elapsed}");

            return new ConcurrentDictionary<ulong, ulong>(aliases);
        }

        public ConcurrentDictionary<ulong, ConcurrentDictionary<ulong, long>> LoadKeys()
        {
            var timer = Stopwatch.StartNew();
            var allkeys = new ConcurrentDictionary<ulong, ConcurrentDictionary<ulong, long>>();

            foreach (var keyFile in Directory.GetFiles(Dir, "*.kmap"))
            {
                var collectionId = ulong.Parse(Path.GetFileNameWithoutExtension(keyFile));
                ConcurrentDictionary<ulong, long> keys;

                if (!allkeys.TryGetValue(collectionId, out keys))
                {
                    keys = new ConcurrentDictionary<ulong, long>();
                    allkeys.GetOrAdd(collectionId, keys);
                }

                using (var stream = new FileStream(keyFile, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite))
                {
                    long i = 0;
                    var buf = new byte[sizeof(ulong)];
                    var read = stream.Read(buf, 0, buf.Length);

                    while (read > 0)
                    {
                        keys.GetOrAdd(BitConverter.ToUInt64(buf, 0), i++);

                        read = stream.Read(buf, 0, buf.Length);
                    }
                }
            }

            _logger.LogInformation($"loaded keyHash -> keyId mappings into memory for {allkeys.Count} collections in {timer.Elapsed}");

            return allkeys;
        }

        public void RegisterKeyMapping(ulong collectionId, ulong keyHash, long keyId)
        {
            var fileName = string.Format("{0}.kmap", collectionId);
            ConcurrentDictionary<ulong, long> keys;

            if (!_keys.TryGetValue(collectionId, out keys))
            {
                keys = new ConcurrentDictionary<ulong, long>();
                _keys.GetOrAdd(collectionId, keys);
            }

            if (!keys.ContainsKey(keyHash))
            {
                keys.GetOrAdd(keyHash, keyId);

                using (var stream = OpenAppendStream(fileName))
                {
                    stream.Write(BitConverter.GetBytes(keyHash), 0, sizeof(ulong));
                }
            }
        }

        public void RegisterCollectionAlias(ulong collectionId, ulong originalCollectionId)
        {
            if (!_collectionAliases.ContainsKey(collectionId))
            {
                _collectionAliases.GetOrAdd(collectionId, originalCollectionId);

                var fileName = "aliases.cmap";

                using (var stream = OpenAppendStream(fileName))
                {
                    Span<ulong> buf = new ulong[2];

                    buf[0] = collectionId;
                    buf[1] = originalCollectionId;

                    stream.Write(MemoryMarshal.Cast<ulong, byte>(buf));
                }

                var keyMapFileName = Path.Combine(Dir, $"{collectionId}.kmap");

                if (!File.Exists(keyMapFileName))
                {
                    var originalKeyMapFileName = Path.Combine(Dir, $"{originalCollectionId}.kmap");

                    File.Copy(originalKeyMapFileName, keyMapFileName);
                }
            }
        }

        public ulong GetCollectionReference(ulong collectionId)
        {
            ulong alias;

            if (!_collectionAliases.TryGetValue(collectionId, out alias))
            {
                return collectionId;
            }

            return alias;
        }

        public long GetKeyId(ulong collectionId, ulong keyHash)
        {
            return _keys[collectionId][keyHash];
        }

        public bool TryGetKeyId(ulong collectionId, ulong keyHash, out long keyId)
        {
            var keys = _keys.GetOrAdd(collectionId, new ConcurrentDictionary<ulong, long>());

            if (!keys.TryGetValue(keyHash, out keyId))
            {
                keyId = -1;
                return false;
            }
            return true;
        }

        public string GetKey(ulong collectionId, long keyId)
        {
            using (var indexReader = new ValueIndexReader(OpenReadStream(Path.Combine(Dir, $"{collectionId}.kix"))))
            using (var reader = new ValueReader(OpenReadStream(Path.Combine(Dir, $"{collectionId}.key"))))
            {
                var keyInfo = indexReader.Get(keyId);

                return (string)reader.Get(keyInfo.offset, keyInfo.len, keyInfo.dataType);
            }
        }

        public DocumentStreamSession CreateDocumentStreamSession(ulong collectionId)
        {
            return new DocumentStreamSession(new DocumentReader(collectionId, this));
        }

        public WriteSession CreateWriteSession(ulong collectionId)
        {
            var documentWriter = new DocumentWriter(collectionId, this);

            return new WriteSession(
                collectionId,
                documentWriter,
                this
            );
        }

        public IIndexSession CreateIndexSession(ulong collectionId)
        {
            return new IndexSession(
                collectionId, 
                this, 
                Model, 
                Config, 
                _logger);
        }

        public TrainSession CreateTrainLexiconSession()
        {
            return new TrainSession(
                "lexicon".ToHash(),
                this, 
                Model, 
                Config, 
                _logger,
                _lexicon);
        }

        public IReadSession CreateReadSession()
        {
            return new ReadSession(
                this,
                Config,
                Model,
                new PostingsReader(this),
                _logger);
        }

        public ValidateSession CreateValidateSession(ulong collectionId)
        {
            return new ValidateSession(
                collectionId,
                this,
                Model,
                Config,
                new PostingsReader(this));
        }

        public Stream OpenAsyncReadStream(string fileName, int bufferSize = 4096)
        {
            var abs = GetAbsolutePath(fileName);

            return File.Exists(abs)
            ? new FileStream(
                abs, 
                FileMode.Open, 
                FileAccess.Read, 
                FileShare.ReadWrite, 
                bufferSize, 
                FileOptions.Asynchronous)
            : null;
        }

        public Stream OpenReadStream(string fileName, int bufferSize = 4096, FileOptions fileOptions = FileOptions.RandomAccess)
        {
            var abs = GetAbsolutePath(fileName);

            return File.Exists(abs)
                ? new FileStream(
                    abs, 
                    FileMode.Open, 
                    FileAccess.Read, 
                    FileShare.ReadWrite, 
                    bufferSize, 
                    fileOptions)
                : null;
        }

        public Stream OpenAsyncAppendStream(string fileName, int bufferSize = 4096)
        {
            var abs = GetAbsolutePath(fileName);

            return new FileStream(
                abs, 
                FileMode.Append, 
                FileAccess.Write, 
                FileShare.ReadWrite, 
                bufferSize, 
                FileOptions.Asynchronous);
        }

        public Stream OpenTruncatedStream(string fileName, int bufferSize = 4096)
        {
            var abs = GetAbsolutePath(fileName);

            return new FileStream(abs, FileMode.Create, FileAccess.Write, FileShare.ReadWrite, bufferSize);
        }

        public Stream OpenAppendStream(string fileName, int bufferSize = 4096)
        {
            var abs = GetAbsolutePath(fileName);

            if (!File.Exists(abs))
            {
                using (var fs = new FileStream(
                    abs, 
                    FileMode.Append, 
                    FileAccess.Write, 
                    FileShare.ReadWrite, 
                    bufferSize)) {}
            }

            return new FileStream(abs, FileMode.Append, FileAccess.Write, FileShare.ReadWrite, bufferSize);
        }

        private string GetAbsolutePath(string fileName)
        {
            return Path.Combine(Dir, fileName);
        }

        public bool CollectionExists(ulong collectionId)
        {
            return File.Exists(Path.Combine(Dir, collectionId + ".vec"));
        }

        public bool CollectionIsIndexOnly(ulong collectionId)
        {
            if (!CollectionExists(collectionId))
                throw new InvalidOperationException($"{collectionId} dows not exist");

            return !File.Exists(Path.Combine(Dir, collectionId + ".docs"));
        }

        public void Dispose()
        {
            foreach (var file in _mmfs.Values)
                file.Dispose();
        }
    }
}