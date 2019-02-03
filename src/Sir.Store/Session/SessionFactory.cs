﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq;

namespace Sir.Store
{
    /// <summary>
    /// Dispatcher of sessions.
    /// </summary>
    public class SessionFactory : IDisposable, ILogger
    {
        private readonly ITokenizer _tokenizer;
        private readonly IConfigurationProvider _config;
        private readonly ConcurrentDictionary<ulong, long> _keys;

        private Stream _writableKeyMapStream { get; }

        public string Dir { get; }

        public SessionFactory(string dir, ITokenizer tokenizer, IConfigurationProvider config)
        {
            Dir = dir;
            _keys = LoadKeyMap();
            _tokenizer = tokenizer;
            _config = config;
            _writableKeyMapStream = CreateAppendStream(Path.Combine(dir, "_.kmap"));
        }

        private ConcurrentDictionary<ulong, long> LoadKeyMap()
        {
            var timer = new Stopwatch();
            timer.Start();

            var keys = new ConcurrentDictionary<ulong, long>();

            using (var stream = new FileStream(
                Path.Combine(Dir, "_.kmap"), FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite))
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

            this.Log("loaded keys into memory in {0}", timer.Elapsed);

            return keys;
        }

        public void PersistKeyMapping(ulong keyHash, long keyId)
        {
            if (!_keys.ContainsKey(keyHash))
            {
                _keys.GetOrAdd(keyHash, keyId);

                _writableKeyMapStream.Write(BitConverter.GetBytes(keyHash), 0, sizeof(ulong));

                _writableKeyMapStream.Flush();
            }
        }

        public long GetKeyId(ulong keyHash)
        {
            return _keys[keyHash];
        }

        public bool TryGetKeyId(ulong keyHash, out long keyId)
        {
            if (!_keys.TryGetValue(keyHash, out keyId))
            {
                keyId = -1;
                return false;
            }
            return true;
        }

        private readonly object _syncMMF = new object();

        public MemoryMappedFile CreateMMF(string fileName, string mapName)
        {
            MemoryMappedFile mmf;

            lock (_syncMMF)
            {
                try
                {
                    mmf = MemoryMappedFile.OpenExisting(mapName, MemoryMappedFileRights.Read);
                }
                catch (FileNotFoundException)
                {
                    try
                    {
                        mmf = MemoryMappedFile.CreateFromFile(fileName, FileMode.Open, mapName, 0, MemoryMappedFileAccess.Read);
                    }
                    catch (IOException)
                    {
                        mmf = MemoryMappedFile.OpenExisting(mapName, MemoryMappedFileRights.Read);
                    }
                }
            }

            return mmf;
        }

        public OptimizeSession CreateOptimizeSession(string collectionName, ulong collectionId)
        {
            return new OptimizeSession(collectionName, collectionId, this, _config);
        }

        public DocumentStreamSession CreateDocumentStreamSession(string collectionName, ulong collectionId)
        {
            return new DocumentStreamSession(collectionName, collectionId, this);
        }

        public WriteSession CreateWriteSession(string collectionName, ulong collectionId)
        {
            return new WriteSession(collectionName, collectionId, this);
        }

        public IndexSession CreateIndexSession(string collectionName, ulong collectionId)
        {
            return new IndexSession(collectionName, collectionId, this, _tokenizer, _config);
        }

        public BOWWriteSession CreateBOWSession(string collectionName, ulong collectionId)
        {
            return new BOWWriteSession(collectionName, collectionId, this, _config, _tokenizer);
        }

        public ValidateSession CreateValidateSession(string collectionName, ulong collectionId)
        {
            return new ValidateSession(collectionName, collectionId, this, _tokenizer, _config);
        }

        public ReadSession CreateReadSession(string collectionName, ulong collectionId)
        {
            return new ReadSession(collectionName, collectionId, this, _config);
        }

        public BOWReadSession CreateBOWReadSession(string collectionName, ulong collectionId)
        {
            return new BOWReadSession(collectionName, collectionId, this, _config);
        }

        public Stream CreateAsyncReadStream(string fileName)
        {
            return new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, 4096, true);
        }

        public Stream CreateReadStream(string fileName)
        {
            return new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        }

        public Stream CreateAsyncAppendStream(string fileName)
        {
            return new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite, 4096, true);
        }

        public Stream CreateAppendStream(string fileName)
        {
            // https://stackoverflow.com/questions/122362/how-to-empty-flush-windows-read-disk-cache-in-c
            //const FileOptions FileFlagNoBuffering = (FileOptions)0x20000000;
            //FileStream file = new FileStream(fileName, fileMode, fileAccess, fileShare, blockSize,
            //    FileFlagNoBuffering | FileOptions.WriteThrough | fileOptions);

            return new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite);
        }

        public void Dispose()
        {
            _writableKeyMapStream.Dispose();
        }
    }
}