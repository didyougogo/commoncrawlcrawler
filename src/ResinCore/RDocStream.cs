﻿using Resin.IO;
using Resin.IO.Read;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Resin
{
    public class RDocStream : DocumentStream, IDisposable
    {
        private readonly DocHashReader _hashReader;
        private readonly DocumentAddressReader _addressReader;
        private readonly DocumentReader _documentReader;
        private readonly IxInfo _ix;
        private readonly int _take;
        private readonly int _skip;
        private readonly string _directory;

        public RDocStream(string fileName, string primaryKeyFieldName = null, int skip = 0, int take = int.MaxValue) 
            : base(primaryKeyFieldName)
        {
            var versionId = Path.GetFileNameWithoutExtension(fileName);
            var directory = Path.GetDirectoryName(fileName);
            var docFileName = Path.Combine(directory, versionId + ".rdoc");
            var docAddressFn = Path.Combine(directory, versionId + ".da");
            var docHashesFileName = Path.Combine(directory, string.Format("{0}.{1}", versionId, "pk"));

            _ix = IxInfo.Load(Path.Combine(directory, versionId + ".ix"));
            _hashReader = new DocHashReader(docHashesFileName);
            _addressReader = new DocumentAddressReader(new FileStream(docAddressFn, FileMode.Open, FileAccess.Read));
            _documentReader = new DocumentReader(new FileStream(docFileName, FileMode.Open, FileAccess.Read), _ix.Compression);
            _skip = skip;
            _take = take;
            _directory = directory;
        }

        public void Dispose()
        {
            _hashReader.Dispose();
            _addressReader.Dispose();
            _documentReader.Dispose();
        }

        public override IEnumerable<Document> ReadSource()
        {
            return ReadSourceAndAssignPk(StreamDocuments());
        }

        private IEnumerable<Document> StreamDocuments()
        {
            var skipped = 0;
            var took = 0;

            for (int docId = 0; docId < _ix.DocumentCount; docId++)
            {
                var hash = _hashReader.Read(docId);

                var address = _addressReader.Read(new[]
                {
                    new BlockInfo(docId * Serializer.SizeOfBlock(), Serializer.SizeOfBlock())
                }).First();

                var document = _documentReader.Read(new List<BlockInfo> { address }).First();

                if (!hash.IsObsolete)
                {
                    if (skipped == _skip && took < _take)
                    {
                        yield return document;
                        took++;
                    }
                    else if (skipped < _skip)
                    {
                        skipped++;
                    }
                    else if (took == _take)
                    {
                        break;
                    }
                }
            }
        }
    }
}
