using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;

namespace Sir.VectorSpace
{
    public interface INodeReader : IDisposable
    {
        long KeyId { get; }
        Hit ClosestTerm(IVector vector, IStringModel model);
    }

    public class SortedListReader : INodeReader
    {
        public long KeyId { get; }

        private readonly ulong _collectionId;
        private readonly ISessionFactory _sessionFactory;
        private readonly Stream _vectorFile;
        private readonly Stream _ixFile;
        private readonly Stream _sortedList;

        public SortedListReader(
            ulong collectionId,
            long keyId,
            ISessionFactory sessionFactory,
            Stream vectorFile,
            Stream ixFile,
            Stream sortedList)
        {
            KeyId = keyId;
            _collectionId = collectionId;
            _sessionFactory = sessionFactory;
            _vectorFile = vectorFile;
            _ixFile = ixFile;
            _sortedList = sortedList;
        }

        public Hit ClosestTerm(IVector vector, IStringModel model)
        {
            var pages = GetAllPages(
                Path.Combine(_sessionFactory.Dir, $"{_collectionId}.{KeyId}.ixtp"));

            var hits = new ConcurrentBag<Hit>();

            //Parallel.ForEach(pages, page =>
            foreach (var page in pages)
            {
                var hit = ClosestTermInPage(vector, model, page.offset);

                if (hit != null)
                    hits.Add(hit);
            }//);

            Hit best = null;

            foreach (var hit in hits)
            {
                if (best == null || hit.Score > best.Score)
                {
                    best = hit;
                }
                else if (hit.Score >= model.IdenticalAngle)
                {
                    GraphBuilder.MergePostings(best.Node, hit.Node);
                }
            }

            return best;
        }

        public IList<(long offset, long length)> GetAllPages(string pageFileName)
        {
            using (var ixpStream = _sessionFactory.CreateReadStream(pageFileName))
            {
                return new PageIndexReader(ixpStream).GetAll();
            }
        }

        private Hit ClosestTermInPage(
            IVector vector, IStringModel model, long pageOffset)
        {
            _ixFile.Seek(pageOffset, SeekOrigin.Begin);

            var hit0 = ClosestMatchInSegment(
                    vector,
                    _ixFile,
                    _vectorFile,
                    model);

            if (hit0.Score > 0)
            {
                return hit0;
            }

            return null;
        }

        private Hit ClosestMatchInSegment(
            IVector queryVector,
            Stream indexFile,
            Stream vectorFile,
            IStringModel model)
        {
            var angle = model.CosAngle(queryVector, model.SortingVector);

            throw new NotImplementedException();
        }

        public void Dispose()
        {
            _vectorFile.Dispose();
            _ixFile.Dispose();
        }
    }
}
