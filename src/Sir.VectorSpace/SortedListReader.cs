//using System;
//using System.Collections.Generic;
//using System.IO;

//namespace Sir.VectorSpace
//{
//    public class SortedListReader : INodeReader
//    {
//        private readonly ulong _collectionId;
//        private readonly ISessionFactory _sessionFactory;
//        private readonly Stream _vectorStream;
//        private readonly Stream _nodeStream;
//        private readonly Stream _sortedListStream;

//        public long KeyId { get; }

//        public SortedListReader(
//            ulong collectionId, 
//            long keyId,
//            ISessionFactory sessionFactory,
//            Stream sortedListStream,
//            Stream ixStream,
//            Stream vectorStream)
//        {
//            KeyId = keyId;
//            _collectionId = collectionId;
//            _nodeStream = ixStream;
//            _sortedListStream = sortedListStream;
//            _sessionFactory = sessionFactory;
//            _vectorStream = vectorStream;
//        }

//        public Hit ClosestMatch(IVector vector, IStringModel model)
//        {
//            var pages = GetAllPages(
//                Path.Combine(_sessionFactory.Dir, $"{_collectionId}.{KeyId}.uixp"));

//            var hits = new List<Hit>();

//            foreach (var page in pages)
//            {
//                var hit = ClosestMatchInPage(vector, model, page.offset, (int)page.length);

//                if (hit != null)
//                    hits.Add(hit);
//            }

//            Hit best = null;

//            foreach (var hit in hits)
//            {
//                if (best == null || hit.Score > best.Score)
//                {
//                    best = hit;
//                }
//                else if (hit.Score >= model.IdenticalAngle)
//                {
//                    GraphBuilder.MergePostings(best.Node, hit.Node);
//                }
//            }

//            return best;
//        }


//        public int IndexOfClosestMatch(double angle)
//        {
//            var map = GraphBuilder.Map(_sortedListStream);

//            return IndexOfClosestMatch(angle, map);
//        }

//        private Hit ClosestMatchInPage(IVector vector, IStringModel model, long offset, int length)
//        {
//            var map = GraphBuilder.Map(_sortedListStream, offset, length);
//            var angle = model.CosAngle(model.SortingVector, vector);
//            var index = IndexOfClosestMatch(angle, map);

//            if (index < 0)
//            {
//                return null;
//            }

//            var nodeOffset = VectorNode.BlockSize * index;
//            var node = GraphBuilder.DeserializeNode(_nodeStream, _vectorStream, model, nodeOffset);
//            var score = model.CosAngle(node.Vector, vector);

//            return new Hit(node, score);
//        }

//        private static int IndexOfClosestMatch(double angle, Memory<double> map)
//        {
//            var result = map.Span.BinarySearch(angle);

//            if (result < 0)
//            {
//                if (result != ~map.Span.Length)
//                {
//                    return ~result;
//                }

//                return -1;
//            }

//            return result;
//        }

//        private IList<(long offset, long length)> GetAllPages(string pageFileName)
//        {
//            using (var stream = _sessionFactory.OpenReadStream(pageFileName))
//            {
//                return new PageIndexReader(stream).GetAllPages();
//            }
//        }

//        public void Dispose()
//        {
//            _nodeStream.Dispose();
//            _sortedListStream.Dispose();
//            _vectorStream.Dispose();
//        }
//    }
//}