using System;
using System.IO;

namespace Sir.VectorSpace
{
    public class ColumnWriter : IDisposable
    {
        private readonly Stream _ixStream;

        public ColumnWriter(
            Stream indexStream)
        {
            _ixStream = indexStream;
        }

        public (int depth, int width) CreatePage(
            VectorNode column, 
            Stream vectorStream, 
            Stream postingsStream, 
            PageIndexWriter pageIndexWriter)
        {
            var page = GraphBuilder.SerializeTree(column, _ixStream, vectorStream, postingsStream);

            pageIndexWriter.Put(page.offset, page.length);

            return PathFinder.Size(column);
        }

        public void Dispose()
        {
            _ixStream.Dispose();
        }
    }
}