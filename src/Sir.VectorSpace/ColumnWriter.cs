using System;
using System.Collections.Generic;
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
            var page = GraphBuilder.SerializeTree(
                column, 
                _ixStream, 
                vectorStream, 
                postingsStream);

            pageIndexWriter.Put(page.offset, page.length);

            return PathFinder.Size(column);
        }

        public (int depth, int width) CreateSortedPage(
            IEnumerable<KeyValuePair<double,VectorNode>> sortedColumn,
            Stream vectorStream,
            Stream postingsStream,
            PageIndexWriter pageIndexWriter)
        {
            var page = GraphBuilder.SerializeTree(
                sortedColumn, 
                _ixStream, 
                vectorStream, 
                postingsStream);

            pageIndexWriter.Put(page.offset, page.length);

            return (page.count, 0);
        }

        public void Dispose()
        {
            _ixStream.Dispose();
        }
    }
}