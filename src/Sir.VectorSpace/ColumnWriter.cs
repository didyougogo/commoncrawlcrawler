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

        public int CreateSortedPage(
            SortedList<double, VectorNode> sortedColumn,
            Stream postingsStream,
            PageIndexWriter pageIndexWriter)
        {
            var page = GraphBuilder.SerializeSortedList(
                sortedColumn,
                _ixStream,
                postingsStream);

            pageIndexWriter.Put(page.offset, page.length);

            return page.count;
        }

        public int CreateSortedPage(SortedList<double,VectorNode> sortedColumn)
        {
            var page = GraphBuilder.SerializeSortedList(
                sortedColumn, 
                _ixStream);

            return page.count;
        }

        public void Dispose()
        {
            _ixStream.Dispose();
        }
    }
}