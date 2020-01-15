using System;
using System.Collections.Generic;
using System.IO;

namespace Sir.VectorSpace
{
    public class ColumnWriter : IDisposable
    {
        private readonly Stream _ixStream;
        private readonly Stream _sortedListStream;
        private readonly PageIndexWriter _sortedListPage;
        private readonly PageIndexWriter _indexPage;

        public ColumnWriter(
            Stream indexStream,
            PageIndexWriter indexPage)
        {
            _ixStream = indexStream;
            _indexPage = indexPage;
        }

        public ColumnWriter(
            Stream sortedListStream,
            Stream indexStream,
            PageIndexWriter sortedListPage,
            PageIndexWriter indexPage)
        {
            _ixStream = indexStream;
            _sortedListStream = sortedListStream;
            _sortedListPage = sortedListPage;
            _indexPage = indexPage;
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

        public int CreatePage(
            SortedList<double, VectorNode> sortedColumn,
            Stream vectorStream,
            Stream postingsStream)
        {
            var page = GraphBuilder.SerializeSortedList(
                sortedColumn,
                _sortedListStream,
                _ixStream,
                vectorStream,
                postingsStream);

            _sortedListPage.Put(page.soffset, page.slength);
            _indexPage.Put(page.ioffset, page.ilength);

            return page.count;
        }

        public int CreatePage(
            SortedList<double, VectorNode> sortedColumn,
            Stream vectorStream)
        {
            var page = GraphBuilder.SerializeSortedList(
                sortedColumn,
                _sortedListStream,
                _ixStream,
                vectorStream);

            _sortedListPage.Put(page.soffset, page.slength);
            _indexPage.Put(page.ioffset, page.ilength);

            return page.count;
        }

        public void Dispose()
        {
            _ixStream.Dispose();
        }
    }
}