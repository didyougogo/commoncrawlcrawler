using System;

namespace Sir.Search
{
    public interface IIndexSession : IDisposable
    {
        IndexInfo GetIndexInfo();
        void Put(long docId, long keyId, string value);
    }
}