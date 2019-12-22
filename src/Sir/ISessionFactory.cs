using System.IO;

namespace Sir
{
    public interface ISessionFactory
    {
        IConfigurationProvider Config { get; }
        string Dir { get; }

        bool CollectionExists(ulong collectionId);
        bool CollectionIsIndexOnly(ulong collectionId);
        Stream OpenAppendStream(string fileName, int bufferSize = 4096);
        Stream OpenAsyncAppendStream(string fileName, int bufferSize = 4096);
        Stream OpenAsyncReadStream(string fileName, int bufferSize = 4096);
        Stream OpenReadStream(string fileName, int bufferSize = 4096, FileOptions fileOptions = FileOptions.RandomAccess);
        void Dispose();
        void RegisterKeyMapping(ulong collectionId, ulong keyHash, long keyId);
        void RegisterCollectionAlias(ulong collectionId, ulong originalCollectionId);
        ulong GetCollectionReference(ulong collectionId);
        void Refresh();
        void Truncate(ulong collectionId);
        void TruncateIndex(ulong collectionId);
        bool TryGetKeyId(ulong collectionId, ulong keyHash, out long keyId);
    }
}