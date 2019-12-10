using System.Collections.Generic;

namespace Sir
{
    public class Job
    {
        public IStringModel Model { get; }
        public ulong CollectionId { get; }
        public IEnumerable<IDictionary<string, object>> Documents { get; }
        public HashSet<string> StoredFieldNames { get; }
        public HashSet<string> IndexedFieldNames { get; }

        public Job(
            ulong collectionId, 
            IEnumerable<IDictionary<string, object>> documents, 
            IStringModel model,
            HashSet<string> storedFieldNames,
            HashSet<string> indexedFieldNames)
        {
            Model = model;
            CollectionId = collectionId;
            Documents = documents;
            StoredFieldNames = storedFieldNames;
            IndexedFieldNames = indexedFieldNames;
        }

        public Job(
            ulong collectionId,
            IEnumerable<IDictionary<string, object>> documents,
            IStringModel model,
            HashSet<string> indexedFieldNames)
        {
            Model = model;
            CollectionId = collectionId;
            Documents = documents;
            IndexedFieldNames = indexedFieldNames;
        }
    }
}