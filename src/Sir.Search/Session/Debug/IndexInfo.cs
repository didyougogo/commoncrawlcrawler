using System.Collections.Generic;
using System.Linq;

namespace Sir.Search
{
    public class IndexInfo
    {
        public IEnumerable<GraphInfo> Info { get; }
        public long Merges { get; private set; }
        public long LexiconWeight { get; }

        public IndexInfo(IEnumerable<GraphInfo> info, long merges)
        {
            Info = info;
            Merges = merges;
        }

        public override string ToString()
        {
            string debug = string.Join("\r\n", Info.Select(x => x.ToString()));

            return $"merges {Merges} \r\n{debug}";
        }
    }
}