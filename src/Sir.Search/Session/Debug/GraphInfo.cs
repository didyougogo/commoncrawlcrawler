using Sir.VectorSpace;

namespace Sir.Search
{
    public class GraphInfo
    {
        private readonly long _keyId;
        private VectorNode _graph;

        public long Weight { get; }

        public GraphInfo(long keyId, VectorNode graph)
        {
            _keyId = keyId;
            _graph = graph;
            Weight = graph.Weight;
        }

        public GraphInfo(long keyId, int weight)
        {
            _keyId = keyId;
            Weight = weight;
        }

        public override string ToString()
        {
            object size = _graph == null ? (object)null : PathFinder.Size(_graph);
            return $"key {_keyId} weight {Weight} {size}";
        }
    }
}