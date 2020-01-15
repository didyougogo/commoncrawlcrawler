using System;

namespace Sir.VectorSpace
{
    public interface INodeReader : IDisposable
    {
        long KeyId { get; }
        Hit ClosestMatch(IVector vector, IStringModel model);
        int IndexOfClosestMatch(double angle);
    }
}