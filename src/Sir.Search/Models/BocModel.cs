using MathNet.Numerics.LinearAlgebra;
using Sir.VectorSpace;
using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;

namespace Sir.Search
{
    public class BocModel : IStringModel
    {
        public double IdenticalAngle => 0.99d;
        public double FoldAngle => 0.49d;
        public int VectorWidth => 256;
        public IVector SortingVector { get; }

        public BocModel()
        {
            SortingVector = VectorOperations.CreateSortingVector(VectorWidth);
        }

        public IEnumerable<IVector> Tokenize(Memory<char> source)
        {
            var tokens = new List<IVector>();

            if (source.Length > 0)
            {
                var embedding = new SortedList<int, double>();
                var offset = 0;
                int index = 0;
                var span = source.Span;

                for (; index < source.Length; index++)
                {
                    char c = char.ToLower(span[index]);

                    if (c < VectorWidth && char.IsLetter(c))
                    {
                        embedding.AddOrAppendToComponent(c);
                    }
                    else
                    {
                        var len = index - offset;

                        if (embedding.Count > 0 && len < 20)
                        {
                            var vector = new IndexedVector(
                                embedding,
                                source.Slice(offset, len),
                                VectorWidth);

                            tokens.Add(vector);
                        }

                        embedding.Clear();
                        offset = index + 1;
                    }
                }

                if (embedding.Count > 0 && (index - offset) < 20)
                {
                    var len = index - offset;

                    var vector = new IndexedVector(
                                embedding,
                                source.Slice(offset, len),
                                VectorWidth);

                    tokens.Add(vector);
                }
            }

            return tokens;
        }

        private IEnumerable<IVector> Yield(IEnumerable<IVector> tokens)
        {
            IVector prev = null;
            var index = 0;

            foreach (var token in tokens)
            {
                var angle = CosAngle(token, SortingVector);
                var vector = new IndexedVector(index, angle, VectorWidth, token.Data);

                if (index++ == 2)
                {
                    index = 0;
                }

                if (prev != null)
                {
                    var bigram = vector.Value.Add(prev.Value);

                    yield return new IndexedVector(bigram);

                    prev = vector;
                }

                prev = vector;
            }

            if (index == 1)
            {
                var bigram = prev.Value.Add(new IndexedVector(1, prev.Value[0], VectorWidth).Value);

                yield return new IndexedVector(bigram);
            }
        }

        public double CosAngle(IVector vec1, IVector vec2)
        {
            //var dotProduct = vec1.Value.DotProduct(vec2.Value);
            //var dotSelf1 = vec1.Value.DotProduct(vec1.Value);
            //var dotSelf2 = vec2.Value.DotProduct(vec2.Value);
            //return (dotProduct / (Math.Sqrt(dotSelf1) * Math.Sqrt(dotSelf2)));

            return vec1.Value.DotProduct(vec2.Value) / (vec1.Value.Norm(2) * vec2.Value.Norm(2));
        }

        public double CosAngle(IVector vector, long vectorOffset, int componentCount, Stream vectorStream)
        {
            Span<byte> buf = new byte[componentCount * 2 * sizeof(double)];

            vectorStream.Seek(vectorOffset, SeekOrigin.Begin);
            vectorStream.Read(buf);

            var index = MemoryMarshal.Cast<byte, int>(buf.Slice(0, componentCount * sizeof(int)));
            var values = MemoryMarshal.Cast<byte, double>(buf.Slice(componentCount * sizeof(double)));
            var tuples = new Tuple<int, double>[componentCount];

            for (int i = 0; i < componentCount; i++)
            {
                tuples[i] = new Tuple<int, double>(index[i], values[i]);
            }

            var otherVector = CreateVector.SparseOfIndexed(vector.Value.Count, tuples);

            var dotProduct = vector.Value.DotProduct(otherVector);
            var dotSelf1 = vector.Value.DotProduct(vector.Value);
            var dotSelf2 = otherVector.DotProduct(otherVector);

            return (dotProduct / (Math.Sqrt(dotSelf1) * Math.Sqrt(dotSelf2)));
        }
    }
}
