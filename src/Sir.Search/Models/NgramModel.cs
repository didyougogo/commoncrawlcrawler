using MathNet.Numerics.LinearAlgebra;
using Sir.VectorSpace;
using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;

namespace Sir.Search
{
    public class NGramModel : IStringModel
    {
        public double IdenticalAngle => 0.999d;
        public double FoldAngle => 0.6d;
        public int VectorWidth => 256;

        public IEnumerable<IVector> Tokenize(string text)
        {
            const int sampleSize = 8;
            var source = text.ToCharArray();

            for (int index = 0; index < source.Length; index++)
            {
                var embedding = new SortedList<int, float>(sampleSize);
                var i = 0;

                while (source.Length - (index + i) > 1 && embedding.Count < sampleSize)
                {
                    char c = char.ToLower(source[index + i++]);

                    if (c < VectorWidth - 1 && char.IsLetterOrDigit(c))
                    {
                        embedding.AddOrAppendToComponent(c, 1);
                    }
                }

                yield return new IndexedVector(
                        embedding,
                        text.Substring(index, i).ToCharArray(),
                        VectorWidth);
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
            Span<byte> buf = new byte[componentCount * 2 * sizeof(float)];

            vectorStream.Seek(vectorOffset, SeekOrigin.Begin);
            vectorStream.Read(buf);

            var index = MemoryMarshal.Cast<byte, int>(buf.Slice(0, componentCount * sizeof(int)));
            var values = MemoryMarshal.Cast<byte, float>(buf.Slice(componentCount * sizeof(float)));
            var tuples = new Tuple<int, float>[componentCount];

            for (int i = 0; i < componentCount; i++)
            {
                tuples[i] = new Tuple<int, float>(index[i], values[i]);
            }

            var otherVector = CreateVector.SparseOfIndexed(VectorWidth, tuples);

            var dotProduct = vector.Value.DotProduct(otherVector);
            var dotSelf1 = vector.Value.DotProduct(vector.Value);
            var dotSelf2 = otherVector.DotProduct(otherVector);

            return (dotProduct / (Math.Sqrt(dotSelf1) * Math.Sqrt(dotSelf2)));
        }
    }
}
