using MathNet.Numerics.LinearAlgebra;
using MathNet.Numerics.LinearAlgebra.Storage;
using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;

namespace Sir
{
    public class IndexedVector : IVector
    {
        public ReadOnlyMemory<char>? Data { get; }
        public Vector<double> Value { get; private set; }
        public int ComponentCount { get; }

        public IndexedVector(
            SortedList<int, double> dictionary, 
            ReadOnlyMemory<char> data, 
            int vectorWidth)
        {
            var tuples = new Tuple<int, double>[Math.Min(dictionary.Count, vectorWidth)];
            var i = 0;

            foreach (var p in dictionary)
            {
                if (i == ( vectorWidth))
                    break;

                tuples[i++] = new Tuple<int, double>(p.Key, p.Value);
            }

            Value = CreateVector.Sparse(
                SparseVectorStorage<double>.OfIndexedEnumerable(vectorWidth, tuples));

            ComponentCount = tuples.Length;
            Data = data;
        }

        public IndexedVector(
            IList<double> values,
            ReadOnlyMemory<char> data,
            int vectorWidth)
        {
            var tuples = new Tuple<int, double>[Math.Min(values.Count, vectorWidth)];
            var i = 0;

            foreach (var x in values)
            {
                if (i == (vectorWidth))
                    break;

                tuples[i] = new Tuple<int, double>(i, x);

                i++;
            }

            Value = CreateVector.Sparse(
                SparseVectorStorage<double>.OfIndexedEnumerable(vectorWidth, tuples));

            ComponentCount = tuples.Length;
            Data = data;
        }

        public IndexedVector(
            IList<double> values,
            int vectorWidth)
        {
            var tuples = new Tuple<int, double>[Math.Min(values.Count, vectorWidth)];
            var i = 0;

            foreach (var x in values)
            {
                if (i == (vectorWidth))
                    break;

                tuples[i] = new Tuple<int, double>(i, x);

                i++;
            }

            Value = CreateVector.Sparse(
                SparseVectorStorage<double>.OfIndexedEnumerable(vectorWidth, tuples));

            ComponentCount = tuples.Length;
        }

        public IndexedVector(int[] index, double[] values, int vectorWidth)
        {
            var tuples = new Tuple<int, double>[Math.Min(index.Length, vectorWidth)];

            for (int i = 0; i < index.Length; i++)
            {
                if (i == vectorWidth)
                    break;

                tuples[i] = new Tuple<int, double>(index[i], values[i]);
            }

            Value = CreateVector.Sparse(
                SparseVectorStorage<double>.OfIndexedEnumerable(vectorWidth, tuples));

            ComponentCount = tuples.Length;
        }

        public IndexedVector(int index, double value, int vectorWidth)
        {
            var tuples = new Tuple<int, double>[] { new Tuple<int, double>(index, value) };

            Value = CreateVector.Sparse(
                SparseVectorStorage<double>.OfIndexedEnumerable(vectorWidth, tuples));

            ComponentCount = tuples.Length;
        }

        public IndexedVector(Tuple<int, double>[] tuples, int vectorWidth)
        {
            Value = CreateVector.Sparse(
                SparseVectorStorage<double>.OfIndexedEnumerable(vectorWidth, tuples));

            ComponentCount = tuples.Length;
        }

        public IndexedVector(Vector<double> vector)
        {
            Value = vector;
            ComponentCount = ((SparseVectorStorage<double>)Value.Storage).Length;
        }

        public IndexedVector(IEnumerable<IVector> vectors)
        { 
            foreach (var vector in vectors)
            {
                if (Value == null)
                    Value = vector.Value;
                else
                    Value = Value.Add(vector.Value);
            }

            ComponentCount = ((SparseVectorStorage<double>)Value.Storage).Length;
        }

        public IndexedVector(IVector vector1, IVector vector2)
        {
            Value = vector1.Value.Add(vector2.Value);

            ComponentCount = ((SparseVectorStorage<double>)Value.Storage).Length;
        }

        public IndexedVector(double[] vector, ReadOnlyMemory<char>? data = null)
        {
            Value = CreateVector.Sparse(
                SparseVectorStorage<double>.OfEnumerable(vector));

            ComponentCount = ((SparseVectorStorage<double>)Value.Storage).Length;

            Data = data;
        }

        public long Serialize(Stream stream)
        {
            var offset = stream.Position;

            stream.Write(MemoryMarshal.Cast<int, byte>(((SparseVectorStorage<double>)Value.Storage).Indices));
            stream.Write(MemoryMarshal.Cast<double, byte>(((SparseVectorStorage<double>)Value.Storage).Values));

            return offset;
        }

        public override string ToString()
        {
            return Data.HasValue ? new string(Data.Value.ToArray()) : string.Empty;
        }
    }

    public interface IVector
    {
        Vector<double> Value { get; }
        long Serialize(Stream stream);
        int ComponentCount { get; }
        ReadOnlyMemory<char>? Data { get; }
    }
}