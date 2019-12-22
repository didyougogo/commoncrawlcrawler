using Sir.VectorSpace;
using System;
using System.Collections.Generic;
using System.IO;

namespace Sir.Search
{
    public class BigramModel : IStringModel
    {
        private readonly BocModel _bocModel;

        public double IdenticalAngle => 0.999d;
        public double FoldAngle => 0.499d;
        public int VectorWidth => 2;
        public IVector SortingVector { get; }

        public BigramModel(BocModel bocModel)
        {
            _bocModel = bocModel;
            SortingVector = VectorOperations.CreateSortingVector(VectorWidth);
        }

        public IEnumerable<IVector> Tokenize(Memory<char> source)
        {
            IVector a = null;
            var count = 0;

            foreach (var token in _bocModel.Tokenize(source))
            {
                var angle = CosAngle(token, _bocModel.SortingVector);
                var vector = new IndexedVector(0, angle, VectorWidth);

                if (a == null)
                {
                    a = vector;    
                }
                else
                {
                    yield return new IndexedVector(a, vector);
                }

                count++;
            }

            if (count==1)
                yield return a;
        }

        public double CosAngle(IVector vec1, IVector vec2)
        {
            return _bocModel.CosAngle(vec1, vec2);
        }

        public double CosAngle(IVector vector, long vectorOffset, int componentCount, Stream vectorStream)
        {
            return _bocModel.CosAngle(vector, vectorOffset, componentCount, vectorStream);
        }
    }
}
