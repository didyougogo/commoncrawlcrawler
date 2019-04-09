﻿using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sir.Store
{
    /// <summary>
    /// Binary tree that is balanced according to the cos angles of the vectors that is each node's payload.
    /// </summary>
    public class VectorNode
    {
        public const int BlockSize = sizeof(float) + sizeof(long) + sizeof(long) + sizeof(int) + sizeof(int) + sizeof(byte);
        public const int ComponentSize = sizeof(long) + sizeof(int);

        private VectorNode _right;
        private VectorNode _left;
        private VectorNode _ancestor;

        public HashSet<long> DocIds { get; private set; }
        private int _weight;

        public int ComponentCount { get; set; }
        public long VectorOffset { get; private set; }
        public long PostingsOffset { get; set; }
        public float Angle { get; private set; }
        public SortedList<long, int> Vector { get; set; }

        public int Weight
        {
            get { return _weight; }
            set
            {
                var diff = value - _weight;

                _weight = value;

                if (diff > 0)
                {
                    var cursor = _ancestor;
                    while (cursor != null)
                    {
                        cursor._weight += diff;
                        cursor = cursor._ancestor;
                    }
                }
            }
        }

        public VectorNode Right
        {
            get => _right;
            set
            {
                _right = value;
                _right._ancestor = this;
                Weight++;
            }
        }

        public VectorNode Left
        {
            get => _left;
            set
            {
                _left = value;
                _left._ancestor = this;
                Weight++;
            }
        }

        public byte Terminator { get; set; }

        public IList<long> PostingsOffsets { get; set; }

        public VectorNode(bool shallow)
        {
        }

        public VectorNode()
            : this('\0'.ToString())
        {
        }

        public VectorNode(string s)
            : this(s.ToCharVector())
        {
        }

        public VectorNode(SortedList<long, int> termVector)
        {
            Vector = termVector;
            PostingsOffset = -1;
            VectorOffset = -1;
        }

        public VectorNode(SortedList<long, int> vector, long docId)
        {
            Vector = vector;
            PostingsOffset = -1;
            VectorOffset = -1;
            DocIds = new HashSet<long>();
            DocIds.Add(docId);
        }

        public Hit ClosestMatch(SortedList<long, int> vector, float foldAngle)
        {
            var best = this;
            var cursor = this;
            float highscore = 0;

            while (cursor != null)
            {
                var angle = vector.CosAngle(cursor.Vector);

                if (angle > foldAngle)
                {
                    if (angle > highscore)
                    {
                        highscore = angle;
                        best = cursor;
                    }

                    cursor = cursor.Left;
                }
                else
                {
                    if (angle > highscore)
                    {
                        highscore = angle;
                        best = cursor;
                    }
                    cursor = cursor.Right;
                }
            }

            return new Hit
            {
                Score = highscore,
                Node = best
            };
        }

        public IEnumerable<Hit> Intersecting(VectorNode node, float foldAngle)
        {
            var intersecting = new List<Hit>();
            var cursor = this;

            while (cursor != null)
            {
                var angle = node.Vector.CosAngle(cursor.Vector);

                if (angle > 0)
                {
                    intersecting.Add(new Hit
                    {
                        Score = angle,
                        Node = cursor
                    });
                }

                if (angle > foldAngle)
                {
                    cursor = cursor.Left;
                }
                else
                {
                    cursor = cursor.Right;
                }
            }

            return intersecting.OrderByDescending(x => x.Score);
        }

        private readonly object _sync = new object();

        public void Add(
            VectorNode node, 
            (float identicalAngle, float foldAngle) similarity, 
            Stream vectorStream = null)
        {
            node._ancestor = null;
            node._left = null;
            node._right = null;
            node._weight = 0;

            var cursor = this;

            while (cursor != null)
            {
                var angle = node.Vector.CosAngle(cursor.Vector);

                if (angle >= similarity.identicalAngle)
                {
                    node.Angle = angle;

                    lock (_sync)
                    {
                        cursor.Merge(node);
                    }

                    break;
                }
                else if (angle > similarity.foldAngle)
                {
                    if (cursor.Left == null)
                    {
                        lock (_sync)
                        {
                            if (cursor.Left == null)
                            {
                                node.Angle = angle;
                                cursor.Left = node;

                                if (vectorStream != null)
                                    cursor.Left.SerializeVector(vectorStream);

                                break;
                            }
                            else
                            {
                                cursor = cursor.Left;
                            }
                        }
                    }
                    else
                    {
                        cursor = cursor.Left;
                    }
                }
                else
                {
                    if (cursor.Right == null)
                    {
                        lock (_sync)
                        {
                            if (cursor.Right == null)
                            {
                                node.Angle = angle;
                                cursor.Right = node;

                                if (vectorStream != null)
                                    cursor.Right.SerializeVector(vectorStream);

                                break;
                            }
                            else
                            {
                                cursor = cursor.Right;
                            }
                        }
                    }
                    else
                    {
                        cursor = cursor.Right;
                    }
                }
            }
        }

        public void Merge(VectorNode node)
        {
            if (DocIds == null)
            {
                DocIds = node.DocIds;
            }
            else
            {
                foreach (var id in node.DocIds)
                {
                    DocIds.Add(id);
                }
            }

            if (node.PostingsOffset >= 0)
            {
                if (PostingsOffset >= 0)
                {
                    if (PostingsOffsets == null)    
                    {
                        PostingsOffsets = new List<long> { PostingsOffset, node.PostingsOffset };
                    }
                    else
                    {
                        PostingsOffsets.Add(node.PostingsOffset);
                    }
                }
                else
                {
                    PostingsOffset = node.PostingsOffset;
                }
            }
        }

        private byte[][] ToStream()
        {
            if (_ancestor != null)
            {
                if (VectorOffset < 0)
                {
                    throw new InvalidOperationException();
                }

                if (PostingsOffset < 0)
                {
                    throw new InvalidOperationException();
                }
            }

            var block = new byte[6][];

            byte[] terminator = new byte[1];

            if (Left == null && Right == null) // there are no children
            {
                terminator[0] = 3;
            }
            else if (Left == null) // there is a right but no left
            {
                terminator[0] = 2;
            }
            else if (Right == null) // there is a left but no right
            {
                terminator[0] = 1;
            }
            else // there is a left and a right
            {
                terminator[0] = 0;
            }

            block[0] = BitConverter.GetBytes(Angle);
            block[1] = BitConverter.GetBytes(VectorOffset);
            block[2] = BitConverter.GetBytes(PostingsOffset);
            block[3] = BitConverter.GetBytes(Vector.Count);
            block[4] = BitConverter.GetBytes(Weight);
            block[5] = terminator;

            return block;
        }

        public (long offset, long length) SerializeTree(Stream indexStream)
        {
            var node = this;
            var stack = new Stack<VectorNode>();
            var offset = indexStream.Position;

            while (node != null)
            {
                foreach (var buf in node.ToStream())
                {
                    indexStream.Write(buf, 0, buf.Length);
                }

                if (node.Right != null)
                {
                    stack.Push(node.Right);
                }

                node = node.Left;

                if (node == null && stack.Count > 0)
                {
                    node = stack.Pop();
                }
            }

            var length = indexStream.Position - offset;

            return (offset, length);
        }

        public void SerializeVector(Stream vectorStream)
        {
            VectorOffset = Vector.Serialize(vectorStream);
        }

        private async Task SerializeVectorAsync(Stream vectorStream)
        {
            VectorOffset = await Vector.SerializeAsync(vectorStream);
        }

        public IList<VectorNode> SerializePostings(Stream lengths, Stream offsets, Stream lists)
        {
            var node = this;
            var stack = new Stack<VectorNode>();
            var result = new List<VectorNode>();

            while (node != null)
            {
                if (node.DocIds != null)
                {
                    // dirty node

                    var list = node.DocIds.ToArray();

                    node.DocIds.Clear();

                    var buf = list.ToStream();

                    lists.Write(buf);
                    lengths.Write(BitConverter.GetBytes(buf.Length));
                    offsets.Write(BitConverter.GetBytes(node.PostingsOffset));

                    result.Add(node);
                }

                if (node.Right != null)
                {
                    stack.Push(node.Right);
                }

                node = node.Left;

                if (node == null)
                {
                    if (stack.Count > 0)
                        node = stack.Pop();
                }
            }

            return result;
        }

        public static void DeserializeTree(
            Stream indexStream, 
            Stream vectorStream, 
            long indexLength, 
            VectorNode root,
            (float identicalAngle, float foldAngle) similarity)
        {
            int read = 0;
            var buf = new byte[BlockSize];

            while (read < indexLength)
            {
                indexStream.Read(buf);

                var terminator = new byte();
                var node = DeserializeNode(buf, vectorStream, ref terminator);

                if (node.VectorOffset > -1)
                    root.Add(node, similarity);

                read += BlockSize;
            }
        }

        public static VectorNode DeserializeTree(Stream indexStream, Stream vectorStream, long indexLength)
        {
            VectorNode root = new VectorNode();
            VectorNode cursor = root;
            var tail = new Stack<VectorNode>();
            byte terminator = 2;
            int read = 0;
            var buf = new byte[BlockSize];

            while (read < indexLength)
            {
                indexStream.Read(buf);

                var node = DeserializeNode(buf, vectorStream, ref terminator);

                if (node.Terminator == 0) // there is both a left and a right child
                {
                    cursor.Left = node;
                    tail.Push(cursor);
                }
                else if (node.Terminator == 1) // there is a left but no right child
                {
                    cursor.Left = node;
                }
                else if (node.Terminator == 2) // there is a right but no left child
                {
                    cursor.Right = node;
                }
                else // there are no children
                {
                    if (tail.Count > 0)
                    {
                        tail.Pop().Right = node;
                    }
                }

                cursor = node;
                read += BlockSize;
            }

            var right = root.Right;

            right._ancestor = null;

            return right;
        }

        public static VectorNode DeserializeNode(byte[] buf, MemoryMappedViewAccessor vectorView, ref byte terminator)
        {
            // Deserialize node
            var angle = BitConverter.ToSingle(buf, 0);
            var vecOffset = BitConverter.ToInt64(buf, sizeof(float));
            var postingsOffset = BitConverter.ToInt64(buf, sizeof(float) + sizeof(long));
            var vectorCount = BitConverter.ToInt32(buf, sizeof(float) + sizeof(long) + sizeof(long));
            var weight = BitConverter.ToInt32(buf, sizeof(float) + sizeof(long) + sizeof(long) + sizeof(int));

            // Deserialize term vector
            var vec = new SortedList<long, int>(vectorCount);
            var vecBuf = new byte[vectorCount * ComponentSize];

            if (vecOffset < 0)
            {
                vec.Add(0, 1);
            }
            else
            {
                vectorView.ReadArray(vecOffset, vecBuf, 0, vecBuf.Length);

                var offs = 0;

                for (int i = 0; i < vectorCount; i++)
                {
                    var key = BitConverter.ToInt64(vecBuf, offs);
                    var val = vecBuf[offs + sizeof(long)];

                    vec.Add(key, val);

                    offs += ComponentSize;
                }
            }

            // Create node
            var node = new VectorNode(vec);

            node.Angle = angle;
            node.PostingsOffset = postingsOffset;
            node.VectorOffset = vecOffset;
            node.Terminator = terminator;
            node.Weight = weight;

            terminator = buf[buf.Length - 1];

            return node;
        }

        public static VectorNode DeserializeNode(byte[] nodeBuffer, Stream vectorStream, ref byte terminator)
        {
            // Deserialize node
            var angle = BitConverter.ToSingle(nodeBuffer, 0);
            var vecOffset = BitConverter.ToInt64(nodeBuffer, sizeof(float));
            var postingsOffset = BitConverter.ToInt64(nodeBuffer, sizeof(float) + sizeof(long));
            var vectorCount = BitConverter.ToInt32(nodeBuffer, sizeof(float) + sizeof(long) + sizeof(long));
            var weight = BitConverter.ToInt32(nodeBuffer, sizeof(float) + sizeof(long) + sizeof(long) + sizeof(int));

            return DeserializeNode(angle, vecOffset, postingsOffset, vectorCount, weight, vectorStream, ref terminator);
        }

        public static VectorNode DeserializeNode(
            float angle, 
            long vecOffset, 
            long postingsOffset, 
            int componentCount, 
            int weight, 
            Stream vectorStream,
            ref byte terminator)
        {
            // Create node
            var node = new VectorNode(shallow:true);

            node.Angle = angle;
            node.PostingsOffset = postingsOffset;
            node.VectorOffset = vecOffset;
            node.Terminator = terminator;
            node.Weight = weight;
            node.ComponentCount = componentCount;

            Load(node, vectorStream);

            return node;
        }

        public static void Load(
            VectorNode shallow,
            Stream vectorStream = null)
        {
            if (shallow.Vector != null)
            {
                return;
            }

            shallow.Vector = DeserializeVector(shallow.VectorOffset, shallow.ComponentCount, vectorStream);
        }

        public static SortedList<long, int> DeserializeVector(long vectorOffset, int componentCount, Stream vectorStream)
        {
            if (vectorStream == null)
            {
                throw new ArgumentNullException(nameof(vectorStream));
            }

            // Deserialize term vector
            var vec = new SortedList<long, int>(componentCount);
            var vecBuf = new byte[componentCount * ComponentSize];

            if (vectorOffset < 0)
            {
                vec.Add(0, 1);
            }
            else
            {
                vectorStream.Seek(vectorOffset, SeekOrigin.Begin);
                vectorStream.Read(vecBuf, 0, vecBuf.Length);

                var offs = 0;

                for (int i = 0; i < componentCount; i++)
                {
                    var key = BitConverter.ToInt64(vecBuf, offs);
                    var val = vecBuf[offs + sizeof(long)];

                    vec.Add(key, val);

                    offs += ComponentSize;
                }
            }

            return vec;
        }

        public string Visualize()
        {
            StringBuilder output = new StringBuilder();
            Visualize(this, output, 0);
            return output.ToString();
        }

        public int Depth()
        {
            var count = 0;
            var node = Left;

            while (node != null)
            {
                count++;
                node = node.Left;
            }
            return count;
        }

        public VectorNode GetRoot()
        {
            var cursor = this;
            while (cursor != null)
            {
                if (cursor._ancestor == null) break;
                cursor = cursor._ancestor;
            }
            return cursor;
        }

        public VectorNode ShallowCopy()
        {
            return new VectorNode (Vector)
            {
                VectorOffset = VectorOffset,
                PostingsOffset = PostingsOffset,
                PostingsOffsets = PostingsOffsets
            };
        }

        public IEnumerable<VectorNode> All()
        {
            var node = this;
            var stack = new Stack<VectorNode>();

            while (node != null)
            {
                if (node.PostingsOffset > -1)
                {
                    yield return node.ShallowCopy();
                }

                if (node.Right != null)
                {
                    stack.Push(node.Right);
                }

                node = node.Left;

                if (node == null)
                {
                    if (stack.Count > 0)
                        node = stack.Pop();
                }
            }
        }

        private void Visualize(VectorNode node, StringBuilder output, int depth)
        {
            if (node == null) return;

            float angle = 0;

            if (node._ancestor != null)
            {
                angle = node.Angle;
            }

            output.Append('\t', depth);
            output.AppendFormat(".{0} ({1})", node.ToString(), angle);
            output.AppendLine();

            if (node.Left != null)
                Visualize(node.Left, output, depth + 1);

            if (node.Right != null)
                Visualize(node.Right, output, depth);
        }

        public (int depth, int width, int avgDepth) Size()
        {
            var width = 0;
            var depth = 1;
            var node = this;
            var aggDepth = 0;
            var count = 0;

            while (node != null)
            {
                var d = node.Depth();
                if (d > depth)
                {
                    depth = d;
                }

                aggDepth += d;
                count++;
                width++;

                node = node.Right;
            }

            return (depth, width, aggDepth / count);
        }

        public override string ToString()
        {
            var w = new StringBuilder();

            foreach (var c in Vector)
            {
                w.Append((char)c.Key);
            }

            return w.ToString();
        }
    }

    public static class StreamHelper
    {
        public static byte[] ToStream(this IEnumerable<long> docIds)
        {
            var payload = new MemoryStream();

            foreach (var id in docIds)
            {
                var buf = BitConverter.GetBytes(id);

                payload.Write(buf, 0, buf.Length);
            }

            return payload.ToArray();
        }
    }
}
