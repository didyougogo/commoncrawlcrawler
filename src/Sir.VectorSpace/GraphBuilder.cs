﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;

namespace Sir.VectorSpace
{
    public static class GraphBuilder
    {
        public static bool TryMerge(
            VectorNode root, 
            VectorNode node,
            IDistanceCalculator model,
            double foldAngle,
            double identicalAngle,
            out VectorNode parent)
        {
            var cursor = root;

            while (true)
            {
                var angle = cursor.Vector == null ? 0 : model.CosAngle(node.Vector, cursor.Vector);

                if (angle >= identicalAngle)
                {
                    parent = cursor;
                    return true;
                }
                else if (angle > foldAngle)
                {
                    if (cursor.Left == null)
                    {
                        cursor.Left = node;
                        parent = cursor;
                        return false;
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
                        cursor.Right = node;
                        parent = cursor;
                        return false;
                    }
                    else
                    {
                        cursor = cursor.Right;
                    }
                }
            }
        }

        public static long GetOrIncrementId(
            VectorNode root, 
            VectorNode node,
            IDistanceCalculator model, 
            double foldAngle, 
            double identicalAngle)
        {
            var cursor = root;

            while (true)
            {
                var angle = cursor.Vector == null ? 0 : model.CosAngle(node.Vector, cursor.Vector);

                if (angle >= identicalAngle)
                {
                    return cursor.PostingsOffset;
                }
                else if (angle > foldAngle)
                {
                    if (cursor.Left == null)
                    {
                        lock (cursor.Sync)
                        {
                            if (cursor.Left == null)
                            {
                                node.PostingsOffset = root.Weight;
                                cursor.Left = node;
                                return node.PostingsOffset;
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
                        lock (cursor.Sync)
                        {
                            if (cursor.Right == null)
                            {
                                node.PostingsOffset = root.Weight;
                                cursor.Right = node;
                                return node.PostingsOffset;
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

        public static void IncrementId(
            VectorNode root,
            VectorNode node,
            IDistanceCalculator model,
            double foldAngle,
            double identicalAngle)
        {
            var cursor = root;

            while (true)
            {
                var angle = cursor.Vector == null ? 0 : model.CosAngle(node.Vector, cursor.Vector);

                if (angle >= identicalAngle)
                {
                    return;
                }
                else if (angle > foldAngle)
                {
                    if (cursor.Left == null)
                    {
                        lock (cursor.Sync)
                        {
                            if (cursor.Left == null)
                            {
                                node.PostingsOffset = root.Weight;
                                cursor.Left = node;
                                return;
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
                        lock (cursor.Sync)
                        {
                            if (cursor.Right == null)
                            {
                                node.PostingsOffset = root.Weight;
                                cursor.Right = node;
                                return;
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

        public static bool MergeOrAdd(
            VectorNode root, 
            VectorNode node,
            IDistanceCalculator model, 
            double foldAngle, 
            double identicalAngle)
        {
            var cursor = root;

            while (true)
            {
                var angle = cursor.Vector == null ? 0 : model.CosAngle(node.Vector, cursor.Vector);

                if (angle >= identicalAngle)
                {
                    MergeDocIds(cursor, node);

                    return true;
                }
                else if (angle > foldAngle)
                {
                    if (cursor.Left == null)
                    {
                        cursor.Left = node;
                        return false;
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
                        cursor.Right = node;
                        return false;
                    }
                    else
                    {
                        cursor = cursor.Right;
                    }
                }
            }
        }

        public static bool Put(
            VectorNode root,
            VectorNode node,
            IDistanceCalculator model,
            double foldAngle,
            double identicalAngle)
        {
            var cursor = root;

            while (true)
            {
                var angle = cursor.Vector == null ? 0 : model.CosAngle(node.Vector, cursor.Vector);

                if (angle >= identicalAngle)
                {
                    return false;
                }
                else if (angle > foldAngle)
                {
                    if (cursor.Left == null)
                    {
                        cursor.Left = node;
                        return true;
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
                        cursor.Right = node;
                        return true;
                    }
                    else
                    {
                        cursor = cursor.Right;
                    }
                }
            }
        }

        public static bool MergeOrAddConcurrent(
            VectorNode root,
            VectorNode node,
            IDistanceCalculator model,
            double foldAngle,
            double identicalAngle)
        {
            var cursor = root;

            while (true)
            {
                var angle = cursor.Vector == null ? 0 : model.CosAngle(node.Vector, cursor.Vector);

                if (angle >= identicalAngle)
                {
                    lock (cursor.Sync)
                    {
                        MergeDocIds(cursor, node);
                    }

                    return true;
                }
                else if (angle > foldAngle)
                {
                    if (cursor.Left == null)
                    {
                        lock (cursor.Sync)
                        {
                            if (cursor.Left == null)
                            {
                                cursor.Left = node;
                                return false;
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
                        lock (cursor.Sync)
                        {
                            if (cursor.Right == null)
                            {
                                cursor.Right = node;
                                return false;
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

        public static void MergePostings(VectorNode target, VectorNode source)
        {
            if (source.PostingsOffsets != null)
                ((List<long>)target.PostingsOffsets).AddRange(source.PostingsOffsets);
        }

        public static void AddDocId(VectorNode target, long docId)
        {
            target.DocIds.Add(docId);
        }

        public static void MergeDocIds(VectorNode target, VectorNode node) 
        {
            if (node.DocIds != null)
            {
                foreach (var docId in node.DocIds)
                {
                    target.DocIds.Add(docId);
                }
            }
        }

        public static void SerializeAngleAndVectorOffset(double angle, VectorNode node, Stream stream)
        {
            stream.Write(BitConverter.GetBytes(node.VectorOffset));
            stream.Write(BitConverter.GetBytes(node.VectorOffset));
            stream.Write(BitConverter.GetBytes(node.ComponentCount));
        }

        public static void SerializeNode(VectorNode node, Stream stream) 
        {
            long terminator = 1;

            if (node.Left == null && node.Right == null) // there are no children
            {
                terminator = 3;
            }
            else if (node.Left == null) // there is a right but no left
            {
                terminator = 2;
            }
            else if (node.Right == null) // there is a left but no right
            {
                terminator = 1;
            }
            else // there is a left and a right
            {
                terminator = 0;
            }

            Span<long> span = stackalloc long[5];

            span[0] = node.VectorOffset;
            span[1] = node.PostingsOffset;
            span[2] = node.Vector.ComponentCount;
            span[3] = node.Weight;
            span[4] = terminator;

            stream.Write(MemoryMarshal.Cast<long, byte>(span));
        }

        public static (long offset, long length, int count) SerializeSortedListOfAngles(
            SortedList<double, VectorNode> sortedNodes,
            Stream indexStream)
        {
            Span<double> span = new double[sortedNodes.Count];

            for (int i = 0; i < span.Length; i++)
            {
                span[i] = sortedNodes.Keys[i];
            }

            var offset = indexStream.Position;
            var buf = MemoryMarshal.Cast<double, byte>(span);

            indexStream.Write(buf);

            return (offset, buf.Length, sortedNodes.Count);
        }

        public static (long soffset, long slength, long ioffset, long ilength, int count)
        SerializeSortedList(
            SortedList<double, VectorNode> sortedNodes,
            Stream sortedListStream,
            Stream indexStream,
            Stream vectorStream)
        {
            var soffset = sortedListStream.Position;
            var ioffset = indexStream.Position;

            Span<double> keys = new double[sortedNodes.Count];

            for (int i = 0; i < keys.Length; i++)
            {
                keys[i] = sortedNodes.Keys[i];

                var node = sortedNodes.Values[i];

                node.VectorOffset = VectorOperations.SerializeVector(node.Vector, vectorStream);

                SerializeNode(node, indexStream);
            }

            var keyBuf = MemoryMarshal.Cast<double, byte>(keys);

            indexStream.Write(keyBuf);

            return (soffset, sortedListStream.Position - soffset, ioffset, indexStream.Position - ioffset, sortedNodes.Count);
        }

        public static (long soffset, long slength, long ioffset, long ilength, int count) 
        SerializeSortedList(
            SortedList<double, VectorNode> sortedNodes,
            Stream sortedListStream,
            Stream indexStream,
            Stream vectorStream,
            Stream postingsStream)
        {
            var soffset = sortedListStream.Position;
            var ioffset = indexStream.Position;

            Span<double> keys = new double[sortedNodes.Count];

            for (int i = 0; i < keys.Length; i++)
            {
                keys[i] = sortedNodes.Keys[i];

                var node = sortedNodes.Values[i];

                node.VectorOffset = VectorOperations.SerializeVector(node.Vector, vectorStream);

                SerializePostings(node, postingsStream);

                SerializeNode(node, indexStream);
            }

            var keyBuf = MemoryMarshal.Cast<double, byte>(keys);

            indexStream.Write(keyBuf);

            return (soffset, sortedListStream.Position - soffset, ioffset, indexStream.Position - ioffset, sortedNodes.Count);
        }

        public static (long offset, long length) SerializeTree(
            VectorNode node, 
            Stream indexStream, 
            Stream vectorStream, 
            Stream postingsStream)
        {
            var stack = new Stack<VectorNode>();
            var offset = indexStream.Position;
            var length = 0;

            if (node.ComponentCount == 0)
            {
                node = node.Right;
            }

            while (node != null)
            {
                if (node.DocIds != null)
                    SerializePostings(node, postingsStream);

                node.VectorOffset = VectorOperations.SerializeVector(node.Vector, vectorStream);

                SerializeNode(node, indexStream);

                length += VectorNode.BlockSize;

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

            return (offset, length);
        }

        public static void SerializePostings(VectorNode node, Stream postingsStream)
        {
            node.PostingsOffset = postingsStream.Position;

            SerializeHeaderAndPayload(node.DocIds, node.DocIds.Count, postingsStream);
        }

        public static void SerializeHeaderAndPayload(IEnumerable<long> items, int itemCount, Stream stream)
        {
            var payload = new long[itemCount + 1];

            payload[0] = itemCount;

            var index = 1;

            foreach (var item in items)
            {
                payload[index++] = item;
            }

            stream.Write(MemoryMarshal.Cast<long, byte>(payload));
        }

        public static VectorNode DeserializeNode(Stream nodeStream, Stream vectorStream, IModel model, long offset)
        {
            nodeStream.Seek(offset, SeekOrigin.Begin);

            Span<byte> buf = new byte[VectorNode.BlockSize];

            var read = nodeStream.Read(buf);

            if (read != VectorNode.BlockSize)
                throw new DataMisalignedException();

            Span<long> list = MemoryMarshal.Cast<byte, long>(buf);

            // Deserialize node
            var vecOffset = list[0];
            var postingsOffset = list[1];
            var componentCount = list[2];
            var weight = list[3];
            var terminator = list[4];

            var vector = VectorOperations.DeserializeVector(vecOffset, (int)componentCount, model.VectorWidth, vectorStream);

            return new VectorNode(postingsOffset, vecOffset, terminator, weight, vector);
        }

        public static VectorNode DeserializeNode(byte[] nodeBuffer, Stream vectorStream, IModel model)
        {
            // Deserialize node
            var vecOffset = BitConverter.ToInt64(nodeBuffer, 0);
            var postingsOffset = BitConverter.ToInt64(nodeBuffer, sizeof(long));
            var vectorCount = BitConverter.ToInt64(nodeBuffer, sizeof(long) + sizeof(long));
            var weight = BitConverter.ToInt64(nodeBuffer, sizeof(long) + sizeof(long) + sizeof(long));
            var terminator = BitConverter.ToInt64(nodeBuffer, sizeof(long) + sizeof(long) + sizeof(long) + sizeof(long));

            return DeserializeNode(vecOffset, postingsOffset, vectorCount, weight, terminator, vectorStream, model);
        }

        public static VectorNode DeserializeNode(
            long vecOffset,
            long postingsOffset,
            long componentCount,
            long weight,
            long terminator,
            Stream vectorStream,
            IVectorSpaceConfig model) 
        {
            var vector = VectorOperations.DeserializeVector(vecOffset, (int)componentCount, model.VectorWidth, vectorStream);
            var node = new VectorNode(postingsOffset, vecOffset, terminator, weight, vector);

            return node;
        }

        public static void DeserializeUnorderedFile(
            Stream indexStream,
            Stream vectorStream,
            VectorNode root,
            float identicalAngle, 
            float foldAngle,
            IModel model) 
        {
            var buf = new byte[VectorNode.BlockSize];
            int read = indexStream.Read(buf);

            while (read == VectorNode.BlockSize)
            {
                var node = DeserializeNode(buf, vectorStream, model);
                VectorNode parent;

                if (TryMerge(root, node, model, model.FoldAngle, model.IdenticalAngle, out parent))
                {
                    MergePostings(parent, node);
                }

                read = indexStream.Read(buf);
            }
        }

        public static Memory<double> Map(Stream stream)
        {
            using (var mem = new MemoryStream())
            {
                stream.CopyTo(mem);

                Span<byte> buf = mem.ToArray();

                return MemoryMarshal.Cast<byte, double>(buf).ToArray();
            }
        }

        public static Memory<double> Map(Stream stream, long offset, int length)
        {
            stream.Seek(offset, SeekOrigin.Begin);

            var buf = new byte[length*sizeof(double)];

            stream.Read(buf);

            return MemoryMarshal.Cast<byte, double>(buf).ToArray();
        }

        public static void DeserializeTree(
            Stream indexStream,
            Stream vectorStream,
            long indexLength,
            VectorNode root,
            (float identicalAngle, float foldAngle) similarity,
            IModel model) 
        {
            int read = 0;
            var buf = new byte[VectorNode.BlockSize];

            while (read < indexLength)
            {
                indexStream.Read(buf);

                var node = DeserializeNode(buf, vectorStream, model);
                VectorNode parent;

                if (TryMerge(root, node, model, model.FoldAngle, model.IdenticalAngle, out parent))
                {
                    MergePostings(parent, node);
                }

                read += VectorNode.BlockSize;
            }
        }

        public static VectorNode DeserializeTree(
            Stream indexStream, Stream vectorStream, long indexLength, IModel model)
        {
            VectorNode root = new VectorNode();
            VectorNode cursor = root;
            var tail = new Stack<VectorNode>();
            int read = 0;
            var buf = new byte[VectorNode.BlockSize];

            while (read < indexLength)
            {
                indexStream.Read(buf);

                var node = DeserializeNode(buf, vectorStream, model);

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
                read += VectorNode.BlockSize;
            }

            return root;
        }
    }
}
