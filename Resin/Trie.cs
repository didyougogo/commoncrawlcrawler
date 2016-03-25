﻿using System;
using System.Collections.Generic;
using System.IO;
using ProtoBuf;

namespace Resin
{
    [ProtoContract]
    public class Trie
    {
        [ProtoMember(1)]
        private readonly char _value;

        [ProtoMember(2)]
        private bool _eow;

        [ProtoMember(3, DataFormat = DataFormat.Group)]
        private readonly IDictionary<char, Trie> _children;

        public Trie()
        {
            _children = new Dictionary<char, Trie>();
        }

        public Trie(IList<string> words) : this()
        {
            if (words == null) throw new ArgumentNullException("words");

            foreach (var word in words)
            {
                Add(word);
            }
        }

        private Trie(string text) : this()
        {
            if (string.IsNullOrWhiteSpace(text)) throw new ArgumentException("word");

            _value = text[0];

            if (text.Length > 1)
            {
                var overflow = text.Substring(1);
                if (overflow.Length > 0)
                {
                    Add(overflow);
                }
            }
            else
            {
                _eow = true;
            }
        }
        
        public IEnumerable<string> Similar(string word, int distance)
        {
            var words = new List<string>();
            SimScan(word, word, distance, 0, words);
            return words;
        }

        private void SimScan(string word, string state, int distance, int index, IList<string> words)
        {
            var childIndex = index + 1;
            foreach (var child in _children.Values)
            {
                var tmp = index == state.Length ? state + child._value : state.ReplaceAt(index, child._value);
                if (Levenshtein.Distance(word, tmp) <= distance)
                {
                    if (child._eow) words.Add(tmp);
                    child.SimScan(word, tmp, distance, childIndex, words);  
                }
            }
        }

        public IEnumerable<string> Prefixed(string prefix)
        {
            var words = new List<string>();
            Trie child;
            if (_children.TryGetValue(prefix[0], out child))
            {
                child.PrefixScan(prefix, prefix, words);
            }
            return words;
        }

        private void PrefixScan(string state, string prefix, List<string> words)
        {
            if (string.IsNullOrWhiteSpace(prefix)) throw new ArgumentException("prefix");

            if (prefix.Length == 1 && prefix[0] == _value)
            {
                // The scan has reached its destination. Find words derived from this node.
                if (_eow) words.Add(state);
                foreach (var node in _children.Values)
                {
                    node.PrefixScan(state+node._value, new string(new []{node._value}), words);
                }
            }
            else if (prefix[0] == _value)
            {
                Trie child;
                if (_children.TryGetValue(prefix[1], out child))
                {
                    child.PrefixScan(state, prefix.Substring(1), words);
                }
            }
        }

        public void Add(string word)
        {
            if (string.IsNullOrWhiteSpace(word)) throw new ArgumentException("word");

            Trie child;
            if (!_children.TryGetValue(word[0], out child))
            {
                child = new Trie(word);
                _children.Add(word[0], child);
            }
            else
            {
                child.Append(word);
            }
        }

        private void Append(string text)
        {
            if (string.IsNullOrWhiteSpace(text)) throw new ArgumentException("word");
            if (text[0] != _value) throw new ArgumentOutOfRangeException("text");

            var overflow = text.Substring(1);
            if (overflow.Length > 0)
            {
                Add(overflow);
            }
        }

        public void Save(string fileName)
        {
            using (var fs = File.Create(fileName))
            {
                Serializer.Serialize(fs, this);
            }
        }

        public static Trie Load(string fileName)
        {
            using (var file = File.OpenRead(fileName))
            {
                return Serializer.Deserialize<Trie>(file);
            }
        }

        public void Remove(string word)
        {
            if (string.IsNullOrWhiteSpace(word)) throw new ArgumentException("word");

            Trie child;
            if (_children.TryGetValue(word[0], out child))
            {
                if (child._children.Count == 0)
                {
                    _children.Remove(child._value);
                }
                else
                {
                    child._eow = false;
                }
                if (word.Length > 1) child.Remove(word.Substring(1));
            }
        }
    }

    public static class Levenshtein
    {
        public static string ReplaceAtOrAppend(this string input, int index, char newChar)
        {
            if (input == null) throw new ArgumentNullException("input");

            if (input.Length == index) return input + newChar;
            return input.ReplaceAt(index, newChar);
        }

        public static string ReplaceAt(this string input, int index, char newChar)
        {
           
            char[] chars = input.ToCharArray();
            chars[index] = newChar;
            return new string(chars);
        }

        //public static int Distance(char[] a, char[] b)
        //{
        //    if (a == null || a.Length == 0)
        //    {
        //        if (b != null && b.Length > 0)
        //        {
        //            return b.Length;
        //        }
        //        return 0;
        //    }

        //    if (b == null || b.Length == 0)
        //    {
        //        if (a.Length > 0)
        //        {
        //            return a.Length;
        //        }
        //        return 0;
        //    }

        //    int[,] d = new int[a.Length + 1, b.Length + 1];

        //    for (int i = 0; i <= d.GetUpperBound(0); i += 1)
        //    {
        //        d[i, 0] = i;
        //    }

        //    for (int i = 0; i <= d.GetUpperBound(1); i += 1)
        //    {
        //        d[0, i] = i;
        //    }

        //    for (int i = 1; i <= d.GetUpperBound(0); i += 1)
        //    {
        //        for (int j = 1; j <= d.GetUpperBound(1); j += 1)
        //        {
        //            var cost = Convert.ToInt32(a[i - 1] != b[j - 1]);

        //            var min1 = d[i - 1, j] + 1;
        //            var min2 = d[i, j - 1] + 1;
        //            var min3 = d[i - 1, j - 1] + cost;
        //            d[i, j] = Math.Min(Math.Min(min1, min2), min3);
        //        }
        //    }
        //    return d[d.GetUpperBound(0), d.GetUpperBound(1)];
        //}

        public static int Distance(string a, string b)
        {
            if (string.IsNullOrEmpty(a))
            {
                if (!string.IsNullOrEmpty(b))
                {
                    return b.Length;
                }
                return 0;
            }

            if (string.IsNullOrEmpty(b))
            {
                if (!string.IsNullOrEmpty(a))
                {
                    return a.Length;
                }
                return 0;
            }

            int[,] d = new int[a.Length + 1, b.Length + 1];

            for (int i = 0; i <= d.GetUpperBound(0); i += 1)
            {
                d[i, 0] = i;
            }

            for (int i = 0; i <= d.GetUpperBound(1); i += 1)
            {
                d[0, i] = i;
            }

            for (int i = 1; i <= d.GetUpperBound(0); i += 1)
            {
                for (int j = 1; j <= d.GetUpperBound(1); j += 1)
                {
                    var cost = Convert.ToInt32(a[i - 1] != b[j - 1]);

                    var min1 = d[i - 1, j] + 1;
                    var min2 = d[i, j - 1] + 1;
                    var min3 = d[i - 1, j - 1] + cost;
                    d[i, j] = Math.Min(Math.Min(min1, min2), min3);
                }
            }
            return d[d.GetUpperBound(0), d.GetUpperBound(1)];
        }
    }
}