﻿using System;
using System.Collections.Generic;

namespace Resin.IO
{
    [Serializable]
    public class IxFile : FileBase<IxFile>
    {
        private string _fixFileName;
        private string _dixFileName;
        private readonly List<Term> _deletions;

        public IxFile()
        {
            _deletions = new List<Term>();
        }

        public IxFile(string fixFileName, string dixFileName, List<Term> deletions)
        {
            _fixFileName = fixFileName;
            _dixFileName = dixFileName;
            _deletions = deletions;
        }

        public string FixFileName { get { return _fixFileName; } set { _fixFileName = value; } }
        public string DixFileName { get { return _dixFileName; } set { _dixFileName = value; } }
        public IList<Term> Deletions { get { return _deletions; } }
    }
}