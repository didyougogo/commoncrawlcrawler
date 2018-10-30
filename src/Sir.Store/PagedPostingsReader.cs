﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Net;

namespace Sir.Store
{
    public class PagedPostingsReader
    {
        private readonly Stream _stream;
        private readonly byte[] _pageBuf;

        public static int PAGE_SIZE = PagedPostingsWriter.PAGE_SIZE;
        public static int BLOCK_SIZE = PagedPostingsWriter.BLOCK_SIZE;
        public static int SLOTS_PER_PAGE = PagedPostingsWriter.SLOTS_PER_PAGE;

        public PagedPostingsReader(Stream stream)
        {
            _stream = stream;
            _pageBuf = new byte[PAGE_SIZE];
        }

        public IList<ulong> Read(long offset)
        {
            var result = new List<ulong>();
            Read(offset, result);
            return result;
        }

        private void Read(long offset, IList<ulong> result)
        {
            _stream.Seek(offset, SeekOrigin.Begin);
            _stream.Read(_pageBuf, 0, PAGE_SIZE);

            var blockBuf = new byte[BLOCK_SIZE];
            int pos = 0;
            ulong docId = 0;

            while (pos < SLOTS_PER_PAGE)
            {
                docId = BitConverter.ToUInt64(_pageBuf, pos * (BLOCK_SIZE));

                if (docId == 0)
                {
                    // Zero means "no data". We can start writing into the page at the current position.
                    break;
                }
                else
                {
                    var indexOfStatusByte = (pos * (BLOCK_SIZE)) + sizeof(ulong);

                    if (_pageBuf[indexOfStatusByte] == 1)
                    {
                        result.Add(docId);
                    }

                    pos++;
                }
            }

            if (pos == SLOTS_PER_PAGE)
            {
                // Page is full but luckily the last word 
                // is the offset for the next page.
                // Jump to that location and continue reading there.

                long nextOffset = Convert.ToInt64(docId);
                Read(nextOffset, result);
            }
        }
    }

    public class RemotePostingsReader
    {
        private IConfigurationService _config;

        public RemotePostingsReader(IConfigurationService config)
        {
            _config = config;
        }

        public IList<ulong> Read(string collectionId, long offset)
        {
            var endpoint = _config.Get("postings_endpoint");
            var request = (HttpWebRequest)WebRequest.Create(endpoint + collectionId);
            request.ContentType = "application/postings";
            request.Method = WebRequestMethods.Http.Get;

            var response = (HttpWebResponse)request.GetResponse();
            var result = new List<ulong>();

            using (var body = response.GetResponseStream())
            {
                var mem = new MemoryStream();
                body.CopyTo(mem);

                var buf = mem.ToArray();

                var read = 0;

                while (read < buf.Length)
                {
                    result.Add(BitConverter.ToUInt64(buf, read));

                    read += sizeof(ulong);
                }
            }

            return result;
        }
    }
}
