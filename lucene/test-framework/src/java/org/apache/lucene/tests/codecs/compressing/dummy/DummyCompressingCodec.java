/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.tests.codecs.compressing.dummy;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressionMode;
import org.apache.lucene.codecs.lucene90.compressing.Lucene90Compressor;
import org.apache.lucene.codecs.lucene90.compressing.Lucene90Decompressor;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.tests.codecs.compressing.CompressingCodec;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

/** CompressionCodec that does not compress data, useful for testing. */
// In its own package to make sure the oal.codecs.compressing classes are
// visible enough to let people write their own CompressionMode
public class DummyCompressingCodec extends CompressingCodec {

  public static final Lucene90CompressionMode DUMMY =
      new Lucene90CompressionMode() {

        @Override
        public Lucene90Compressor newCompressor() {
          return DUMMY_COMPRESSOR;
        }

        @Override
        public Lucene90Decompressor newDecompressor() {
          return DUMMY_DECOMPRESSOR;
        }

        @Override
        public String toString() {
          return "DUMMY";
        }
      };

  private static final Lucene90Decompressor DUMMY_DECOMPRESSOR =
      new Lucene90Decompressor() {

        @Override
        public InputStream decompress(DataInput in, int originalLength, int offset, int length)
            throws IOException {
          assert offset + length <= originalLength;
          final BytesRef bytes = new BytesRef();
          bytes.bytes = new byte[ArrayUtil.oversize(originalLength, 1)];
          in.readBytes(bytes.bytes, 0, offset + length);
          bytes.offset = offset;
          bytes.length = length;

          return new ByteArrayInputStream(bytes.bytes, bytes.offset, bytes.length);
        }

        @Override
        public Lucene90Decompressor clone() {
          return this;
        }
      };

  private static final Lucene90Compressor DUMMY_COMPRESSOR =
      new Lucene90Compressor() {

        @Override
        public void compress(byte[] bytes, int off, int len, DataOutput out) throws IOException {
          out.writeBytes(bytes, off, len);
        }

        @Override
        public void close() throws IOException {}
      };

  /** Constructor that allows to configure the chunk size. */
  public DummyCompressingCodec(
      int chunkSize, int maxDocsPerChunk, boolean withSegmentSuffix, int blockSize) {
    super(
        "DummyCompressingStoredFieldsData",
        withSegmentSuffix ? "DummyCompressingStoredFields" : "",
        DUMMY,
        chunkSize,
        maxDocsPerChunk,
        blockSize);
  }

  /** Default constructor. */
  public DummyCompressingCodec() {
    this(1 << 14, 128, false, 10);
  }
}
