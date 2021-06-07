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
package org.apache.lucene.codecs.zstd;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.compressing.Compressor;
import org.apache.lucene.codecs.compressing.Decompressor;
import org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsFormat;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

/** Stored fields format that uses ZSTD for compression. */
public final class ZstdStoredFieldsFormat extends Lucene90CompressingStoredFieldsFormat {

  // Size of top-level blocks, which themselves contain a dictionary and 10 sub blocks
  private static final int BLOCK_SIZE = 10 * 48 * 1024;
  // Shoot for 10 sub blocks
  private static final int NUM_SUB_BLOCKS = 10;
  // And a dictionary whose size is about 3x smaller than sub blocks
  private static final int DICT_SIZE_FACTOR = 6;

  /** Stored fields format with specified compression level. */
  public ZstdStoredFieldsFormat(int level) {
    // same block size, max number of docs per block and block shift as the default codec with
    // DEFLATE
    super("ZstdStoredfields", new ZstdCompressionMode(level), BLOCK_SIZE, 4096, 10);
  }

  private static class ZstdCompressionMode extends CompressionMode {
    private final int level;

    ZstdCompressionMode(int level) {
      this.level = level;
    }

    @Override
    public Compressor newCompressor() {
      return new ZstdCompressor(level);
    }

    @Override
    public Decompressor newDecompressor() {
      return new ZstdDecompressor();
    }
  }

  private static final class ZstdDecompressor extends Decompressor {

    byte[] compressed;

    ZstdDecompressor() {
      compressed = BytesRef.EMPTY_BYTES;
    }

    private void doDecompress(
        DataInput in,
        Zstd.Decompressor dctx,
        Zstd.DecompressionDictionary ddict,
        BytesRef bytes,
        int decompressedLen)
        throws IOException {
      final int compressedLength = in.readVInt();
      if (compressedLength == 0) {
        return;
      }
      compressed = ArrayUtil.grow(compressed, compressedLength);
      in.readBytes(compressed, 0, compressedLength);

      final ByteBuffer compressedBuffer = ByteBuffer.wrap(compressed, 0, compressedLength);
      bytes.bytes = ArrayUtil.grow(bytes.bytes, bytes.length + decompressedLen);

      final int decompressedLen2 =
          dctx.decompress(
              ByteBuffer.wrap(bytes.bytes, bytes.length, bytes.bytes.length - bytes.length),
              compressedBuffer,
              ddict);
      if (decompressedLen != decompressedLen2) {
        throw new IllegalStateException(decompressedLen + " " + decompressedLen2);
      }
      bytes.length += decompressedLen2;
    }

    @Override
    public void decompress(DataInput in, int originalLength, int offset, int length, BytesRef bytes)
        throws IOException {
      assert offset + length <= originalLength;
      if (length == 0) {
        bytes.length = 0;
        return;
      }
      final int dictLength = in.readVInt();
      final int blockLength = in.readVInt();
      bytes.bytes = ArrayUtil.grow(bytes.bytes, dictLength);
      bytes.offset = bytes.length = 0;

      try (Zstd.Decompressor dctx = new Zstd.Decompressor()) {
        // Read the dictionary
        try (Zstd.DecompressionDictionary ddict =
            Zstd.createDecompressionDictionary(ByteBuffer.allocate(0))) {
          doDecompress(in, dctx, ddict, bytes, dictLength);
        }

        int offsetInBlock = dictLength;
        int offsetInBytesRef = offset;

        // Skip unneeded blocks
        while (offsetInBlock + blockLength < offset) {
          final int compressedLength = in.readVInt();
          in.skipBytes(compressedLength);
          offsetInBlock += blockLength;
          offsetInBytesRef -= blockLength;
        }

        // Read blocks that intersect with the interval we need
        if (offsetInBlock < offset + length) {
          try (Zstd.DecompressionDictionary ddict =
              Zstd.createDecompressionDictionary(ByteBuffer.wrap(bytes.bytes, 0, dictLength))) {
            while (offsetInBlock < offset + length) {
              bytes.bytes = ArrayUtil.grow(bytes.bytes, bytes.length + blockLength);
              doDecompress(
                  in, dctx, ddict, bytes, Math.min(blockLength, originalLength - offsetInBlock));
              offsetInBlock += blockLength;
            }
          }
        }

        bytes.offset = offsetInBytesRef;
        bytes.length = length;
        assert bytes.isValid();
      }
    }

    @Override
    public Decompressor clone() {
      return new ZstdDecompressor();
    }
  }

  private static class ZstdCompressor extends Compressor {

    final int level;
    byte[] compressed;

    ZstdCompressor(int level) {
      this.level = level;
      compressed = BytesRef.EMPTY_BYTES;
    }

    private void doCompress(
        byte[] bytes,
        int off,
        int len,
        Zstd.Compressor cctx,
        Zstd.CompressionDictionary cdict,
        DataOutput out)
        throws IOException {
      if (len == 0) {
        out.writeVInt(0);
        return;
      }

      final int maxCompressedLength = Zstd.getMaxCompressedLen(len);
      compressed = ArrayUtil.grow(compressed, maxCompressedLength);

      int compressedLen =
          cctx.compress(
              ByteBuffer.wrap(compressed, 0, compressed.length),
              ByteBuffer.wrap(bytes, off, len),
              cdict,
              level);

      out.writeVInt(compressedLen);
      out.writeBytes(compressed, compressedLen);
    }

    @Override
    public void compress(byte[] bytes, int off, int len, DataOutput out) throws IOException {
      final int dictLength = len / (NUM_SUB_BLOCKS * DICT_SIZE_FACTOR);
      final int blockLength = (len - dictLength + NUM_SUB_BLOCKS - 1) / NUM_SUB_BLOCKS;
      out.writeVInt(dictLength);
      out.writeVInt(blockLength);
      final int end = off + len;

      // Compress the dictionary first
      try (Zstd.Compressor cctx = new Zstd.Compressor()) {

        // First compress the dictionary
        doCompress(bytes, off, dictLength, cctx, null, out);

        // And then sub blocks with this dictionary
        try (Zstd.CompressionDictionary cdict =
            Zstd.createCompressionDictionary(ByteBuffer.wrap(bytes, off, dictLength), level)) {
          for (int start = off + dictLength; start < end; start += blockLength) {
            doCompress(bytes, start, Math.min(blockLength, off + len - start), cctx, cdict, out);
          }
        }
      }
    }

    @Override
    public void close() throws IOException {}
  }
}
