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
package org.apache.lucene.codecs.lucene90;

import java.io.IOException;
import java.io.InputStream;
import org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressionMode;
import org.apache.lucene.codecs.lucene90.compressing.Lucene90Compressor;
import org.apache.lucene.codecs.lucene90.compressing.Lucene90Decompressor;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.compress.LZ4;

/**
 * A compression mode that compromises on the compression ratio to provide fast compression and
 * decompression.
 *
 * @lucene.internal
 */
public final class LZ4WithPresetDictCompressionMode extends Lucene90CompressionMode {

  // Shoot for 10 sub blocks
  private static final int NUM_SUB_BLOCKS = 10;
  // And a dictionary whose size is about 2x smaller than sub blocks
  private static final int DICT_SIZE_FACTOR = 2;

  /** Sole constructor. */
  public LZ4WithPresetDictCompressionMode() {}

  @Override
  public Lucene90Compressor newCompressor() {
    return new LZ4WithPresetDictCompressor();
  }

  @Override
  public Lucene90Decompressor newDecompressor() {
    return new LZ4WithPresetDictDecompressor();
  }

  @Override
  public String toString() {
    return "BEST_SPEED";
  }

  private static final class LZ4WithPresetDictDecompressor extends Lucene90Decompressor {

    private int[] compressedLengths;
    private byte[] buffer;

    LZ4WithPresetDictDecompressor() {
      compressedLengths = new int[0];
      buffer = new byte[0];
    }

    private int readCompressedLengths(
        DataInput in, int originalLength, int dictLength, int blockLength) throws IOException {
      in.readVInt(); // compressed length of the dictionary, unused
      int totalLength = dictLength;
      int i = 0;
      compressedLengths = ArrayUtil.growNoCopy(compressedLengths, originalLength / blockLength + 1);
      while (totalLength < originalLength) {

        compressedLengths[i++] = in.readVInt();
        totalLength += blockLength;
      }
      return i;
    }

    @Override
    public InputStream decompress(DataInput in, int originalLength, int offset, int length)
        throws IOException {
      assert offset + length <= originalLength;

      if (length == 0) {
        return InputStream.nullInputStream();
      }

      final int dictLength = in.readVInt();
      final int blockLength = in.readVInt();

      final int numBlocks = readCompressedLengths(in, originalLength, dictLength, blockLength);

      buffer = ArrayUtil.growNoCopy(buffer, dictLength + blockLength);
      final BytesRef bytes = new BytesRef();
      bytes.length = 0;
      // Read the dictionary
      if (LZ4.decompress(in, dictLength, buffer, 0) != dictLength) {
        throw new CorruptIndexException("Illegal dict length", in);
      }

      int offsetInBlock = dictLength;
      int offsetInBytesRef = offset;
      if (offset >= dictLength) {
        offsetInBytesRef -= dictLength;

        // Skip unneeded blocks
        int numBytesToSkip = 0;
        for (int i = 0; i < numBlocks && offsetInBlock + blockLength < offset; ++i) {
          int compressedBlockLength = compressedLengths[i];
          numBytesToSkip += compressedBlockLength;
          offsetInBlock += blockLength;
          offsetInBytesRef -= blockLength;
        }
        in.skipBytes(numBytesToSkip);
      } else {
        // The dictionary contains some bytes we need, copy its content to the BytesRef
        bytes.bytes = ArrayUtil.growNoCopy(bytes.bytes, dictLength);
        System.arraycopy(buffer, 0, bytes.bytes, 0, dictLength);
        bytes.length = dictLength;
      }

      int finalOffsetInBytesRef = offsetInBytesRef;
      int finalOffsetInBlock = offsetInBlock;
      return new InputStream() {
        // need to decompress length + finalOffsetInBytesRef bytes
        final int totalDecompressedLength = finalOffsetInBytesRef + length;
        // already decompressed bytes count
        int decompressed = bytes.length;
        int offsetInBlock = finalOffsetInBlock;

        {
          skip(finalOffsetInBytesRef);
        }

        private void fillBuffer() throws IOException {
          assert decompressed <= totalDecompressedLength;
          // no more bytes to decompress
          if (decompressed == totalDecompressedLength) {
            return;
          }

          final int bytesToDecompress = Math.min(blockLength, offset + length - offsetInBlock);
          assert bytesToDecompress > 0;
          LZ4.decompress(in, bytesToDecompress, buffer, dictLength);
          bytes.bytes = ArrayUtil.grow(bytes.bytes, decompressed + bytesToDecompress);
          System.arraycopy(buffer, dictLength, bytes.bytes, decompressed, bytesToDecompress);
          bytes.length += bytesToDecompress;
          decompressed += bytesToDecompress;
          offsetInBlock += bytesToDecompress;
        }

        @Override
        public int read() throws IOException {
          if (bytes.length == 0) {
            fillBuffer();
          }
          // no more bytes
          if (bytes.length == 0) {
            return -1;
          }
          --bytes.length;
          int b = bytes.bytes[bytes.offset++];
          // correct int auto converting from byte ff
          if (b == -1) {
            b = 255;
          }
          return b;
        }

        @Override
        public int read(byte b[], int off, int len) throws IOException {
          int bytesRead = 0;
          while (len > bytes.length) {
            System.arraycopy(bytes.bytes, bytes.offset, b, off, bytes.length);
            bytes.offset += bytes.length;
            len -= bytes.length;
            off += bytes.length;
            bytesRead += bytes.length;
            bytes.length = 0;
            // decompress more when buffer is empty
            fillBuffer();
            // return actual read bytes count when there are not enough bytes
            if (bytes.length == 0) {
              // return -1 when read nothing
              return bytesRead == 0 ? -1 : bytesRead;
            }
          }
          // copy the rest from the buffer
          System.arraycopy(bytes.bytes, bytes.offset, b, off, len);
          bytes.offset += len;
          bytes.length -= len;
          bytesRead += len;
          return bytesRead;
        }

        @Override
        public long skip(long n) throws IOException {
          if (n < 0) {
            throw new IllegalArgumentException("numBytes must be >= 0, got " + n);
          }
          while (n > bytes.length) {
            int actualSkipped = bytes.length;
            fillBuffer();
            // return actual skipped bytes when there are not enough bytes
            if (actualSkipped == bytes.length) {
              return actualSkipped;
            }
          }
          bytes.offset += n;
          bytes.length -= n;
          return n;
        }

        @Override
        public byte[] readAllBytes() throws IOException {
          byte[] buf = new byte[0];
          int totalRead = 0;
          if (bytes.length == 0) {
            fillBuffer();
          }
          while (bytes.length != 0) {
            buf = ArrayUtil.grow(buf, totalRead + bytes.length);
            System.arraycopy(bytes.bytes, bytes.offset, buf, totalRead, bytes.length);
            bytes.offset += bytes.length;
            totalRead += bytes.length;
            bytes.length = 0;
            fillBuffer();
          }

          return buf;
        }
      };
    }

    @Override
    public Lucene90Decompressor clone() {
      return new LZ4WithPresetDictDecompressor();
    }
  }

  private static class LZ4WithPresetDictCompressor extends Lucene90Compressor {

    final ByteBuffersDataOutput compressed;
    final LZ4.FastCompressionHashTable hashTable;
    byte[] buffer;

    LZ4WithPresetDictCompressor() {
      compressed = ByteBuffersDataOutput.newResettableInstance();
      hashTable = new LZ4.FastCompressionHashTable();
      buffer = BytesRef.EMPTY_BYTES;
    }

    private void doCompress(byte[] bytes, int dictLen, int len, DataOutput out) throws IOException {
      long prevCompressedSize = compressed.size();
      LZ4.compressWithDictionary(bytes, 0, dictLen, len, compressed, hashTable);
      // Write the number of compressed bytes
      out.writeVInt(Math.toIntExact(compressed.size() - prevCompressedSize));
    }

    @Override
    public void compress(byte[] bytes, int off, int len, DataOutput out) throws IOException {
      final int dictLength = len / (NUM_SUB_BLOCKS * DICT_SIZE_FACTOR);
      final int blockLength = (len - dictLength + NUM_SUB_BLOCKS - 1) / NUM_SUB_BLOCKS;
      buffer = ArrayUtil.growNoCopy(buffer, dictLength + blockLength);
      out.writeVInt(dictLength);
      out.writeVInt(blockLength);
      final int end = off + len;

      compressed.reset();
      // Compress the dictionary first
      System.arraycopy(bytes, off, buffer, 0, dictLength);
      doCompress(buffer, 0, dictLength, out);

      // And then sub blocks
      for (int start = off + dictLength; start < end; start += blockLength) {
        int l = Math.min(blockLength, off + len - start);
        System.arraycopy(bytes, start, buffer, dictLength, l);
        doCompress(buffer, dictLength, l, out);
      }

      // We only wrote lengths so far, now write compressed data
      compressed.copyTo(out);
    }

    @Override
    public void close() throws IOException {
      // no-op
    }
  }
}
