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
package org.apache.lucene.codecs.lucene90.compressing;

import java.io.IOException;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.compressing.Compressor;
import org.apache.lucene.codecs.compressing.Decompressor;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.SortingStoredFieldsConsumer;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

/** A {@link SortingStoredFieldsConsumer} using Lucene90StoredFields format. */
public final class Lucene90SortingStoredFieldsConsumer extends SortingStoredFieldsConsumer {
  /** A mode that appends bytes without applying compression. */
  public static final CompressionMode NO_COMPRESSION =
      new CompressionMode() {
        @Override
        public Compressor newCompressor() {
          return new Compressor() {
            @Override
            public void close() {}

            @Override
            public void compress(ByteBuffersDataInput buffersInput, DataOutput out)
                throws IOException {
              out.copyBytes(buffersInput, buffersInput.length());
            }
          };
        }

        @Override
        public Decompressor newDecompressor() {
          return new Decompressor() {
            @Override
            public void decompress(
                DataInput in, int originalLength, int offset, int length, BytesRef bytes)
                throws IOException {
              bytes.bytes = ArrayUtil.growNoCopy(bytes.bytes, length);
              in.skipBytes(offset);
              in.readBytes(bytes.bytes, 0, length);
              bytes.offset = 0;
              bytes.length = length;
            }

            @Override
            public Decompressor clone() {
              return this;
            }
          };
        }
      };

  private static final StoredFieldsFormat LUCENE90_NO_COMPRESSION_STORED_FIELDS_FORMAT =
      new Lucene90CompressingStoredFieldsFormat(
          "Lucene90TempStoredFields", NO_COMPRESSION, 128 * 1024, 1, 10);

  public Lucene90SortingStoredFieldsConsumer(Codec codec, Directory directory, SegmentInfo info) {
    super(codec, LUCENE90_NO_COMPRESSION_STORED_FIELDS_FORMAT, directory, info);
  }

  @Override
  protected void copyDocs(
      SegmentWriteState state,
      Sorter.DocMap sortMap,
      StoredFieldsWriter writer,
      StoredFieldsReader reader)
      throws IOException {
    if (reader instanceof Lucene90CompressingStoredFieldsReader compressReader
        && compressReader.getVersion() == Lucene90CompressingStoredFieldsWriter.VERSION_CURRENT) {
      if (writer instanceof Lucene90CompressingStoredFieldsWriter compressWriter) {
        for (int docID = 0; docID < state.segmentInfo.maxDoc(); docID++) {
          compressWriter.copyOneDoc(
              compressReader, sortMap == null ? docID : sortMap.newToOld(docID));
        }
        return;
      }
    } else {
      assert false : "the temp stored fields format wasn't used?";
    }
    super.copyDocs(state, sortMap, writer, reader);
  }
}
