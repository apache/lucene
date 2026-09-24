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
package org.apache.lucene.sandbox.codecs.dedup;

import static org.apache.lucene.util.StringHelper.GOOD_FAST_HASH_SEED;
import static org.apache.lucene.util.StringHelper.murmurhash3_x64_128;

import java.io.IOException;
import org.apache.lucene.codecs.lucene95.OrdToDocDISIReaderConfiguration;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.sandbox.codecs.dedup.DedupVectorValues.FieldOrdToGroupOrd;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.packed.DirectWriter;

/**
 * Shared helpers for the de-duplicating flat format: reading / writing field and group metadata,
 * vector hashing and alignment, and the {@link DedupVectorValues} views used on the read path.
 *
 * @lucene.experimental
 */
final class DedupUtil {

  private static final int ORD_TO_DOC_DIRECT_MONOTONIC_BLOCK_SHIFT = 16;

  private static final int END_MARKER = -1;

  /** Alignment bytes on disk for fieldOrdToGroupOrd. */
  private static final int FIELD_ORD_TO_GROUP_ORD_ALIGN_BYTES = 4;

  /** Initial allocation size for internal re-used int[] scratch buffers. */
  static final int SCRATCH_INITIAL_SIZE = 16;

  /** Key used to group vectors (dimension + encoding). */
  record GroupKey(int dimension, VectorEncoding encoding) {
    GroupKey(FieldInfo fieldInfo) {
      this(fieldInfo.getVectorDimension(), fieldInfo.getVectorEncoding());
    }
  }

  record GroupInfo(
      int groupOrd,
      int dimension,
      VectorEncoding encoding,
      int groupNumVectors,
      long vectorDataOffset,
      long vectorDataSize) {

    void write(IndexOutput meta) throws IOException {
      meta.writeInt(groupOrd);
      meta.writeInt(dimension);
      meta.writeInt(encoding.ordinal());
      meta.writeInt(groupNumVectors);
      meta.writeLong(vectorDataOffset);
      meta.writeLong(vectorDataSize);
    }

    static GroupInfo readFromMeta(IndexInput meta) throws IOException {
      int groupOrd = meta.readInt();
      if (groupOrd == END_MARKER) {
        return null;
      }

      int dimension = meta.readInt();
      VectorEncoding encoding = VectorEncoding.values()[meta.readInt()];
      int groupNumVectors = meta.readInt();
      long vectorDataOffset = meta.readLong();
      long vectorDataSize = meta.readLong();

      return new GroupInfo(
          groupOrd, dimension, encoding, groupNumVectors, vectorDataOffset, vectorDataSize);
    }
  }

  static void writeFieldInfo(
      IndexOutput meta,
      IndexOutput vectorData,
      int fieldNumber,
      VectorSimilarityFunction function,
      int dimension,
      VectorEncoding encoding,
      int groupOrd,
      int vectorCount,
      int maxDoc,
      int maxGroupOrd,
      DocsWithFieldSet docs,
      FieldOrdToGroupOrd fieldOrdToGroupOrd)
      throws IOException {

    meta.writeInt(fieldNumber);
    meta.writeInt(function.ordinal());
    meta.writeInt(dimension);
    meta.writeInt(encoding.ordinal());
    meta.writeInt(groupOrd);
    meta.writeInt(vectorCount);

    // write ordToDoc
    OrdToDocDISIReaderConfiguration.writeStoredMeta(
        ORD_TO_DOC_DIRECT_MONOTONIC_BLOCK_SHIFT, meta, vectorData, vectorCount, maxDoc, docs);

    // write fieldOrdToGroupOrd
    //
    // The group ordinals are typically far smaller than 2^32 (the whole point of de-duplication is
    // that distinct vectors are few relative to documents), so we pack each ordinal using the
    // minimum number of bits that can represent the largest group ordinal referenced by this field
    // (tracked by the caller as vectors are added). The chosen width is persisted to the metadata
    // so the reader can decode without assuming a fixed width.
    int groupOrdBitsPerValue = DirectWriter.bitsRequired(maxGroupOrd);

    long fieldOrdToGroupOrdOffset = vectorData.alignFilePointer(FIELD_ORD_TO_GROUP_ORD_ALIGN_BYTES);
    DirectWriter writer = DirectWriter.getInstance(vectorData, vectorCount, groupOrdBitsPerValue);
    for (int i = 0; i < vectorCount; i++) {
      writer.add(fieldOrdToGroupOrd.get(i));
    }
    writer.finish();
    long fieldOrdToGroupOrdSize = vectorData.getFilePointer() - fieldOrdToGroupOrdOffset;

    meta.writeLong(fieldOrdToGroupOrdOffset);
    meta.writeLong(fieldOrdToGroupOrdSize);
    meta.writeInt(groupOrdBitsPerValue);
  }

  static void writeEndMarker(IndexOutput meta) throws IOException {
    meta.writeInt(END_MARKER);
  }

  record ReadFieldInfo(
      int fieldNumber,
      VectorSimilarityFunction function,
      int dimension,
      VectorEncoding encoding,
      int groupOrd,
      int vectorCount,
      OrdToDocDISIReaderConfiguration ordToDoc,
      long fieldOrdToGroupOrdOffset,
      long fieldOrdToGroupOrdSize,
      // Bits used to pack each entry of the fieldOrdToGroupOrd array (a group ordinal); named to
      // match its sibling fieldOrdToGroupOrd* fields, referred to as groupOrdBitsPerValue
      // elsewhere.
      int fieldOrdToGroupOrdBitsPerValue) {

    static ReadFieldInfo read(IndexInput meta) throws IOException {

      int fieldNumber = meta.readInt();
      if (fieldNumber == END_MARKER) {
        return null;
      }

      VectorSimilarityFunction function = VectorSimilarityFunction.values()[meta.readInt()];
      int dimension = meta.readInt();
      VectorEncoding encoding = VectorEncoding.values()[meta.readInt()];
      int groupOrd = meta.readInt();
      int vectorCount = meta.readInt();
      OrdToDocDISIReaderConfiguration ordToDoc =
          OrdToDocDISIReaderConfiguration.fromStoredMeta(meta, vectorCount);
      long fieldOrdToGroupOrdOffset = meta.readLong();
      long fieldOrdToGroupOrdSize = meta.readLong();
      int fieldOrdToGroupOrdBitsPerValue = meta.readInt();

      return new ReadFieldInfo(
          fieldNumber,
          function,
          dimension,
          encoding,
          groupOrd,
          vectorCount,
          ordToDoc,
          fieldOrdToGroupOrdOffset,
          fieldOrdToGroupOrdSize,
          fieldOrdToGroupOrdBitsPerValue);
    }
  }

  static long hashBytes(byte[] bytes) {
    return murmurhash3_x64_128(bytes, 0, bytes.length, GOOD_FAST_HASH_SEED)[0];
  }

  static long alignBytes(IndexOutput output, VectorEncoding encoding) throws IOException {
    int alignBytes =
        switch (encoding) {
          case BYTE -> 4;
          case FLOAT32, FLOAT16 -> 64;
        };
    return output.alignFilePointer(alignBytes);
  }
}
