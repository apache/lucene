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
import java.util.Collection;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.DenseLiveDocs;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.SparseFixedBitSet;
import org.apache.lucene.util.SparseLiveDocs;

/**
 * Lucene 9.0 live docs format
 *
 * <p>The .liv file is optional, and only exists when a segment contains deletions.
 *
 * <p>Although per-segment, this file is maintained exterior to compound segment files.
 *
 * <p>Deletions (.liv) --&gt; IndexHeader,Generation,Bits
 *
 * <ul>
 *   <li>SegmentHeader --&gt; {@link CodecUtil#writeIndexHeader IndexHeader}
 *   <li>Bits --&gt; &lt;{@link DataOutput#writeLong Int64}&gt; <sup>LongCount</sup>
 * </ul>
 */
public final class Lucene90LiveDocsFormat extends LiveDocsFormat {

  /** extension of live docs */
  private static final String EXTENSION = "liv";

  /** codec of live docs */
  private static final String CODEC_NAME = "Lucene90LiveDocs";

  /** supported version range */
  private static final int VERSION_START = 0;

  private static final int VERSION_CURRENT = VERSION_START;

  /**
   * Deletion rate threshold for choosing sparse vs dense representation. If deletion rate is at or
   * below this threshold, use SparseFixedBitSet; otherwise use dense FixedBitSet.
   */
  private static final double SPARSE_DENSE_THRESHOLD = 0.01; // 1%

  /** Sole constructor. */
  public Lucene90LiveDocsFormat() {}

  @Override
  public Bits readLiveDocs(Directory dir, SegmentCommitInfo info, IOContext context)
      throws IOException {
    long gen = info.getDelGen();
    String name = IndexFileNames.fileNameFromGeneration(info.info.name, EXTENSION, gen);
    final int maxDoc = info.info.maxDoc();
    final int delCount = info.getDelCount();
    final double deletionRate = (double) delCount / maxDoc;

    try (ChecksumIndexInput input = dir.openChecksumInput(name)) {
      Throwable priorE = null;
      try {
        CodecUtil.checkIndexHeader(
            input,
            CODEC_NAME,
            VERSION_START,
            VERSION_CURRENT,
            info.info.getId(),
            Long.toString(gen, Character.MAX_RADIX));

        return readLiveDocs(input, maxDoc, deletionRate, delCount);
      } catch (Throwable exception) {
        priorE = exception;
      } finally {
        CodecUtil.checkFooter(input, priorE);
      }
    }
    throw new AssertionError();
  }

  /**
   * Reads live docs from input and chooses between sparse and dense representation based on
   * deletion rate.
   */
  private Bits readLiveDocs(IndexInput input, int maxDoc, double deletionRate, int expectedDelCount)
      throws IOException {
    Bits liveDocs;
    int actualDelCount;

    if (deletionRate <= SPARSE_DENSE_THRESHOLD) {
      SparseFixedBitSet sparse = readSparseFixedBitSet(input, maxDoc);
      actualDelCount = sparse.cardinality();
      liveDocs = SparseLiveDocs.builder(sparse, maxDoc).withDeletedCount(actualDelCount).build();
    } else {
      FixedBitSet dense = readFixedBitSet(input, maxDoc);
      actualDelCount = maxDoc - dense.cardinality();
      liveDocs = DenseLiveDocs.builder(dense, maxDoc).withDeletedCount(actualDelCount).build();
    }

    if (actualDelCount != expectedDelCount) {
      throw new CorruptIndexException(
          "bits.deleted=" + actualDelCount + " info.delcount=" + expectedDelCount, input);
    }

    return liveDocs;
  }

  private FixedBitSet readFixedBitSet(IndexInput input, int length) throws IOException {
    long[] data = new long[FixedBitSet.bits2words(length)];
    input.readLongs(data, 0, data.length);
    return new FixedBitSet(data, length);
  }

  private SparseFixedBitSet readSparseFixedBitSet(IndexInput input, int length) throws IOException {
    long[] data = new long[FixedBitSet.bits2words(length)];
    input.readLongs(data, 0, data.length);

    SparseFixedBitSet sparse = new SparseFixedBitSet(length);
    for (int wordIndex = 0; wordIndex < data.length; wordIndex++) {
      long word = data[wordIndex];
      // Semantic inversion: disk format stores LIVE docs (bit=1 means live, bit=0 means deleted)
      // but SparseLiveDocs stores DELETED docs (bit=1 means deleted).
      // Skip words with all bits set (all docs live in disk format = no deletions to convert)
      if (word == -1L) {
        continue;
      }
      int baseDocId = wordIndex << 6;
      int maxDocInWord = Math.min(baseDocId + 64, length);
      for (int docId = baseDocId; docId < maxDocInWord; docId++) {
        int bitIndex = docId & 63;
        // If bit is 0 in disk format (deleted doc), set it in sparse representation (bit=1 means
        // deleted)
        if ((word & (1L << bitIndex)) == 0) {
          sparse.set(docId);
        }
      }
    }
    return sparse;
  }

  @Override
  public void writeLiveDocs(
      Bits bits, Directory dir, SegmentCommitInfo info, int newDelCount, IOContext context)
      throws IOException {
    long gen = info.getNextDelGen();
    String name = IndexFileNames.fileNameFromGeneration(info.info.name, EXTENSION, gen);
    int delCount;
    try (IndexOutput output = dir.createOutput(name, context)) {

      CodecUtil.writeIndexHeader(
          output,
          CODEC_NAME,
          VERSION_CURRENT,
          info.info.getId(),
          Long.toString(gen, Character.MAX_RADIX));

      delCount = writeBits(output, bits);

      CodecUtil.writeFooter(output);
    }
    if (delCount != info.getDelCount() + newDelCount) {
      throw new CorruptIndexException(
          "bits.deleted="
              + delCount
              + " info.delcount="
              + info.getDelCount()
              + " newdelcount="
              + newDelCount,
          name);
    }
  }

  private int writeBits(IndexOutput output, Bits bits) throws IOException {
    int delCount = bits.length();
    // Copy bits in batches of 1024 bits at once using Bits#applyMask, which is faster than checking
    // bits one by one.
    FixedBitSet copy = new FixedBitSet(1024);
    for (int offset = 0; offset < bits.length(); offset += copy.length()) {
      int numBitsToCopy = Math.min(bits.length() - offset, copy.length());
      copy.set(0, copy.length());
      if (numBitsToCopy < copy.length()) {
        // Clear ghost bits
        copy.clear(numBitsToCopy, copy.length());
      }
      bits.applyMask(copy, offset);
      delCount -= copy.cardinality();
      int longCount = FixedBitSet.bits2words(numBitsToCopy);
      for (int i = 0; i < longCount; ++i) {
        output.writeLong(copy.getBits()[i]);
      }
    }
    return delCount;
  }

  @Override
  public void files(SegmentCommitInfo info, Collection<String> files) throws IOException {
    if (info.hasDeletions()) {
      files.add(IndexFileNames.fileNameFromGeneration(info.info.name, EXTENSION, info.getDelGen()));
    }
  }
}
