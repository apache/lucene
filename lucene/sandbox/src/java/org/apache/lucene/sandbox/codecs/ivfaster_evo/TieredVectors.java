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

package org.apache.lucene.sandbox.codecs.ivfaster_evo;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MemorySegmentAccessInput;

/** Cell-contiguous coarse and fine sections, aligned by slot number. */
final class TieredVectors extends FloatVectorValues {
  final TierCodec coarse, fine;
  final VectorSimilarityFunction similarity;
  final int[] docs;
  final long offset;
  final int slotCount;
  final int[] primarySlots;
  final IndexInput input;
  private final IndexInput fineInput;
  private final float[] scratch;
  private final byte[] fineScratch;

  TieredVectors(
      TierCodec coarse,
      TierCodec fine,
      VectorSimilarityFunction similarity,
      int[] docs,
      long offset,
      int slotCount,
      int[] primarySlots,
      IndexInput input,
      IndexInput fineInput) {
    this.coarse = coarse;
    this.fine = fine;
    this.similarity = similarity;
    this.docs = docs;
    this.offset = offset;
    this.slotCount = slotCount;
    this.primarySlots = primarySlots;
    this.input = input;
    this.fineInput = fineInput;
    scratch = new float[fine.dim];
    fineScratch = new byte[fine.bytes];
  }

  @Override
  public int size() {
    return docs.length;
  }

  @Override
  public int dimension() {
    return fine.dim;
  }

  @Override
  public int ordToDoc(int ord) {
    return docs[ord];
  }

  @Override
  public DocIndexIterator iterator() {
    return createSparseIterator();
  }

  @Override
  public TieredVectors copy() {
    return new TieredVectors(
        coarse,
        fine,
        similarity,
        docs,
        offset,
        slotCount,
        primarySlots,
        input.clone(),
        fineInput.clone());
  }

  void read(int ord, boolean coarseTier, byte[] dest) throws IOException {
    java.util.Objects.checkIndex(ord, size());
    readSlot(primarySlots[ord], coarseTier, dest);
  }

  /**
   * The two sections are read through separate inputs over the same file. Coarse records are
   * scanned in contiguous runs and keep the default read advice; fine records are fetched a
   * shortlist at a time from scattered positions, so their input asks for random access, which
   * stops the OS reading around every record it faults in.
   */
  private IndexInput input(boolean coarseTier) {
    return coarseTier ? input : fineInput;
  }

  /** File position of a slot's record: all coarse records come first, then all fine records. */
  private long position(int slot, boolean coarseTier) {
    return offset
        + (coarseTier
            ? (long) slot * coarse.bytes
            : (long) slotCount * coarse.bytes + (long) slot * fine.bytes);
  }

  void readSlot(int slot, boolean coarseTier, byte[] dest) throws IOException {
    java.util.Objects.checkIndex(slot, slotCount);
    IndexInput in = input(coarseTier);
    in.seek(position(slot, coarseTier));
    in.readBytes(dest, 0, dest.length);
  }

  /**
   * Hints a run of records ahead of reading it, so that cold page faults overlap instead of
   * arriving one at a time. A mapped input backs off to a counter increment once pages are
   * resident, so this costs nothing measurable when the index fits in memory.
   */
  void prefetch(int slot, int rows, boolean coarseTier) throws IOException {
    long length = (long) rows * (coarseTier ? coarse.bytes : fine.bytes);
    input(coarseTier).prefetch(position(slot, coarseTier), length);
  }

  void copyRecord(int ord, IndexOutput out) throws IOException {
    int slot = primarySlots[ord];
    input.seek(position(slot, true));
    out.copyBytes(input, coarse.bytes);
    fineInput.seek(position(slot, false));
    out.copyBytes(fineInput, fine.bytes);
  }

  /**
   * Hamming distances from a Nitrox2 query code to the admitted rows of a slot range: one mapped
   * range or one contiguous buffered read, never one seek/copy per coarse record.
   */
  void hammingBulk(
      int slot, int rows, byte[] query, boolean[] admitted, byte[] block, int[] distances)
      throws IOException {
    long start = position(slot, true);
    MemorySegment records =
        input instanceof MemorySegmentAccessInput mapped
            ? mapped.segmentSliceOrNull(start, (long) rows * coarse.bytes)
            : null;
    if (records == null) {
      input.seek(start);
      input.readBytes(block, 0, rows * coarse.bytes);
      records = MemorySegment.ofArray(block);
    }
    VectorKernels.INSTANCE.hammingBulk(query, records, coarse.bytes, rows, admitted, distances);
  }

  @Override
  public float[] vectorValue(int ord) throws IOException {
    read(ord, false, fineScratch);
    fine.decode(fineScratch, scratch);
    return scratch;
  }

  @Override
  public VectorScorer scorer(float[] target) {
    TieredVectors view = copy();
    var iterator = view.iterator();
    var scorer = fine.scorer(target, similarity);
    return new VectorScorer() {
      @Override
      public DocIdSetIterator iterator() {
        return iterator;
      }

      @Override
      public float score() throws IOException {
        view.read(iterator.index(), false, view.fineScratch);
        return (float) scorer.score(view.fineScratch);
      }
    };
  }
}
