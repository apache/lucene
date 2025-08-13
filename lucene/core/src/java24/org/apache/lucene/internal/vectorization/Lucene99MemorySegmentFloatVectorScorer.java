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
package org.apache.lucene.internal.vectorization;

import static org.apache.lucene.util.VectorUtil.normalizeToUnitInterval;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Optional;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.util.hnsw.RandomVectorScorer;

abstract sealed class Lucene99MemorySegmentFloatVectorScorer
    extends RandomVectorScorer.AbstractRandomVectorScorer {

  final FloatVectorValues values;
  final int vectorByteSize;
  final MemorySegment seg;
  final float[] query;

  /**
   * Return an optional whose value, if present, is the scorer. Otherwise, an empty optional is
   * returned.
   */
  public static Optional<Lucene99MemorySegmentFloatVectorScorer> create(
      VectorSimilarityFunction type,
      IndexInput input,
      FloatVectorValues values,
      float[] queryVector)
      throws IOException {
    input = FilterIndexInput.unwrapOnlyTest(input);
    MemorySegment seg;
    if (!(input instanceof MemorySegmentAccessInput msInput
        && (seg = msInput.segmentSliceOrNull(0L, msInput.length())) != null)) {
      return Optional.empty();
    }
    checkInvariants(values.size(), values.getVectorByteLength(), input);
    return switch (type) {
      case COSINE -> Optional.empty(); // of(new CosineScorer(msInput, values, queryVector));
      case DOT_PRODUCT -> Optional.of(new DotProductScorer(seg, values, queryVector));
      case EUCLIDEAN -> Optional.empty(); // of(new EuclideanScorer(msInput, values, queryVector));
      case MAXIMUM_INNER_PRODUCT ->
          Optional.empty(); // of(new MaxInnerProductScorer(msInput, values, queryVector));
    };
  }

  Lucene99MemorySegmentFloatVectorScorer(
      MemorySegment seg, FloatVectorValues values, float[] queryVector) {
    super(values);
    this.values = values;
    this.seg = seg;
    this.vectorByteSize = values.getVectorByteLength();
    this.query = queryVector;
  }

  static void checkInvariants(int maxOrd, int vectorByteLength, IndexInput input) {
    if (input.length() < (long) vectorByteLength * maxOrd) {
      throw new IllegalArgumentException("input length is less than expected vector data");
    }
  }

  final void checkOrdinal(int ord) {
    if (ord < 0 || ord >= maxOrd()) {
      throw new IllegalArgumentException("illegal ordinal: " + ord);
    }
  }

  static final class DotProductScorer extends Lucene99MemorySegmentFloatVectorScorer {

    static final MemorySegmentBulkVectorOps.DotProduct DOT_OPS =
        MemorySegmentBulkVectorOps.DOT_INSTANCE;

    final float[] scratchScores = new float[4];

    DotProductScorer(MemorySegment input, FloatVectorValues values, float[] query) {
      super(input, values, query);
    }

    @Override
    public float score(int node) throws IOException {
      checkOrdinal(node);
      // just delegates to existing scorer that copies on-heap
      return VectorSimilarityFunction.DOT_PRODUCT.compare(query, values.vectorValue(node));
    }

    @Override
    public void bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
      int i = 0;
      final int limit = numNodes & ~3;
      for (; i < limit; i += 4) {
        long addr1 = (long) nodes[i] * vectorByteSize;
        long addr2 = (long) nodes[i + 1] * vectorByteSize;
        long addr3 = (long) nodes[i + 2] * vectorByteSize;
        long addr4 = (long) nodes[i + 3] * vectorByteSize;
        DOT_OPS.dotProductBulk(seg, scratchScores, query, addr1, addr2, addr3, addr4, query.length);
        scores[i + 0] = normalizeToUnitInterval(scratchScores[0]);
        scores[i + 1] = normalizeToUnitInterval(scratchScores[1]);
        scores[i + 2] = normalizeToUnitInterval(scratchScores[2]);
        scores[i + 3] = normalizeToUnitInterval(scratchScores[3]);
      }
      // Handle remaining 1–3 nodes in bulk (if any)
      int remaining = numNodes - i;
      if (remaining > 0) {
        long addr1 = (long) nodes[i] * vectorByteSize;
        long addr2 = (remaining > 1) ? (long) nodes[i + 1] * vectorByteSize : addr1;
        long addr3 = (remaining > 2) ? (long) nodes[i + 2] * vectorByteSize : addr1;
        DOT_OPS.dotProductBulk(seg, scratchScores, query, addr1, addr2, addr3, addr1, query.length);
        scores[i] = normalizeToUnitInterval(scratchScores[0]);
        if (remaining > 1) scores[i + 1] = normalizeToUnitInterval(scratchScores[1]);
        if (remaining > 2) scores[i + 2] = normalizeToUnitInterval(scratchScores[2]);
      }
    }
  }
}
