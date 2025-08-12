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
  final MemorySegmentAccessInput input;
  final float[] query;
  byte[] scratch;

  /**
   * Return an optional whose value, if present, is the scorer. Otherwise, an empty optional is
   * returned.
   */
  public static Optional<Lucene99MemorySegmentFloatVectorScorer> create(
      VectorSimilarityFunction type,
      IndexInput input,
      FloatVectorValues values,
      float[] queryVector) {
    input = FilterIndexInput.unwrapOnlyTest(input);
    if (!(input instanceof MemorySegmentAccessInput msInput)) {
      return Optional.empty();
    }
    checkInvariants(values.size(), values.getVectorByteLength(), input);
    return switch (type) {
      case COSINE -> Optional.empty(); // of(new CosineScorer(msInput, values, queryVector));
      case DOT_PRODUCT -> Optional.of(new DotProductScorer(msInput, values, queryVector));
      case EUCLIDEAN -> Optional.empty(); // of(new EuclideanScorer(msInput, values, queryVector));
      case MAXIMUM_INNER_PRODUCT ->
          Optional.empty(); // of(new MaxInnerProductScorer(msInput, values, queryVector));
    };
  }

  Lucene99MemorySegmentFloatVectorScorer(
      MemorySegmentAccessInput input, FloatVectorValues values, float[] queryVector) {
    super(values);
    this.values = values;
    this.input = input;
    this.vectorByteSize = values.getVectorByteLength();
    this.query = queryVector;
  }

  final MemorySegment getSegment(int ord) throws IOException {
    checkOrdinal(ord);
    long byteOffset = (long) ord * vectorByteSize;
    MemorySegment seg = input.segmentSliceOrNull(byteOffset, vectorByteSize);
    if (seg == null) {
      if (scratch == null) {
        scratch = new byte[vectorByteSize];
      }
      input.readBytes(byteOffset, scratch, 0, vectorByteSize);
      seg = MemorySegment.ofArray(scratch);
    }
    return seg;
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

    static final MemorySegmentBulkVectorOps.DotFromQueryArray DOT_OPS =
        new MemorySegmentBulkVectorOps.DotFromQueryArray();

    DotProductScorer(MemorySegmentAccessInput input, FloatVectorValues values, float[] query) {
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
      float[] scratchScores = new float[4];
      int i = 0;
      final int limit = numNodes & ~3;
      for (; i < limit; i += 4) {
        MemorySegment ms1 = getSegment(nodes[i]);
        MemorySegment ms2 = getSegment(nodes[i + 1]);
        MemorySegment ms3 = getSegment(nodes[i + 2]);
        MemorySegment ms4 = getSegment(nodes[i + 3]);
        DOT_OPS.dotProductBulk(scratchScores, query, ms1, ms2, ms3, ms4, query.length);
        scores[i + 0] = normalizeToUnitInterval(scratchScores[0]);
        scores[i + 1] = normalizeToUnitInterval(scratchScores[1]);
        scores[i + 2] = normalizeToUnitInterval(scratchScores[2]);
        scores[i + 3] = normalizeToUnitInterval(scratchScores[3]);
      }
      // Handle remaining 1–3 nodes in bulk (if any)
      int remaining = numNodes - i;
      if (remaining > 0) {
        MemorySegment ms1 = getSegment(nodes[i]);
        MemorySegment ms2 = (remaining > 1) ? getSegment(nodes[i + 1]) : ms1;
        MemorySegment ms3 = (remaining > 2) ? getSegment(nodes[i + 2]) : ms1;
        DOT_OPS.dotProductBulk(scratchScores, query, ms1, ms2, ms3, ms1, query.length);
        scores[i] = normalizeToUnitInterval(scratchScores[0]);
        if (remaining > 1) scores[i + 1] = normalizeToUnitInterval(scratchScores[1]);
        if (remaining > 2) scores[i + 2] = normalizeToUnitInterval(scratchScores[2]);
      }
    }
  }
}
