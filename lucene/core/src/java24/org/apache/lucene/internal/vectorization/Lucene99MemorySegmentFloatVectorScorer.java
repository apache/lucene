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

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Optional;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.util.VectorUtil;
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
      VectorSimilarityFunction type, IndexInput input, FloatVectorValues values, float[] query) {
    input = FilterIndexInput.unwrapOnlyTest(input);
    if (!(input instanceof MemorySegmentAccessInput msInput)) {
      return Optional.empty();
    }
    checkInvariants(values.size(), values.getVectorByteLength(), input);
    return switch (type) {
      case COSINE -> Optional.of(new CosineScorer(msInput, values, query));
      case DOT_PRODUCT -> Optional.of(new DotProductScorer(msInput, values, query));
      case EUCLIDEAN -> Optional.of(new EuclideanScorer(msInput, values, query));
      case MAXIMUM_INNER_PRODUCT -> Optional.of(new MaxInnerProductScorer(msInput, values, query));
    };
  }

  Lucene99MemorySegmentFloatVectorScorer(
      MemorySegmentAccessInput input, FloatVectorValues values, float[] query) {
    super(values);
    this.values = values;
    this.input = input;
    this.vectorByteSize = values.getVectorByteLength();
    this.query = query;
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
      vectorOp(scratchScores, query, ms1, ms2, ms3, ms4, query.length);
      scores[i + 0] = normalizeRawScore(scratchScores[0]);
      scores[i + 1] = normalizeRawScore(scratchScores[1]);
      scores[i + 2] = normalizeRawScore(scratchScores[2]);
      scores[i + 3] = normalizeRawScore(scratchScores[3]);
    }
    // Handle remaining 1–3 nodes in bulk (if any)
    int remaining = numNodes - i;
    if (remaining > 0) {
      MemorySegment ms1 = getSegment(nodes[i]);
      MemorySegment ms2 = (remaining > 1) ? getSegment(nodes[i + 1]) : ms1;
      MemorySegment ms3 = (remaining > 2) ? getSegment(nodes[i + 2]) : ms1;
      vectorOp(scratchScores, query, ms1, ms2, ms3, ms1, query.length);
      scores[i] = normalizeRawScore(scratchScores[0]);
      if (remaining > 1) scores[i + 1] = normalizeRawScore(scratchScores[1]);
      if (remaining > 2) scores[i + 2] = normalizeRawScore(scratchScores[2]);
    }
  }

  abstract void vectorOp(
      float[] scores,
      float[] query,
      MemorySegment ms1,
      MemorySegment ms2,
      MemorySegment ms3,
      MemorySegment ms4,
      int elementCount);

  abstract float normalizeRawScore(float value);

  static final class CosineScorer extends Lucene99MemorySegmentFloatVectorScorer {

    static final MemorySegmentBulkVectorOps.CosineFromQueryArray COS_OPS =
        new MemorySegmentBulkVectorOps.CosineFromQueryArray();

    CosineScorer(MemorySegmentAccessInput input, FloatVectorValues values, float[] query) {
      super(input, values, query);
    }

    @Override
    public float score(int node) throws IOException {
      checkOrdinal(node);
      // just delegates to existing scorer that copies on-heap
      return VectorSimilarityFunction.COSINE.compare(query, values.vectorValue(node));
    }

    @Override
    void vectorOp(
        float[] scores,
        float[] query,
        MemorySegment ms1,
        MemorySegment ms2,
        MemorySegment ms3,
        MemorySegment ms4,
        int elementCount) {
      COS_OPS.cosineBulk(scores, query, ms1, ms2, ms3, ms4, elementCount);
    }

    @Override
    float normalizeRawScore(float rawScore) {
      return VectorUtil.normalizeToUnitInterval(rawScore);
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
    void vectorOp(
        float[] scores,
        float[] query,
        MemorySegment ms1,
        MemorySegment ms2,
        MemorySegment ms3,
        MemorySegment ms4,
        int elementCount) {
      DOT_OPS.dotProductBulk(scores, query, ms1, ms2, ms3, ms4, elementCount);
    }

    @Override
    float normalizeRawScore(float rawScore) {
      return VectorUtil.normalizeToUnitInterval(rawScore);
    }
  }

  static final class EuclideanScorer extends Lucene99MemorySegmentFloatVectorScorer {

    static final MemorySegmentBulkVectorOps.SqrDistanceFromQueryArray SQR_OPS =
        new MemorySegmentBulkVectorOps.SqrDistanceFromQueryArray();

    EuclideanScorer(MemorySegmentAccessInput input, FloatVectorValues values, float[] query) {
      super(input, values, query);
    }

    @Override
    public float score(int node) throws IOException {
      checkOrdinal(node);
      // just delegates to existing scorer that copies on-heap
      return VectorSimilarityFunction.EUCLIDEAN.compare(query, values.vectorValue(node));
    }

    @Override
    void vectorOp(
        float[] scores,
        float[] query,
        MemorySegment ms1,
        MemorySegment ms2,
        MemorySegment ms3,
        MemorySegment ms4,
        int elementCount) {
      SQR_OPS.sqrDistanceBulk(scores, query, ms1, ms2, ms3, ms4, elementCount);
    }

    @Override
    float normalizeRawScore(float rawScore) {
      return VectorUtil.normalizeDistanceToUnitInterval(rawScore);
    }
  }

  static final class MaxInnerProductScorer extends Lucene99MemorySegmentFloatVectorScorer {

    static final MemorySegmentBulkVectorOps.DotFromQueryArray DOT_OPS =
        new MemorySegmentBulkVectorOps.DotFromQueryArray();

    MaxInnerProductScorer(MemorySegmentAccessInput input, FloatVectorValues values, float[] query) {
      super(input, values, query);
    }

    @Override
    public float score(int node) throws IOException {
      checkOrdinal(node);
      // just delegates to existing scorer that copies on-heap
      return VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT.compare(
          query, values.vectorValue(node));
    }

    @Override
    void vectorOp(
        float[] scores,
        float[] query,
        MemorySegment ms1,
        MemorySegment ms2,
        MemorySegment ms3,
        MemorySegment ms4,
        int elementCount) {
      DOT_OPS.dotProductBulk(scores, query, ms1, ms2, ms3, ms4, elementCount);
    }

    @Override
    float normalizeRawScore(float rawScore) {
      return VectorUtil.scaleMaxInnerProductScore(rawScore);
    }
  }
}
