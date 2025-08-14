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
  final MemorySegment seg;
  final float[] query;
  final float[] scratchScores = new float[4];

  /**
   * Return an optional whose value, if present, is the scorer. Otherwise, an empty optional is
   * returned.
   */
  public static Optional<Lucene99MemorySegmentFloatVectorScorer> create(
      VectorSimilarityFunction type, IndexInput input, FloatVectorValues values, float[] query)
      throws IOException {
    input = FilterIndexInput.unwrapOnlyTest(input);
    MemorySegment seg;
    if (!(input instanceof MemorySegmentAccessInput msInput
        && (seg = msInput.segmentSliceOrNull(0L, msInput.length())) != null)) {
      return Optional.empty();
    }
    checkInvariants(values.size(), values.getVectorByteLength(), input);
    return switch (type) {
      case COSINE -> Optional.of(new CosineScorer(seg, values, query));
      case DOT_PRODUCT -> Optional.of(new DotProductScorer(seg, values, query));
      case EUCLIDEAN -> Optional.of(new EuclideanScorer(seg, values, query));
      case MAXIMUM_INNER_PRODUCT -> Optional.of(new MaxInnerProductScorer(seg, values, query));
    };
  }

  Lucene99MemorySegmentFloatVectorScorer(
      MemorySegment seg, FloatVectorValues values, float[] query) {
    super(values);
    this.values = values;
    this.seg = seg;
    this.vectorByteSize = values.getVectorByteLength();
    this.query = query;
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
    int i = 0;
    final int limit = numNodes & ~3;
    for (; i < limit; i += 4) {
      long offset1 = (long) nodes[i] * vectorByteSize;
      long offset2 = (long) nodes[i + 1] * vectorByteSize;
      long offset3 = (long) nodes[i + 2] * vectorByteSize;
      long offset4 = (long) nodes[i + 3] * vectorByteSize;
      vectorOp(seg, scratchScores, offset1, offset2, offset3, offset4, query.length);
      scores[i + 0] = normalizeRawScore(scratchScores[0]);
      scores[i + 1] = normalizeRawScore(scratchScores[1]);
      scores[i + 2] = normalizeRawScore(scratchScores[2]);
      scores[i + 3] = normalizeRawScore(scratchScores[3]);
    }
    // Handle remaining 1–3 nodes in bulk (if any)
    int remaining = numNodes - i;
    if (remaining > 0) {
      long addr1 = (long) nodes[i] * vectorByteSize;
      long addr2 = (remaining > 1) ? (long) nodes[i + 1] * vectorByteSize : addr1;
      long addr3 = (remaining > 2) ? (long) nodes[i + 2] * vectorByteSize : addr1;
      vectorOp(seg, scratchScores, addr1, addr2, addr3, addr3, query.length);
      scores[i] = normalizeRawScore(scratchScores[0]);
      if (remaining > 1) scores[i + 1] = normalizeRawScore(scratchScores[1]);
      if (remaining > 2) scores[i + 2] = normalizeRawScore(scratchScores[2]);
    }
  }

  abstract void vectorOp(
      MemorySegment seg,
      float[] scores,
      long node1Offset,
      long node2Offset,
      long node3Offset,
      long node4Offset,
      int elementCount);

  abstract float normalizeRawScore(float value);

  static final class CosineScorer extends Lucene99MemorySegmentFloatVectorScorer {

    static final MemorySegmentBulkVectorOps.Cosine COS_OPS =
        MemorySegmentBulkVectorOps.COS_INSTANCE;

    CosineScorer(MemorySegment seg, FloatVectorValues values, float[] query) {
      super(seg, values, query);
    }

    @Override
    public float score(int node) throws IOException {
      checkOrdinal(node);
      // just delegates to existing scorer that copies on-heap
      return VectorSimilarityFunction.COSINE.compare(query, values.vectorValue(node));
    }

    @Override
    void vectorOp(
        MemorySegment seg,
        float[] scores,
        long node1Offset,
        long node2Offset,
        long node3Offset,
        long node4Offset,
        int elementCount) {
      COS_OPS.cosineBulk(
          seg, scores, query, node1Offset, node2Offset, node3Offset, node4Offset, elementCount);
    }

    @Override
    float normalizeRawScore(float rawScore) {
      return VectorUtil.normalizeToUnitInterval(rawScore);
    }
  }

  static final class DotProductScorer extends Lucene99MemorySegmentFloatVectorScorer {

    static final MemorySegmentBulkVectorOps.DotProduct DOT_OPS =
        MemorySegmentBulkVectorOps.DOT_INSTANCE;

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
    void vectorOp(
        MemorySegment seg,
        float[] scores,
        long node1Offset,
        long node2Offset,
        long node3Offset,
        long node4Offset,
        int elementCount) {
      DOT_OPS.dotProductBulk(
          seg, scores, query, node1Offset, node2Offset, node3Offset, node4Offset, elementCount);
    }

    @Override
    float normalizeRawScore(float rawScore) {
      return VectorUtil.normalizeToUnitInterval(rawScore);
    }
  }

  static final class EuclideanScorer extends Lucene99MemorySegmentFloatVectorScorer {

    static final MemorySegmentBulkVectorOps.SqrDistance SQR_OPS =
        MemorySegmentBulkVectorOps.SQR_INSTANCE;

    EuclideanScorer(MemorySegment seg, FloatVectorValues values, float[] query) {
      super(seg, values, query);
    }

    @Override
    public float score(int node) throws IOException {
      checkOrdinal(node);
      // just delegates to existing scorer that copies on-heap
      return VectorSimilarityFunction.EUCLIDEAN.compare(query, values.vectorValue(node));
    }

    @Override
    void vectorOp(
        MemorySegment seg,
        float[] scores,
        long node1Offset,
        long node2Offset,
        long node3Offset,
        long node4Offset,
        int elementCount) {
      SQR_OPS.sqrDistanceBulk(
          seg, scores, query, node1Offset, node2Offset, node3Offset, node4Offset, elementCount);
    }

    @Override
    float normalizeRawScore(float rawScore) {
      return VectorUtil.normalizeDistanceToUnitInterval(rawScore);
    }
  }

  static final class MaxInnerProductScorer extends Lucene99MemorySegmentFloatVectorScorer {

    static final MemorySegmentBulkVectorOps.DotProduct DOT_OPS =
        MemorySegmentBulkVectorOps.DOT_INSTANCE;

    MaxInnerProductScorer(MemorySegment seg, FloatVectorValues values, float[] query) {
      super(seg, values, query);
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
        MemorySegment seg,
        float[] scores,
        long node1Offset,
        long node2Offset,
        long node3Offset,
        long node4Offset,
        int elementCount) {
      DOT_OPS.dotProductBulk(
          seg, scores, query, node1Offset, node2Offset, node3Offset, node4Offset, elementCount);
    }

    @Override
    float normalizeRawScore(float rawScore) {
      return VectorUtil.scaleMaxInnerProductScore(rawScore);
    }
  }
}
