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
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;

/** A score supplier of vectors whose element size is byte. */
public abstract sealed class Lucene99MemorySegmentFloatVectorScorerSupplier
    implements RandomVectorScorerSupplier {
  final int vectorByteSize;
  final int maxOrd;
  final int dims;
  final MemorySegment seg;
  final FloatVectorValues values; // to support ordToDoc/getAcceptOrds

  /**
   * Return an optional whose value, if present, is the scorer supplier. Otherwise, an empty
   * optional is returned.
   */
  static Optional<RandomVectorScorerSupplier> create(
      VectorSimilarityFunction type, IndexInput input, FloatVectorValues values)
      throws IOException {
    input = FilterIndexInput.unwrapOnlyTest(input);
    MemorySegment seg;
    if (!(input instanceof MemorySegmentAccessInput msInput
        && (seg = msInput.segmentSliceOrNull(0L, msInput.length())) != null)) {
      return Optional.empty();
    }
    checkInvariants(values.size(), values.getVectorByteLength(), input);
    return switch (type) {
      case COSINE -> Optional.of(new CosineSupplier(seg, values));
      case DOT_PRODUCT -> Optional.of(new DotProductSupplier(seg, values));
      case EUCLIDEAN -> Optional.of(new EuclideanSupplier(seg, values));
      case MAXIMUM_INNER_PRODUCT -> Optional.of(new MaxInnerProductSupplier(seg, values));
    };
  }

  Lucene99MemorySegmentFloatVectorScorerSupplier(MemorySegment seg, FloatVectorValues values) {
    this.seg = seg;
    this.values = values;
    this.vectorByteSize = values.getVectorByteLength();
    this.maxOrd = values.size();
    this.dims = values.dimension();
  }

  static void checkInvariants(int maxOrd, int vectorByteLength, IndexInput input) {
    if (input.length() < (long) vectorByteLength * maxOrd) {
      throw new IllegalArgumentException("input length is less than expected vector data");
    }
  }

  static final class CosineSupplier extends Lucene99MemorySegmentFloatVectorScorerSupplier {

    static final MemorySegmentBulkVectorOps.Cosine COS_OPS =
        MemorySegmentBulkVectorOps.COS_INSTANCE;

    CosineSupplier(MemorySegment seg, FloatVectorValues values) {
      super(seg, values);
    }

    @Override
    public UpdateableRandomVectorScorer scorer() {
      return new AbstractBulkScorer(values) {
        @Override
        float vectorOp(MemorySegment seg, long q, long d, int elementCount) {
          return COS_OPS.cosine(seg, q, d, dims);
        }

        @Override
        void vectorOp(
            MemorySegment seg,
            float[] scores,
            long queryOffset,
            long node1Offset,
            long node2Offset,
            long node3Offset,
            long node4Offset,
            int elementCount) {
          COS_OPS.cosineBulk(
              seg, scores, queryOffset, node1Offset, node2Offset, node3Offset, node4Offset, dims);
        }

        @Override
        float normalizeRawScore(float rawScore) {
          return VectorUtil.normalizeToUnitInterval(rawScore);
        }
      };
    }

    @Override
    public CosineSupplier copy() throws IOException {
      return new CosineSupplier(seg, values.copy()); // TODO: check copy
    }
  }

  static final class DotProductSupplier extends Lucene99MemorySegmentFloatVectorScorerSupplier {

    static final MemorySegmentBulkVectorOps.DotProduct DOT_OPS =
        MemorySegmentBulkVectorOps.DOT_INSTANCE;

    DotProductSupplier(MemorySegment seg, FloatVectorValues values) {
      super(seg, values);
    }

    @Override
    public UpdateableRandomVectorScorer scorer() {
      return new AbstractBulkScorer(values) {
        @Override
        float vectorOp(MemorySegment seg, long q, long d, int elementCount) {
          return DOT_OPS.dotProduct(seg, q, d, dims);
        }

        @Override
        void vectorOp(
            MemorySegment seg,
            float[] scores,
            long queryOffset,
            long node1Offset,
            long node2Offset,
            long node3Offset,
            long node4Offset,
            int elementCount) {
          DOT_OPS.dotProductBulk(
              seg, scores, queryOffset, node1Offset, node2Offset, node3Offset, node4Offset, dims);
        }

        @Override
        float normalizeRawScore(float rawScore) {
          return VectorUtil.normalizeToUnitInterval(rawScore);
        }
      };
    }

    @Override
    public DotProductSupplier copy() throws IOException {
      return new DotProductSupplier(seg, values);
    }
  }

  static final class EuclideanSupplier extends Lucene99MemorySegmentFloatVectorScorerSupplier {

    static final MemorySegmentBulkVectorOps.SqrDistance SQR_OPS =
        MemorySegmentBulkVectorOps.SQR_INSTANCE;

    EuclideanSupplier(MemorySegment seg, FloatVectorValues values) {
      super(seg, values);
    }

    @Override
    public UpdateableRandomVectorScorer scorer() {
      return new AbstractBulkScorer(values) {
        @Override
        float vectorOp(MemorySegment seg, long q, long d, int elementCount) {
          return SQR_OPS.sqrDistance(seg, q, d, dims);
        }

        @Override
        void vectorOp(
            MemorySegment seg,
            float[] scores,
            long queryOffset,
            long node1Offset,
            long node2Offset,
            long node3Offset,
            long node4Offset,
            int elementCount) {
          SQR_OPS.sqrDistanceBulk(
              seg, scores, queryOffset, node1Offset, node2Offset, node3Offset, node4Offset, dims);
        }

        @Override
        float normalizeRawScore(float rawScore) {
          return VectorUtil.normalizeDistanceToUnitInterval(rawScore);
        }
      };
    }

    @Override
    public EuclideanSupplier copy() throws IOException {
      return new EuclideanSupplier(seg, values); // TODO: need to copy ?
    }
  }

  static final class MaxInnerProductSupplier
      extends Lucene99MemorySegmentFloatVectorScorerSupplier {

    static final MemorySegmentBulkVectorOps.DotProduct DOT_OPS =
        MemorySegmentBulkVectorOps.DOT_INSTANCE;

    MaxInnerProductSupplier(MemorySegment seg, FloatVectorValues values) {
      super(seg, values);
    }

    @Override
    public UpdateableRandomVectorScorer scorer() {
      return new AbstractBulkScorer(values) {
        @Override
        float vectorOp(MemorySegment seg, long q, long d, int elementCount) {
          return DOT_OPS.dotProduct(seg, q, d, dims);
        }

        @Override
        void vectorOp(
            MemorySegment seg,
            float[] scores,
            long queryOffset,
            long node1Offset,
            long node2Offset,
            long node3Offset,
            long node4Offset,
            int elementCount) {
          DOT_OPS.dotProductBulk(
              seg, scores, queryOffset, node1Offset, node2Offset, node3Offset, node4Offset, dims);
        }

        @Override
        float normalizeRawScore(float rawScore) {
          return VectorUtil.scaleMaxInnerProductScore(rawScore);
        }
      };
    }

    @Override
    public MaxInnerProductSupplier copy() throws IOException {
      return new MaxInnerProductSupplier(seg, values);
    }
  }

  abstract class AbstractBulkScorer
      extends UpdateableRandomVectorScorer.AbstractUpdateableRandomVectorScorer {
    private int queryOrd;
    final float[] scratchScores = new float[4];

    AbstractBulkScorer(FloatVectorValues values) {
      super(values);
    }

    final void checkOrdinal(int ord) {
      if (ord < 0 || ord >= maxOrd) {
        throw new IllegalArgumentException("illegal ordinal: " + ord);
      }
    }

    abstract float vectorOp(MemorySegment seg, long q, long d, int elementCount);

    abstract void vectorOp(
        MemorySegment seg,
        float[] scores,
        long queryOffset,
        long node1Offset,
        long node2Offset,
        long node3Offset,
        long node4Offset,
        int elementCount);

    abstract float normalizeRawScore(float rawScore);

    @Override
    public float score(int node) {
      checkOrdinal(node);
      long queryAddr = (long) queryOrd * vectorByteSize;
      long addr = (long) node * vectorByteSize;
      var raw = vectorOp(seg, queryAddr, addr, dims);
      return normalizeRawScore(raw);
    }

    @Override
    public float bulkScore(int[] nodes, float[] scores, int numNodes) {
      int i = 0;
      long queryAddr = (long) queryOrd * vectorByteSize;
      float maxScore = Float.NEGATIVE_INFINITY;
      final int limit = numNodes & ~3;
      for (; i < limit; i += 4) {
        long offset1 = (long) nodes[i] * vectorByteSize;
        long offset2 = (long) nodes[i + 1] * vectorByteSize;
        long offset3 = (long) nodes[i + 2] * vectorByteSize;
        long offset4 = (long) nodes[i + 3] * vectorByteSize;
        vectorOp(seg, scratchScores, queryAddr, offset1, offset2, offset3, offset4, dims);
        scores[i + 0] = normalizeRawScore(scratchScores[0]);
        maxScore = Math.max(maxScore, scores[i + 0]);
        scores[i + 1] = normalizeRawScore(scratchScores[1]);
        maxScore = Math.max(maxScore, scores[i + 1]);
        scores[i + 2] = normalizeRawScore(scratchScores[2]);
        maxScore = Math.max(maxScore, scores[i + 2]);
        scores[i + 3] = normalizeRawScore(scratchScores[3]);
        maxScore = Math.max(maxScore, scores[i + 3]);
      }
      // Handle remaining 1–3 nodes in bulk (if any)
      int remaining = numNodes - i;
      if (remaining > 0) {
        long addr1 = (long) nodes[i] * vectorByteSize;
        long addr2 = (remaining > 1) ? (long) nodes[i + 1] * vectorByteSize : addr1;
        long addr3 = (remaining > 2) ? (long) nodes[i + 2] * vectorByteSize : addr1;
        vectorOp(seg, scratchScores, queryAddr, addr1, addr2, addr3, addr1, dims);
        scores[i] = normalizeRawScore(scratchScores[0]);
        maxScore = Math.max(maxScore, scores[i]);
        if (remaining > 1) {
          scores[i + 1] = normalizeRawScore(scratchScores[1]);
          maxScore = Math.max(maxScore, scores[i + 1]);
        }
        if (remaining > 2) {
          scores[i + 2] = normalizeRawScore(scratchScores[2]);
          maxScore = Math.max(maxScore, scores[i + 2]);
        }
      }
      return maxScore;
    }

    @Override
    public void setScoringOrdinal(int node) {
      checkOrdinal(node);
      queryOrd = node;
    }
  }
}
