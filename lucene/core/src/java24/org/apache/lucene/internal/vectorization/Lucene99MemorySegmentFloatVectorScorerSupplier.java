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
  final MemorySegmentAccessInput input;
  final FloatVectorValues values; // to support ordToDoc/getAcceptOrds
  byte[] queryScratch, scratch1, scratch2, scratch3, scratch4;

  /**
   * Return an optional whose value, if present, is the scorer supplier. Otherwise, an empty
   * optional is returned.
   */
  static Optional<RandomVectorScorerSupplier> create(
      VectorSimilarityFunction type, IndexInput input, FloatVectorValues values) {
    input = FilterIndexInput.unwrapOnlyTest(input);
    if (!(input instanceof MemorySegmentAccessInput msInput)) {
      return Optional.empty();
    }
    checkInvariants(values.size(), values.getVectorByteLength(), input);
    return switch (type) {
      case COSINE -> Optional.of(new CosineSupplier(msInput, values));
      case DOT_PRODUCT -> Optional.of(new DotProductSupplier(msInput, values));
      case EUCLIDEAN -> Optional.of(new EuclideanSupplier(msInput, values));
      case MAXIMUM_INNER_PRODUCT -> Optional.of(new MaxInnerProductSupplier(msInput, values));
    };
  }

  Lucene99MemorySegmentFloatVectorScorerSupplier(
      MemorySegmentAccessInput input, FloatVectorValues values) {
    this.input = input;
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

  final MemorySegment getSegment(int ord, byte[] scratch) throws IOException {
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

  static final class CosineSupplier extends Lucene99MemorySegmentFloatVectorScorerSupplier {

    static final MemorySegmentBulkVectorOps.CosineFromQuerySegment COS_OPS =
        new MemorySegmentBulkVectorOps.CosineFromQuerySegment();

    CosineSupplier(MemorySegmentAccessInput input, FloatVectorValues values) {
      super(input, values);
    }

    @Override
    public UpdateableRandomVectorScorer scorer() {
      return new AbstractBulkScorer(values) {
        @Override
        float vectorOp(MemorySegment query, MemorySegment ms, int elementCount) {
          return COS_OPS.cosineBulk(query, ms, dims);
        }

        @Override
        void vectorOp(
            float[] scores,
            MemorySegment query,
            MemorySegment ms1,
            MemorySegment ms2,
            MemorySegment ms3,
            MemorySegment ms4,
            int elementCount) {
          COS_OPS.cosineBulk(scores, query, ms1, ms2, ms3, ms4, dims);
        }

        @Override
        float normalizeRawScore(float rawScore) {
          return VectorUtil.normalizeToUnitInterval(rawScore);
        }
      };
    }

    @Override
    public CosineSupplier copy() throws IOException {
      return new CosineSupplier(input.clone(), values);
    }
  }

  static final class DotProductSupplier extends Lucene99MemorySegmentFloatVectorScorerSupplier {

    static final MemorySegmentBulkVectorOps.DotFromQuerySegment DOT_OPS =
        new MemorySegmentBulkVectorOps.DotFromQuerySegment();

    DotProductSupplier(MemorySegmentAccessInput input, FloatVectorValues values) {
      super(input, values);
    }

    @Override
    public UpdateableRandomVectorScorer scorer() {
      return new AbstractBulkScorer(values) {
        @Override
        float vectorOp(MemorySegment query, MemorySegment ms, int elementCount) {
          return DOT_OPS.dotProduct(query, ms, dims);
        }

        @Override
        void vectorOp(
            float[] scores,
            MemorySegment query,
            MemorySegment ms1,
            MemorySegment ms2,
            MemorySegment ms3,
            MemorySegment ms4,
            int elementCount) {
          DOT_OPS.dotProductBulk(scores, query, ms1, ms2, ms3, ms4, dims);
        }

        @Override
        float normalizeRawScore(float rawScore) {
          return VectorUtil.normalizeToUnitInterval(rawScore);
        }
      };
    }

    @Override
    public DotProductSupplier copy() throws IOException {
      return new DotProductSupplier(input.clone(), values);
    }
  }

  static final class EuclideanSupplier extends Lucene99MemorySegmentFloatVectorScorerSupplier {

    static final MemorySegmentBulkVectorOps.SqrDistanceFromQuerySegment SQR_OPS =
        new MemorySegmentBulkVectorOps.SqrDistanceFromQuerySegment();

    EuclideanSupplier(MemorySegmentAccessInput input, FloatVectorValues values) {
      super(input, values);
    }

    @Override
    public UpdateableRandomVectorScorer scorer() {
      return new AbstractBulkScorer(values) {
        @Override
        float vectorOp(MemorySegment query, MemorySegment ms, int elementCount) {
          return SQR_OPS.sqrDistance(query, ms, dims);
        }

        @Override
        void vectorOp(
            float[] scores,
            MemorySegment query,
            MemorySegment ms1,
            MemorySegment ms2,
            MemorySegment ms3,
            MemorySegment ms4,
            int elementCount) {
          SQR_OPS.sqrDistanceBulk(scores, query, ms1, ms2, ms3, ms4, dims);
        }

        @Override
        float normalizeRawScore(float rawScore) {
          return VectorUtil.normalizeDistanceToUnitInterval(rawScore);
        }
      };
    }

    @Override
    public EuclideanSupplier copy() throws IOException {
      return new EuclideanSupplier(input.clone(), values);
    }
  }

  static final class MaxInnerProductSupplier
      extends Lucene99MemorySegmentFloatVectorScorerSupplier {

    static final MemorySegmentBulkVectorOps.DotFromQuerySegment DOT_OPS =
        new MemorySegmentBulkVectorOps.DotFromQuerySegment();

    MaxInnerProductSupplier(MemorySegmentAccessInput input, FloatVectorValues values) {
      super(input, values);
    }

    @Override
    public UpdateableRandomVectorScorer scorer() {
      return new AbstractBulkScorer(values) {
        @Override
        float vectorOp(MemorySegment query, MemorySegment ms, int elementCount) {
          return DOT_OPS.dotProduct(query, ms, dims);
        }

        @Override
        void vectorOp(
            float[] scores,
            MemorySegment query,
            MemorySegment ms1,
            MemorySegment ms2,
            MemorySegment ms3,
            MemorySegment ms4,
            int elementCount) {
          DOT_OPS.dotProductBulk(scores, query, ms1, ms2, ms3, ms4, dims);
        }

        @Override
        float normalizeRawScore(float rawScore) {
          return VectorUtil.scaleMaxInnerProductScore(rawScore);
        }
      };
    }

    @Override
    public MaxInnerProductSupplier copy() throws IOException {
      return new MaxInnerProductSupplier(input.clone(), values);
    }
  }

  abstract class AbstractBulkScorer
      extends UpdateableRandomVectorScorer.AbstractUpdateableRandomVectorScorer {
    private int queryOrd;

    AbstractBulkScorer(FloatVectorValues values) {
      super(values);
    }

    final void checkOrdinal(int ord) {
      if (ord < 0 || ord >= maxOrd) {
        throw new IllegalArgumentException("illegal ordinal: " + ord);
      }
    }

    abstract float vectorOp(MemorySegment query, MemorySegment ms, int elementCount);

    abstract void vectorOp(
        float[] scores,
        MemorySegment query,
        MemorySegment ms1,
        MemorySegment ms2,
        MemorySegment ms3,
        MemorySegment ms4,
        int elementCount);

    abstract float normalizeRawScore(float rawScore);

    @Override
    public float score(int node) throws IOException {
      checkOrdinal(node);
      MemorySegment query = getSegment(queryOrd, queryScratch);
      MemorySegment ms = getSegment(node, scratch1);
      float raw = vectorOp(query, ms, dims);
      return normalizeRawScore(raw);
    }

    @Override
    public void bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
      float[] scratchScores = new float[4];
      int i = 0;
      MemorySegment query = getSegment(queryOrd, queryScratch);
      final int limit = numNodes & ~3;
      for (; i < limit; i += 4) {
        MemorySegment ms1 = getSegment(nodes[i], scratch1);
        MemorySegment ms2 = getSegment(nodes[i + 1], scratch2);
        MemorySegment ms3 = getSegment(nodes[i + 2], scratch3);
        MemorySegment ms4 = getSegment(nodes[i + 3], scratch4);
        vectorOp(scratchScores, query, ms1, ms2, ms3, ms4, dims);
        scores[i + 0] = normalizeRawScore(scratchScores[0]);
        scores[i + 1] = normalizeRawScore(scratchScores[1]);
        scores[i + 2] = normalizeRawScore(scratchScores[2]);
        scores[i + 3] = normalizeRawScore(scratchScores[3]);
      }
      // Handle remaining 1–3 nodes in bulk (if any)
      int remaining = numNodes - i;
      if (remaining > 0) {
        MemorySegment ms1 = getSegment(nodes[i], scratch1);
        MemorySegment ms2 = (remaining > 1) ? getSegment(nodes[i + 1], scratch2) : ms1;
        MemorySegment ms3 = (remaining > 2) ? getSegment(nodes[i + 2], scratch3) : ms1;
        vectorOp(scratchScores, query, ms1, ms2, ms3, ms1, dims);
        scores[i] = normalizeRawScore(scratchScores[0]);
        if (remaining > 1) scores[i + 1] = normalizeRawScore(scratchScores[1]);
        if (remaining > 2) scores[i + 2] = normalizeRawScore(scratchScores[2]);
      }
    }

    @Override
    public void setScoringOrdinal(int node) {
      checkOrdinal(node);
      queryOrd = node;
    }
  }
}
