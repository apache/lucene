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
import org.apache.lucene.index.Float16VectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;

/** A score supplier of vectors whose element size is byte. */
public abstract sealed class Lucene99MemorySegmentFloat16VectorScorerSupplier
    implements RandomVectorScorerSupplier {
  final int vectorByteSize;
  final int maxOrd;
  final int dims;
  final MemorySegment seg;
  final Float16VectorValues values; // to support ordToDoc/getAcceptOrds

  /**
   * Return an optional whose value, if present, is the scorer supplier. Otherwise, an empty
   * optional is returned.
   */
  static Optional<RandomVectorScorerSupplier> create(
      VectorSimilarityFunction type, IndexInput input, Float16VectorValues values)
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

  Lucene99MemorySegmentFloat16VectorScorerSupplier(MemorySegment seg, Float16VectorValues values) {
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

  final void checkOrdinal(int ord) {
    if (ord < 0 || ord >= maxOrd) {
      throw new IllegalArgumentException("illegal ordinal: " + ord);
    }
  }


  static final class CosineSupplier extends Lucene99MemorySegmentFloat16VectorScorerSupplier {

    static final MemorySegmentBulkVectorOps.Cosine COS_OPS =
        MemorySegmentBulkVectorOps.COS_INSTANCE;

    CosineSupplier(MemorySegment seg, Float16VectorValues values) {
      super(seg, values);
    }

    @Override
    public UpdateableRandomVectorScorer scorer() {
      return null;
    }

    @Override
    public CosineSupplier copy() throws IOException {
      return new CosineSupplier(seg, values.copy()); // TODO: check copy
    }
  }

  static final class DotProductSupplier extends Lucene99MemorySegmentFloat16VectorScorerSupplier {

    DotProductSupplier(MemorySegment seg, Float16VectorValues values) {
      super(seg, values);
    }

    @Override
    public UpdateableRandomVectorScorer scorer() {
      return new UpdateableRandomVectorScorer.AbstractUpdateableRandomVectorScorer(values) {
        private int queryOrd = 0;

        @Override
        public float score(int node) {
          checkOrdinal(node);
          long queryAddr = (long) queryOrd * vectorByteSize;
          long addr = (long) node * vectorByteSize;
          float rawScore = vectorOp(seg, queryAddr, addr);
          return VectorUtil.normalizeToUnitInterval(rawScore);
        }

        float vectorOp(MemorySegment seg, long q, long d) {
          return Float.float16ToFloat(PanamaVectorUtilSupport.dotProduct(seg, q, d, dims));
        }

        @Override
        public void setScoringOrdinal(int node) {
          checkOrdinal(node);
          queryOrd = node;
        }
      };
    }

    @Override
    public DotProductSupplier copy() throws IOException {
      return new DotProductSupplier(seg, values);
    }
  }

  static final class EuclideanSupplier extends Lucene99MemorySegmentFloat16VectorScorerSupplier {

    static final MemorySegmentBulkVectorOps.SqrDistance SQR_OPS =
        MemorySegmentBulkVectorOps.SQR_INSTANCE;

    EuclideanSupplier(MemorySegment seg, Float16VectorValues values) {
      super(seg, values);
    }

    @Override
    public UpdateableRandomVectorScorer scorer() {
      return null;
    }

    @Override
    public EuclideanSupplier copy() throws IOException {
      return new EuclideanSupplier(seg, values); // TODO: need to copy ?
    }
  }

  static final class MaxInnerProductSupplier
      extends Lucene99MemorySegmentFloat16VectorScorerSupplier {

    static final MemorySegmentBulkVectorOps.DotProduct DOT_OPS =
        MemorySegmentBulkVectorOps.DOT_INSTANCE;

    MaxInnerProductSupplier(MemorySegment seg, Float16VectorValues values) {
      super(seg, values);
    }

    @Override
    public UpdateableRandomVectorScorer scorer() {
      return null;
    }

    @Override
    public MaxInnerProductSupplier copy() throws IOException {
      return new MaxInnerProductSupplier(seg, values);
    }
  }

}
