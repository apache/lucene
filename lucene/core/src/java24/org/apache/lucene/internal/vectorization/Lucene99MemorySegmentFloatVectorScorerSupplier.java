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
      case COSINE -> Optional.empty(); // of(new CosineSupplier(msInput, values));
      case DOT_PRODUCT -> Optional.of(new DotProductSupplier(seg, values));
      case EUCLIDEAN -> Optional.empty(); // of(new EuclideanSupplier(msInput, values));
      case MAXIMUM_INNER_PRODUCT ->
          Optional.empty(); // of(new MaxInnerProductSupplier(msInput, values));
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

  final void checkOrdinal(int ord) {
    if (ord < 0 || ord >= maxOrd) {
      throw new IllegalArgumentException("illegal ordinal: " + ord);
    }
  }

  static final class DotProductSupplier extends Lucene99MemorySegmentFloatVectorScorerSupplier {

    static final MemorySegmentBulkVectorOps.DotProduct DOT_OPS =
        MemorySegmentBulkVectorOps.DOT_INSTANCE;

    final float[] scratchScores = new float[4];

    DotProductSupplier(MemorySegment seg, FloatVectorValues values) {
      super(seg, values);
    }

    @Override
    public UpdateableRandomVectorScorer scorer() {
      return new UpdateableRandomVectorScorer.AbstractUpdateableRandomVectorScorer(values) {
        private int queryOrd;

        @Override
        public float score(int node) throws IOException {
          checkOrdinal(node);
          long queryAddr = (long) queryOrd * vectorByteSize;
          long addr = (long) node * vectorByteSize;
          var raw = DOT_OPS.dotProduct(seg, queryAddr, addr, dims);
          return normalizeToUnitInterval(raw);
        }

        @Override
        public void bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
          // TODO checkOrdinal(node1 ....);
          int i = 0;
          long queryAddr = (long) queryOrd * vectorByteSize;
          final int limit = numNodes & ~3;
          for (; i < limit; i += 4) {
            long addr1 = (long) nodes[i + 0] * vectorByteSize;
            long addr2 = (long) nodes[i + 1] * vectorByteSize;
            long addr3 = (long) nodes[i + 2] * vectorByteSize;
            long addr4 = (long) nodes[i + 3] * vectorByteSize;
            DOT_OPS.dotProductBulk(seg, scratchScores, queryAddr, addr1, addr2, addr3, addr4, dims);
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
            DOT_OPS.dotProductBulk(seg, scratchScores, queryAddr, addr1, addr2, addr3, addr3, dims);
            scores[i] = normalizeToUnitInterval(scratchScores[0]);
            if (remaining > 1) scores[i + 1] = normalizeToUnitInterval(scratchScores[1]);
            if (remaining > 2) scores[i + 2] = normalizeToUnitInterval(scratchScores[2]);
          }
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
}
