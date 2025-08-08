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
      case COSINE -> Optional.empty(); // of(new CosineSupplier(msInput, values));
      case DOT_PRODUCT -> Optional.of(new DotProductSupplier(msInput, values));
      case EUCLIDEAN -> Optional.empty(); // of(new EuclideanSupplier(msInput, values));
      case MAXIMUM_INNER_PRODUCT ->
          Optional.empty(); // of(new MaxInnerProductSupplier(msInput, values));
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

  final void checkOrdinal(int ord) {
    if (ord < 0 || ord >= maxOrd) {
      throw new IllegalArgumentException("illegal ordinal: " + ord);
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

  static final class DotProductSupplier extends Lucene99MemorySegmentFloatVectorScorerSupplier {

    static final MemorySegmentBulkVectorOps.DotFromQuerySegment DOT_OPS =
        new MemorySegmentBulkVectorOps.DotFromQuerySegment();

    DotProductSupplier(MemorySegmentAccessInput input, FloatVectorValues values) {
      super(input, values);
    }

    @Override
    public UpdateableRandomVectorScorer scorer() {
      return new UpdateableRandomVectorScorer.AbstractUpdateableRandomVectorScorer(values) {
        private int queryOrd;

        @Override
        public float score(int node) throws IOException {
          checkOrdinal(node);
          MemorySegment query = getSegment(queryOrd, queryScratch);
          MemorySegment ms = getSegment(node, scratch1);
          var raw = DOT_OPS.dotProduct(query, ms, dims);
          return normalizeToUnitInterval(raw);
        }

        @Override
        public void bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
          // TODO checkOrdinal(node1 ....);
          float[] scratchScores = new float[4];
          int i = 0;
          MemorySegment query = getSegment(queryOrd, queryScratch);
          final int limit = numNodes & ~3;
          for (; i < limit; i += 4) {
            MemorySegment ms1 = getSegment(nodes[i], scratch1);
            MemorySegment ms2 = getSegment(nodes[i + 1], scratch2);
            MemorySegment ms3 = getSegment(nodes[i + 2], scratch3);
            MemorySegment ms4 = getSegment(nodes[i + 3], scratch4);
            DOT_OPS.dotProductBulk(scratchScores, query, ms1, ms2, ms3, ms4, dims);
            scores[i + 0] = normalizeToUnitInterval(scratchScores[0]);
            scores[i + 1] = normalizeToUnitInterval(scratchScores[1]);
            scores[i + 2] = normalizeToUnitInterval(scratchScores[2]);
            scores[i + 3] = normalizeToUnitInterval(scratchScores[3]);
          }
          // Handle remaining 1–3 nodes in bulk (if any)
          int remaining = numNodes - i;
          if (remaining > 0) {
            MemorySegment ms1 = getSegment(nodes[i], scratch1);
            MemorySegment ms2 = (remaining > 1) ? getSegment(nodes[i + 1], scratch2) : ms1;
            MemorySegment ms3 = (remaining > 2) ? getSegment(nodes[i + 2], scratch3) : ms1;
            DOT_OPS.dotProductBulk(scratchScores, query, ms1, ms2, ms3, ms1, dims);
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
      return new DotProductSupplier(input.clone(), values);
    }
  }
}
