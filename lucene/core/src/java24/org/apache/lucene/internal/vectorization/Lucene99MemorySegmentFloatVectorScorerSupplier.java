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

  // debugging
  //  final float[] getFloatValue(int ord) throws IOException {
  //    long byteOffset = (long) ord * vectorByteSize;
  //    float[] fa = new float[dims];
  //    for (int i = 0; i < fa.length; i++) {
  //      fa[i] = Float.intBitsToFloat(input.readInt(byteOffset + i));
  //    }
  //    return fa;
  //  }

  //  static final class CosineSupplier extends Lucene99MemorySegmentByteVectorScorerSupplier {
  //
  //    CosineSupplier(MemorySegmentAccessInput input, KnnVectorValues values) {
  //      super(input, values);
  //    }
  //
  //    @Override
  //    public UpdateableRandomVectorScorer scorer() {
  //      return new UpdateableRandomVectorScorer.AbstractUpdateableRandomVectorScorer(values) {
  //        private int queryOrd = 0;
  //
  //        @Override
  //        public float score(int node) throws IOException {
  //          checkOrdinal(node);
  //          float raw =
  //              PanamaVectorUtilSupport.cosine(getFirstSegment(queryOrd), getSecondSegment(node));
  //          return (1 + raw) / 2;
  //        }
  //
  //        @Override
  //        public void setScoringOrdinal(int node) {
  //          checkOrdinal(node);
  //          queryOrd = node;
  //        }
  //      };
  //    }
  //
  //    @Override
  //    public CosineSupplier copy() throws IOException {
  //      return new CosineSupplier(input.clone(), values);
  //    }
  //  }

  // TODO revert back to KnnVectorValues - since it does not have a getValue, so no danger of copy
  static final class DotProductSupplier extends Lucene99MemorySegmentFloatVectorScorerSupplier {

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
          //          float[] qv = getFloatValue(queryOrd);
          //          float[] qv = getFloatValue(node);

          // TODO: wasteful?  TODO: add a single overload for this one? Hmm.... yet another one! :-(
          // PanamaVectorUtilSupport.dotProductBulkFromSegments(scratchScores, query, ms1, ms1, ms1,
          // ms1, dims);
          float dot = PanamaVectorUtilSupport.dotProductBulkFromTwoSegments(query, ms, dims);
          return Math.max((1 + dot) / 2, 0);
        }

        @Override
        public boolean supportsBulk() {
          return true;
        }

        @Override
        public void scoreBulk(float[] scores, int node1, int node2, int node3, int node4)
            throws IOException {
          // TODO checkOrdinal(node1 ....);
          MemorySegment query = getSegment(queryOrd, queryScratch);
          MemorySegment ms1 = getSegment(node1, scratch1);
          MemorySegment ms2 = getSegment(node2, scratch2);
          MemorySegment ms3 = getSegment(node3, scratch3);
          MemorySegment ms4 = getSegment(node4, scratch4);
          PanamaVectorUtilSupport.dotProductBulkFromSegments(
              scores, query, ms1, ms2, ms3, ms4, dims);
          scores[0] = Math.max((1 + scores[0]) / 2, 0);
          scores[1] = Math.max((1 + scores[1]) / 2, 0);
          scores[2] = Math.max((1 + scores[2]) / 2, 0);
          scores[3] = Math.max((1 + scores[3]) / 2, 0);
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

  //  static final class EuclideanSupplier extends Lucene99MemorySegmentByteVectorScorerSupplier {
  //
  //    EuclideanSupplier(MemorySegmentAccessInput input, KnnVectorValues values) {
  //      super(input, values);
  //    }
  //
  //    @Override
  //    public UpdateableRandomVectorScorer scorer() {
  //      return new UpdateableRandomVectorScorer.AbstractUpdateableRandomVectorScorer(values) {
  //        private int queryOrd = 0;
  //
  //        @Override
  //        public float score(int node) throws IOException {
  //          checkOrdinal(node);
  //          float raw =
  //              PanamaVectorUtilSupport.squareDistance(
  //                  getFirstSegment(queryOrd), getSecondSegment(node));
  //          return 1 / (1f + raw);
  //        }
  //
  //        @Override
  //        public void setScoringOrdinal(int node) {
  //          checkOrdinal(node);
  //          queryOrd = node;
  //        }
  //      };
  //    }
  //
  //    @Override
  //    public EuclideanSupplier copy() throws IOException {
  //      return new EuclideanSupplier(input.clone(), values);
  //    }
  //  }

  //  static final class MaxInnerProductSupplier extends
  // Lucene99MemorySegmentByteVectorScorerSupplier {
  //
  //    MaxInnerProductSupplier(MemorySegmentAccessInput input, KnnVectorValues values) {
  //      super(input, values);
  //    }
  //
  //    @Override
  //    public UpdateableRandomVectorScorer scorer() {
  //      return new UpdateableRandomVectorScorer.AbstractUpdateableRandomVectorScorer(values) {
  //        private int queryOrd = 0;
  //
  //        @Override
  //        public float score(int node) throws IOException {
  //          checkOrdinal(node);
  //          float raw =
  //              PanamaVectorUtilSupport.dotProduct(getFirstSegment(queryOrd),
  // getSecondSegment(node));
  //          if (raw < 0) {
  //            return 1 / (1 + -1 * raw);
  //          }
  //          return raw + 1;
  //        }
  //
  //        @Override
  //        public void setScoringOrdinal(int node) {
  //          checkOrdinal(node);
  //          queryOrd = node;
  //        }
  //      };
  //    }
  //
  //    @Override
  //    public MaxInnerProductSupplier copy() throws IOException {
  //      return new MaxInnerProductSupplier(input.clone(), values);
  //    }
  //  }
}
