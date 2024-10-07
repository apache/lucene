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
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;

/** A score supplier of vectors whose element size is byte. */
public abstract sealed class Lucene99MemorySegmentByteVectorScorerSupplier
    implements RandomVectorScorerSupplier {
  final int vectorByteSize;
  final int maxOrd;
  final MemorySegmentAccessInput input;
  final KnnVectorValues values; // to support ordToDoc/getAcceptOrds

  /**
   * Return an optional whose value, if present, is the scorer supplier. Otherwise, an empty
   * optional is returned.
   */
  static Optional<RandomVectorScorerSupplier> create(
      VectorSimilarityFunction type, IndexInput input, KnnVectorValues values) {
    assert values instanceof ByteVectorValues;
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

  Lucene99MemorySegmentByteVectorScorerSupplier(
      MemorySegmentAccessInput input, KnnVectorValues values) {
    this.input = input;
    this.values = values;
    this.vectorByteSize = values.getVectorByteLength();
    this.maxOrd = values.size();
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

  final MemorySegment getSegment(MemorySegmentAccessInput input, byte[] scratch, int ord)
      throws IOException {
    long byteOffset = (long) ord * vectorByteSize;
    MemorySegment seg = input.segmentSliceOrNull(byteOffset, vectorByteSize);
    if (seg == null) {
      input.readBytes(byteOffset, scratch, 0, vectorByteSize);
      seg = MemorySegment.ofArray(scratch);
    }
    return seg;
  }

  static final class CosineSupplier extends Lucene99MemorySegmentByteVectorScorerSupplier {

    CosineSupplier(MemorySegmentAccessInput input, KnnVectorValues values) {
      super(input, values);
    }

    @Override
    public RandomVectorScorer scorer(int ord) {
      checkOrdinal(ord);
      MemorySegmentAccessInput slice = input.clone();
      byte[] scratch1 = new byte[vectorByteSize];
      byte[] scratch2 = new byte[vectorByteSize];
      return new RandomVectorScorer.AbstractRandomVectorScorer(values) {
        @Override
        public float score(int node) throws IOException {
          checkOrdinal(node);
          float raw =
              PanamaVectorUtilSupport.cosine(
                  getSegment(slice, scratch1, ord), getSegment(slice, scratch2, node));
          return (1 + raw) / 2;
        }
      };
    }
  }

  static final class DotProductSupplier extends Lucene99MemorySegmentByteVectorScorerSupplier {

    DotProductSupplier(MemorySegmentAccessInput input, KnnVectorValues values) {
      super(input, values);
    }

    @Override
    public RandomVectorScorer scorer(int ord) {
      checkOrdinal(ord);
      MemorySegmentAccessInput slice = input.clone();
      byte[] scratch1 = new byte[vectorByteSize];
      byte[] scratch2 = new byte[vectorByteSize];
      return new RandomVectorScorer.AbstractRandomVectorScorer(values) {
        @Override
        public float score(int node) throws IOException {
          checkOrdinal(node);
          // divide by 2 * 2^14 (maximum absolute value of product of 2 signed bytes) * len
          float raw =
              PanamaVectorUtilSupport.dotProduct(
                  getSegment(slice, scratch1, ord), getSegment(slice, scratch2, node));
          return 0.5f + raw / (float) (values.dimension() * (1 << 15));
        }
      };
    }
  }

  static final class EuclideanSupplier extends Lucene99MemorySegmentByteVectorScorerSupplier {

    EuclideanSupplier(MemorySegmentAccessInput input, KnnVectorValues values) {
      super(input, values);
    }

    @Override
    public RandomVectorScorer scorer(int ord) {
      checkOrdinal(ord);
      MemorySegmentAccessInput slice = input.clone();
      byte[] scratch1 = new byte[vectorByteSize];
      byte[] scratch2 = new byte[vectorByteSize];

      return new RandomVectorScorer.AbstractRandomVectorScorer(values) {
        @Override
        public float score(int node) throws IOException {
          checkOrdinal(node);
          float raw =
              PanamaVectorUtilSupport.squareDistance(
                  getSegment(slice, scratch1, ord), getSegment(slice, scratch2, node));
          return 1 / (1f + raw);
        }
      };
    }
  }

  static final class MaxInnerProductSupplier extends Lucene99MemorySegmentByteVectorScorerSupplier {

    MaxInnerProductSupplier(MemorySegmentAccessInput input, KnnVectorValues values) {
      super(input, values);
    }

    @Override
    public RandomVectorScorer scorer(int ord) {
      checkOrdinal(ord);
      MemorySegmentAccessInput slice = input.clone();
      byte[] scratch1 = new byte[vectorByteSize];
      byte[] scratch2 = new byte[vectorByteSize];

      return new RandomVectorScorer.AbstractRandomVectorScorer(values) {
        @Override
        public float score(int node) throws IOException {
          checkOrdinal(node);
          float raw =
              PanamaVectorUtilSupport.dotProduct(
                  getSegment(slice, scratch1, ord), getSegment(slice, scratch2, node));
          if (raw < 0) {
            return 1 / (1 + -1 * raw);
          }
          return raw + 1;
        }
      };
    }
  }
}
