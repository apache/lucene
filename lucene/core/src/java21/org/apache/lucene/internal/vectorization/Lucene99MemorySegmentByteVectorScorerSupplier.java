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

import static org.apache.lucene.index.VectorSimilarityFunction.COSINE;
import static org.apache.lucene.index.VectorSimilarityFunction.DOT_PRODUCT;
import static org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;
import static org.apache.lucene.index.VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Optional;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;

/** A score supplier of vectors whose element size is byte. */
public abstract class Lucene99MemorySegmentByteVectorScorerSupplier
    implements RandomVectorScorerSupplier {
  final int vectorByteSize;
  final int maxOrd;
  final MemorySegmentAccessInput input;
  final RandomAccessVectorValues values; // to support ordToDoc/getAcceptOrds
  byte[] scratch1, scratch2;

  /**
   * Return an optional whose value, if present, is the scorer supplier. Otherwise, an empty
   * optional is returned.
   */
  static Optional<RandomVectorScorerSupplier> create(
      VectorSimilarityFunction type, IndexInput input, RandomAccessVectorValues values) {
    input = FilterIndexInput.unwrapOnlyTest(input);
    if (!(input instanceof MemorySegmentAccessInput)) {
      return Optional.empty();
    }
    MemorySegmentAccessInput msInput = (MemorySegmentAccessInput) input;
    checkInvariants(values.size(), values.getVectorByteLength(), input);
    if (type == COSINE) {
      return Optional.of(new CosineSupplier(msInput, values));
    } else if (type == DOT_PRODUCT) {
      return Optional.of(new DotProductSupplier(msInput, values));
    } else if (type == EUCLIDEAN) {
      return Optional.of(new EuclideanSupplier(msInput, values));
    } else if (type == MAXIMUM_INNER_PRODUCT) {
      return Optional.of(new MaxInnerProductSupplier(msInput, values));
    } else {
      throw new IllegalArgumentException("unknown type: " + type);
    }
  }

  Lucene99MemorySegmentByteVectorScorerSupplier(
      MemorySegmentAccessInput input, RandomAccessVectorValues values) {
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

  final MemorySegment getFirstSegment(int ord) throws IOException {
    long byteOffset = (long) ord * vectorByteSize;
    MemorySegment seg = input.segmentSliceOrNull(byteOffset, vectorByteSize);
    if (seg == null) {
      if (scratch1 == null) {
        scratch1 = new byte[vectorByteSize];
      }
      input.readBytes(byteOffset, scratch1, 0, vectorByteSize);
      seg = MemorySegment.ofArray(scratch1);
    }
    return seg;
  }

  final MemorySegment getSecondSegment(int ord) throws IOException {
    long byteOffset = (long) ord * vectorByteSize;
    MemorySegment seg = input.segmentSliceOrNull(byteOffset, vectorByteSize);
    if (seg == null) {
      if (scratch2 == null) {
        scratch2 = new byte[vectorByteSize];
      }
      input.readBytes(byteOffset, scratch2, 0, vectorByteSize);
      seg = MemorySegment.ofArray(scratch2);
    }
    return seg;
  }

  static final class CosineSupplier extends Lucene99MemorySegmentByteVectorScorerSupplier {

    CosineSupplier(MemorySegmentAccessInput input, RandomAccessVectorValues values) {
      super(input, values);
    }

    @Override
    public RandomVectorScorer scorer(int ord) {
      checkOrdinal(ord);
      return new RandomVectorScorer.AbstractRandomVectorScorer(values) {
        @Override
        public float score(int node) throws IOException {
          checkOrdinal(node);
          float raw = PanamaVectorUtilSupport.cosine(getFirstSegment(ord), getSecondSegment(node));
          return (1 + raw) / 2;
        }
      };
    }

    @Override
    public CosineSupplier copy() throws IOException {
      return new CosineSupplier(input.clone(), values);
    }
  }

  static final class DotProductSupplier extends Lucene99MemorySegmentByteVectorScorerSupplier {

    DotProductSupplier(MemorySegmentAccessInput input, RandomAccessVectorValues values) {
      super(input, values);
    }

    @Override
    public RandomVectorScorer scorer(int ord) {
      checkOrdinal(ord);
      return new RandomVectorScorer.AbstractRandomVectorScorer(values) {
        @Override
        public float score(int node) throws IOException {
          checkOrdinal(node);
          // divide by 2 * 2^14 (maximum absolute value of product of 2 signed bytes) * len
          float raw =
              PanamaVectorUtilSupport.dotProduct(getFirstSegment(ord), getSecondSegment(node));
          return 0.5f + raw / (float) (values.dimension() * (1 << 15));
        }
      };
    }

    @Override
    public DotProductSupplier copy() throws IOException {
      return new DotProductSupplier(input.clone(), values);
    }
  }

  static final class EuclideanSupplier extends Lucene99MemorySegmentByteVectorScorerSupplier {

    EuclideanSupplier(MemorySegmentAccessInput input, RandomAccessVectorValues values) {
      super(input, values);
    }

    @Override
    public RandomVectorScorer scorer(int ord) {
      checkOrdinal(ord);
      return new RandomVectorScorer.AbstractRandomVectorScorer(values) {
        @Override
        public float score(int node) throws IOException {
          checkOrdinal(node);
          float raw =
              PanamaVectorUtilSupport.squareDistance(getFirstSegment(ord), getSecondSegment(node));
          return 1 / (1f + raw);
        }
      };
    }

    @Override
    public EuclideanSupplier copy() throws IOException {
      return new EuclideanSupplier(input.clone(), values);
    }
  }

  static final class MaxInnerProductSupplier extends Lucene99MemorySegmentByteVectorScorerSupplier {

    MaxInnerProductSupplier(MemorySegmentAccessInput input, RandomAccessVectorValues values) {
      super(input, values);
    }

    @Override
    public RandomVectorScorer scorer(int ord) {
      checkOrdinal(ord);
      return new RandomVectorScorer.AbstractRandomVectorScorer(values) {
        @Override
        public float score(int node) throws IOException {
          checkOrdinal(node);
          float raw =
              PanamaVectorUtilSupport.dotProduct(getFirstSegment(ord), getSecondSegment(node));
          if (raw < 0) {
            return 1 / (1 + -1 * raw);
          }
          return raw + 1;
        }
      };
    }

    @Override
    public MaxInnerProductSupplier copy() throws IOException {
      return new MaxInnerProductSupplier(input.clone(), values);
    }
  }
}
