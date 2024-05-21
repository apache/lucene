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

abstract class Lucene99MemorySegmentByteVectorScorer
    extends RandomVectorScorer.AbstractRandomVectorScorer {

  final int vectorByteSize;
  final MemorySegmentAccessInput input;
  final MemorySegment query;
  byte[] scratch;

  /**
   * Return an optional whose value, if present, is the scorer. Otherwise, an empty optional is
   * returned.
   */
  public static Optional<Lucene99MemorySegmentByteVectorScorer> create(
      VectorSimilarityFunction type,
      IndexInput input,
      RandomAccessVectorValues values,
      byte[] queryVector) {
    input = FilterIndexInput.unwrapOnlyTest(input);
    if (!(input instanceof MemorySegmentAccessInput)) {
      return Optional.empty();
    }
    MemorySegmentAccessInput msInput = (MemorySegmentAccessInput) input;
    checkInvariants(values.size(), values.getVectorByteLength(), input);
    if (type == COSINE) {
      return Optional.of(new CosineScorer(msInput, values, queryVector));
    } else if (type == DOT_PRODUCT) {
      return Optional.of(new DotProductScorer(msInput, values, queryVector));
    } else if (type == EUCLIDEAN) {
      return Optional.of(new EuclideanScorer(msInput, values, queryVector));
    } else if (type == MAXIMUM_INNER_PRODUCT) {
      return Optional.of(new MaxInnerProductScorer(msInput, values, queryVector));
    } else {
      throw new IllegalArgumentException("unknown type: " + type);
    }
  }

  Lucene99MemorySegmentByteVectorScorer(
      MemorySegmentAccessInput input, RandomAccessVectorValues values, byte[] queryVector) {
    super(values);
    this.input = input;
    this.vectorByteSize = values.getVectorByteLength();
    this.query = MemorySegment.ofArray(queryVector);
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

  static final class CosineScorer extends Lucene99MemorySegmentByteVectorScorer {
    CosineScorer(MemorySegmentAccessInput input, RandomAccessVectorValues values, byte[] query) {
      super(input, values, query);
    }

    @Override
    public float score(int node) throws IOException {
      checkOrdinal(node);
      float raw = PanamaVectorUtilSupport.cosine(query, getSegment(node));
      return (1 + raw) / 2;
    }
  }

  static final class DotProductScorer extends Lucene99MemorySegmentByteVectorScorer {
    DotProductScorer(
        MemorySegmentAccessInput input, RandomAccessVectorValues values, byte[] query) {
      super(input, values, query);
    }

    @Override
    public float score(int node) throws IOException {
      checkOrdinal(node);
      // divide by 2 * 2^14 (maximum absolute value of product of 2 signed bytes) * len
      float raw = PanamaVectorUtilSupport.dotProduct(query, getSegment(node));
      return 0.5f + raw / (float) (query.byteSize() * (1 << 15));
    }
  }

  static final class EuclideanScorer extends Lucene99MemorySegmentByteVectorScorer {
    EuclideanScorer(MemorySegmentAccessInput input, RandomAccessVectorValues values, byte[] query) {
      super(input, values, query);
    }

    @Override
    public float score(int node) throws IOException {
      checkOrdinal(node);
      float raw = PanamaVectorUtilSupport.squareDistance(query, getSegment(node));
      return 1 / (1f + raw);
    }
  }

  static final class MaxInnerProductScorer extends Lucene99MemorySegmentByteVectorScorer {
    MaxInnerProductScorer(
        MemorySegmentAccessInput input, RandomAccessVectorValues values, byte[] query) {
      super(input, values, query);
    }

    @Override
    public float score(int node) throws IOException {
      checkOrdinal(node);
      float raw = PanamaVectorUtilSupport.dotProduct(query, getSegment(node));
      if (raw < 0) {
        return 1 / (1 + -1 * raw);
      }
      return raw + 1;
    }
  }
}
