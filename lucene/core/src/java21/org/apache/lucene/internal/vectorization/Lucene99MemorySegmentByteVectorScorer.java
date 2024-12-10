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
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Optional;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.hnsw.RandomVectorScorer;

abstract sealed class Lucene99MemorySegmentByteVectorScorer
    extends RandomVectorScorer.AbstractRandomVectorScorer {

  final int vectorByteSize;
  final MemorySegmentAccessInput input;
  final MemorySegment query;
  MemorySegment scratch;

  /**
   * Return an optional whose value, if present, is the scorer. Otherwise, an empty optional is
   * returned.
   */
  public static Optional<Lucene99MemorySegmentByteVectorScorer> create(
      VectorSimilarityFunction type, IndexInput input, KnnVectorValues values, byte[] queryVector) {
    assert values instanceof ByteVectorValues;
    input = FilterIndexInput.unwrapOnlyTest(input);
    if (!(input instanceof MemorySegmentAccessInput msInput)) {
      return Optional.empty();
    }
    checkInvariants(values.size(), values.getVectorByteLength(), input);
    return switch (type) {
      case COSINE -> Optional.of(new CosineScorer(msInput, values, queryVector));
      case DOT_PRODUCT -> Optional.of(new DotProductScorer(msInput, values, queryVector));
      case EUCLIDEAN -> Optional.of(new EuclideanScorer(msInput, values, queryVector));
      case MAXIMUM_INNER_PRODUCT ->
          Optional.of(new MaxInnerProductScorer(msInput, values, queryVector));
    };
  }

  Lucene99MemorySegmentByteVectorScorer(
      MemorySegmentAccessInput input, KnnVectorValues values, MemorySegment query) {
    super(values);
    this.input = input;
    this.vectorByteSize = values.getVectorByteLength();
    this.query = query;
  }

  /**
   * Retrieve vector for 'ord' in an off-Heap MemorySegment if requested. When offHeap is true, a
   * slice of the underlying off-heap MemorySegment with vector data is returned in the common case.
   *
   * <p>In the rare case that data for requested vector is split across multiple off-Heap
   * MemmorySegments, an off-heap scratch space is allocated, vector data is copied and returned to
   * the caller.
   *
   * <p>When off-heap is false, the vector data is copied to an on-heap byte[] which is wrapped in a
   * MemorySegment and returned to the caller.
   *
   * <p>An offHeap MemorySegment is needed for vector distance computations implemented in native
   * code invoked using Java FFM APIs. The native code can only work with off-Heap memory which is
   * not under the control of Java garbage-collector.
   *
   * @param ord
   * @param offHeap
   * @return
   * @throws IOException
   */
  final MemorySegment getSegment(int ord, boolean offHeap) throws IOException {
    checkOrdinal(ord);
    long byteOffset = (long) ord * vectorByteSize;
    MemorySegment seg = input.segmentSliceOrNull(byteOffset, vectorByteSize);
    if (seg == null) {
      if (scratch == null) {
        scratch =
            (offHeap)
                ? Arena.ofAuto().allocate(vectorByteSize, ValueLayout.JAVA_BYTE.byteAlignment())
                : MemorySegment.ofArray(new byte[vectorByteSize]);
      }
      input.readBytes(byteOffset, scratch, 0, vectorByteSize);
      seg = scratch;
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

  private static MemorySegment querySegment(byte[] query, boolean offHeap) {
    MemorySegment querySegment;
    if (offHeap) {
      querySegment = Arena.ofAuto().allocate(query.length, ValueLayout.JAVA_BYTE.byteAlignment());
      MemorySegment.copy(query, 0, querySegment, ValueLayout.JAVA_BYTE, 0, query.length);
    } else {
      querySegment = MemorySegment.ofArray(query);
    }
    return querySegment;
  }

  static final class CosineScorer extends Lucene99MemorySegmentByteVectorScorer {
    CosineScorer(MemorySegmentAccessInput input, KnnVectorValues values, byte[] query) {
      super(input, values, querySegment(query, false));
    }

    @Override
    public float score(int node) throws IOException {
      checkOrdinal(node);
      float raw = PanamaVectorUtilSupport.cosine(query, getSegment(node, false));
      return (1 + raw) / 2;
    }
  }

  static final class DotProductScorer extends Lucene99MemorySegmentByteVectorScorer {
    DotProductScorer(MemorySegmentAccessInput input, KnnVectorValues values, byte[] query) {
      super(input, values, querySegment(query, Constants.NATIVE_DOT_PRODUCT_ENABLED));
    }

    @Override
    public float score(int node) throws IOException {
      checkOrdinal(node);
      // divide by 2 * 2^14 (maximum absolute value of product of 2 signed bytes) * len
      float raw =
          PanamaVectorUtilSupport.dotProduct(
              query, getSegment(node, Constants.NATIVE_DOT_PRODUCT_ENABLED));
      return 0.5f + raw / (float) (query.byteSize() * (1 << 15));
    }
  }

  static final class EuclideanScorer extends Lucene99MemorySegmentByteVectorScorer {
    EuclideanScorer(MemorySegmentAccessInput input, KnnVectorValues values, byte[] query) {
      super(input, values, querySegment(query, false));
    }

    @Override
    public float score(int node) throws IOException {
      checkOrdinal(node);
      float raw = PanamaVectorUtilSupport.squareDistance(query, getSegment(node, false));
      return 1 / (1f + raw);
    }
  }

  static final class MaxInnerProductScorer extends Lucene99MemorySegmentByteVectorScorer {
    MaxInnerProductScorer(MemorySegmentAccessInput input, KnnVectorValues values, byte[] query) {
      super(input, values, querySegment(query, Constants.NATIVE_DOT_PRODUCT_ENABLED));
    }

    @Override
    public float score(int node) throws IOException {
      checkOrdinal(node);
      float raw =
          PanamaVectorUtilSupport.dotProduct(
              query, getSegment(node, Constants.NATIVE_DOT_PRODUCT_ENABLED));
      if (raw < 0) {
        return 1 / (1 + -1 * raw);
      }
      return raw + 1;
    }
  }
}
