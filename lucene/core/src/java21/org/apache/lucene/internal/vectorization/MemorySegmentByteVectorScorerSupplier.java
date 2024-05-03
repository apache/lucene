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
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MemorySegmentAccess;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;

/** A scorer of vectors whose element size is byte. */
public abstract sealed class MemorySegmentByteVectorScorerSupplier
    implements RandomVectorScorerSupplier, RandomVectorScorer
    permits DotProductByteVectorScorerSupplier, EuclideanByteVectorScorerSupplier {
  final int vectorByteSize;
  final int dims;
  final int maxOrd;
  final IndexInput input;
  final MemorySegmentAccess memorySegmentAccess;

  final RandomAccessVectorValues values; // to support ordToDoc/getAcceptOrds
  final byte[] scratch1, scratch2;

  MemorySegment first;

  /**
   * Return an optional whose value, if present, is the scorer. Otherwise, an empty optional is
   * returned.
   */
  public static Optional<MemorySegmentByteVectorScorerSupplier> create(
      int dims,
      int maxOrd,
      int vectorByteSize,
      VectorSimilarityFunction type,
      IndexInput input,
      RandomAccessVectorValues values) {
    input = FilterIndexInput.unwrap(input);
    if (!(input instanceof MemorySegmentAccess)) {
      return Optional.empty();
    }
    checkInvariants(maxOrd, vectorByteSize, input);
    return switch (type) {
      case DOT_PRODUCT -> Optional.of(
          new DotProductByteVectorScorerSupplier(dims, maxOrd, vectorByteSize, input, values));
      case EUCLIDEAN -> Optional.of(
          new EuclideanByteVectorScorerSupplier(dims, maxOrd, vectorByteSize, input, values));
      case MAXIMUM_INNER_PRODUCT -> Optional.empty(); // TODO: implement MAXIMUM_INNER_PRODUCT
      case COSINE -> Optional.empty(); // TODO: implement Cosine
    };
  }

  MemorySegmentByteVectorScorerSupplier(
      int dims, int maxOrd, int vectorByteSize, IndexInput input, RandomAccessVectorValues values) {
    this.vectorByteSize = vectorByteSize;
    this.dims = dims;
    this.maxOrd = maxOrd;
    this.input = input;
    this.memorySegmentAccess = (MemorySegmentAccess) input;
    this.values = values;
    scratch1 = new byte[vectorByteSize];
    scratch2 = new byte[vectorByteSize];
  }

  static void checkInvariants(int maxOrd, int vectorByteLength, IndexInput input) {
    if (input.length() < (long) vectorByteLength * maxOrd) {
      throw new IllegalArgumentException("input length not equal to expected vector data");
    }
  }

  final void checkOrdinal(int ord, int maxOrd) {
    if (ord < 0 || ord >= maxOrd) {
      throw new IllegalArgumentException("illegal ordinal: " + ord);
    }
  }

  protected final MemorySegment getSegment(int ord, byte[] scratch) throws IOException {
    checkOrdinal(ord, maxOrd);
    int byteOffset = ord * vectorByteSize; // TODO: random + meta size
    MemorySegment seg = memorySegmentAccess.segmentSliceOrNull(byteOffset, vectorByteSize);
    if (seg == null) {
      input.seek(byteOffset);
      input.readBytes(scratch, 0, vectorByteSize);
      seg = MemorySegment.ofArray(scratch);
    }
    return seg;
  }

  public final RandomVectorScorer scorer(byte[] target) {
    first = MemorySegment.ofArray(target);
    return this;
  }

  @Override
  public final RandomVectorScorer scorer(int ord) throws IOException {
    first = getSegment(ord, scratch1);
    return this;
  }

  @Override
  public final int maxOrd() {
    return maxOrd;
  }

  @Override
  public final int ordToDoc(int ord) {
    return values.ordToDoc(ord);
  }

  @Override
  public final Bits getAcceptOrds(Bits acceptDocs) {
    return values.getAcceptOrds(acceptDocs);
  }
}
