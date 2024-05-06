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
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;

/**
 * A scorer of vectors whose element size is byte.
 *
 * <p>This class is both a scorer supplier and a scorer. Since score suppliers and their scorers are
 * not thread-safe, this allows to share per-thread state and temporary scratch buffers.
 */
public abstract sealed class MemorySegmentByteVectorScorerSupplier
    implements RandomVectorScorerSupplier, RandomVectorScorer {
  final int vectorByteSize;
  final int dims;
  final int maxOrd;
  final MemorySegmentAccessInput input;

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
    input = FilterIndexInput.unwrapOnlyTest(input);
    if (!(input instanceof MemorySegmentAccessInput msInput)) {
      return Optional.empty();
    }
    checkInvariants(maxOrd, vectorByteSize, input);
    return switch (type) {
      case COSINE -> Optional.of(new Cosine(dims, maxOrd, vectorByteSize, msInput, values));
      case DOT_PRODUCT -> Optional.of(
          new DotProduct(dims, maxOrd, vectorByteSize, msInput, values));
      case EUCLIDEAN -> Optional.of(new Euclidean(dims, maxOrd, vectorByteSize, msInput, values));
      case MAXIMUM_INNER_PRODUCT -> Optional.of(
          new MaxInnerProduct(dims, maxOrd, vectorByteSize, msInput, values));
    };
  }

  MemorySegmentByteVectorScorerSupplier(
      int dims,
      int maxOrd,
      int vectorByteSize,
      MemorySegmentAccessInput input,
      RandomAccessVectorValues values) {
    this.vectorByteSize = vectorByteSize;
    this.dims = dims;
    this.maxOrd = maxOrd;
    this.input = input;
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
    MemorySegment seg = input.segmentSliceOrNull(byteOffset, vectorByteSize);
    if (seg == null) {
      input.readBytes(byteOffset, scratch, 0, vectorByteSize);
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

  static final class Cosine extends MemorySegmentByteVectorScorerSupplier {

    Cosine(
        int dims,
        int maxOrd,
        int vectorByteSize,
        MemorySegmentAccessInput input,
        RandomAccessVectorValues values) {
      super(dims, maxOrd, vectorByteSize, input, values);
    }

    @Override
    public float score(int node) throws IOException {
      float raw = PanamaVectorUtilSupport.cosine(first, getSegment(node, scratch2));
      return (1 + raw) / 2;
    }

    @Override
    public Cosine copy() throws IOException {
      return new Cosine(dims, maxOrd, vectorByteSize, input.clone(), values);
    }
  }

  static final class DotProduct extends MemorySegmentByteVectorScorerSupplier {

    DotProduct(
        int dims,
        int maxOrd,
        int vectorByteSize,
        MemorySegmentAccessInput input,
        RandomAccessVectorValues values) {
      super(dims, maxOrd, vectorByteSize, input, values);
    }

    @Override
    public float score(int node) throws IOException {
      // divide by 2 * 2^14 (maximum absolute value of product of 2 signed bytes) * len
      float raw = PanamaVectorUtilSupport.dotProduct(first, getSegment(node, scratch2));
      return 0.5f + raw / (float) (dims * (1 << 15));
    }

    @Override
    public DotProduct copy() throws IOException {
      return new DotProduct(dims, maxOrd, vectorByteSize, input.clone(), values);
    }
  }

  static final class Euclidean extends MemorySegmentByteVectorScorerSupplier {

    Euclidean(
        int dims,
        int maxOrd,
        int vectorByteSize,
        MemorySegmentAccessInput input,
        RandomAccessVectorValues values) {
      super(dims, maxOrd, vectorByteSize, input, values);
    }

    @Override
    public float score(int node) throws IOException {
      float raw = PanamaVectorUtilSupport.squareDistance(first, getSegment(node, scratch2));
      return 1 / (1f + raw);
    }

    @Override
    public Euclidean copy() throws IOException {
      return new Euclidean(dims, maxOrd, vectorByteSize, input.clone(), values);
    }
  }

  static final class MaxInnerProduct extends MemorySegmentByteVectorScorerSupplier {

    MaxInnerProduct(
        int dims,
        int maxOrd,
        int vectorByteSize,
        MemorySegmentAccessInput input,
        RandomAccessVectorValues values) {
      super(dims, maxOrd, vectorByteSize, input, values);
    }

    @Override
    public float score(int node) throws IOException {
      float raw = PanamaVectorUtilSupport.dotProduct(first, getSegment(node, scratch2));
      if (raw < 0) {
        return 1 / (1 + -1 * raw);
      }
      return raw + 1;
    }

    @Override
    public MaxInnerProduct copy() throws IOException {
      return new MaxInnerProduct(dims, maxOrd, vectorByteSize, input.clone(), values);
    }
  }
}
