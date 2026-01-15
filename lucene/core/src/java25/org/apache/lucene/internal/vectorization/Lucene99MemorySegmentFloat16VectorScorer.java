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
import org.apache.lucene.util.hnsw.RandomVectorScorer;

abstract sealed class Lucene99MemorySegmentFloat16VectorScorer
    extends RandomVectorScorer.AbstractRandomVectorScorer {

  final Float16VectorValues values;
  final int vectorByteSize;
  final MemorySegment seg;
  final short[] query;

  /**
   * Return an optional whose value, if present, is the scorer. Otherwise, an empty optional is
   * returned.
   */
  public static Optional<Lucene99MemorySegmentFloat16VectorScorer> create(
      VectorSimilarityFunction type, IndexInput input, Float16VectorValues values, short[] query)
      throws IOException {
    input = FilterIndexInput.unwrapOnlyTest(input);
    MemorySegment seg;
    if (!(input instanceof MemorySegmentAccessInput msInput
        && (seg = msInput.segmentSliceOrNull(0L, msInput.length())) != null)) {
      return Optional.empty();
    }
    checkInvariants(values.size(), values.getVectorByteLength(), input);
    return switch (type) {
      case COSINE -> Optional.of(new CosineScorer(seg, values, query));
      case DOT_PRODUCT -> Optional.of(new DotProductScorer(seg, values, query));
      case EUCLIDEAN -> Optional.of(new EuclideanScorer(seg, values, query));
      case MAXIMUM_INNER_PRODUCT -> Optional.of(new MaxInnerProductScorer(seg, values, query));
    };
  }

  Lucene99MemorySegmentFloat16VectorScorer(
      MemorySegment seg, Float16VectorValues values, short[] query) {
    super(values);
    this.values = values;
    this.seg = seg;
    this.vectorByteSize = values.getVectorByteLength();
    this.query = query;
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


  static final class CosineScorer extends Lucene99MemorySegmentFloat16VectorScorer {

    static final MemorySegmentBulkVectorOps.Cosine COS_OPS =
        MemorySegmentBulkVectorOps.COS_INSTANCE;

    CosineScorer(MemorySegment seg, Float16VectorValues values, short[] query) {
      super(seg, values, query);
    }

    @Override
    public float score(int node) throws IOException {
      checkOrdinal(node);
      // just delegates to existing scorer that copies on-heap
      return VectorSimilarityFunction.COSINE.compare(query, values.vectorValue(node));
    }
  }

  static final class DotProductScorer extends Lucene99MemorySegmentFloat16VectorScorer {

    static final MemorySegmentBulkVectorOps.DotProduct DOT_OPS =
        MemorySegmentBulkVectorOps.DOT_INSTANCE;

    DotProductScorer(MemorySegment input, Float16VectorValues values, short[] query) {
      super(input, values, query);
    }

    @Override
    public float score(int node) throws IOException {
      checkOrdinal(node);
      // just delegates to existing scorer that copies on-heap
      return VectorSimilarityFunction.DOT_PRODUCT.compare(query, values.vectorValue(node));
    }

  }

  static final class EuclideanScorer extends Lucene99MemorySegmentFloat16VectorScorer {

    static final MemorySegmentBulkVectorOps.SqrDistance SQR_OPS =
        MemorySegmentBulkVectorOps.SQR_INSTANCE;

    EuclideanScorer(MemorySegment seg, Float16VectorValues values, short[] query) {
      super(seg, values, query);
    }

    @Override
    public float score(int node) throws IOException {
      checkOrdinal(node);
      // just delegates to existing scorer that copies on-heap
      return VectorSimilarityFunction.EUCLIDEAN.compare(query, values.vectorValue(node));
    }
  }

  static final class MaxInnerProductScorer extends Lucene99MemorySegmentFloat16VectorScorer {

    static final MemorySegmentBulkVectorOps.DotProduct DOT_OPS =
        MemorySegmentBulkVectorOps.DOT_INSTANCE;

    MaxInnerProductScorer(MemorySegment seg, Float16VectorValues values, short[] query) {
      super(seg, values, query);
    }

    @Override
    public float score(int node) throws IOException {
      checkOrdinal(node);
      // just delegates to existing scorer that copies on-heap
      return VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT.compare(
          query, values.vectorValue(node));
    }
  }
}
