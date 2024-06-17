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
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.quantization.RandomAccessQuantizedByteVectorValues;

/** A score supplier of vectors whose element size is byte. */
public abstract sealed class Lucene99MemorySegmentScalarQuantizedVectorScorerSupplier
    implements RandomVectorScorerSupplier {

  final int vectorByteSize, trueVectorByteSize;
  final int maxOrd;
  final MemorySegmentAccessInput input;
  final RandomAccessQuantizedByteVectorValues values; // to support ordToDoc/getAcceptOrds
  byte[] scratch1, scratch2;
  final int scratch1Size;
  final float constMultiplier;

  /**
   * Return an optional whose value, if present, is the scorer supplier. Otherwise, an empty
   * optional is returned.
   */
  static Optional<RandomVectorScorerSupplier> create(
      VectorSimilarityFunction type,
      byte bits,
      float constMultiplier,
      RandomAccessQuantizedByteVectorValues values) {
    IndexInput input = values.getSlice();
    if (input == null) {
      return Optional.empty();
    }
    input = FilterIndexInput.unwrapOnlyTest(input);
    if (!(input instanceof MemorySegmentAccessInput msInput)) {
      return Optional.empty();
    }
    final boolean compressed = (values.getVectorByteLength() - Float.BYTES) != values.dimension();
    if (compressed) {
      assert bits == 4;
      assert (values.getVectorByteLength() - Float.BYTES) == values.dimension() / 2;
    }
    checkInvariants(values.size(), values.getVectorByteLength(), input);
    return switch (type) {
      case COSINE, DOT_PRODUCT -> {
        if (bits == 4) {
          yield Optional.of(
              new Int4DotProductSupplier(msInput, values, constMultiplier, compressed));
        }
        yield Optional.of(new DotProductSupplier(msInput, values, constMultiplier));
      }
      case EUCLIDEAN -> Optional.of(new EuclideanSupplier(msInput, values, constMultiplier));
      case MAXIMUM_INNER_PRODUCT -> {
        if (bits == 4) {
          yield Optional.of(
              new Int4MaxInnerProductSupplier(msInput, values, constMultiplier, compressed));
        }
        yield Optional.of(new MaxInnerProductSupplier(msInput, values, constMultiplier));
      }
    };
  }

  Lucene99MemorySegmentScalarQuantizedVectorScorerSupplier(
      MemorySegmentAccessInput input,
      RandomAccessQuantizedByteVectorValues values,
      float constMultiplier) {
    this.input = input;
    this.values = values;
    this.vectorByteSize = values.getVectorByteLength();
    this.trueVectorByteSize = (values.getVectorByteLength() - Float.BYTES);
    if (values.dimension() != trueVectorByteSize) {
      assert values.dimension() == trueVectorByteSize / 2;
      this.scratch1Size = trueVectorByteSize;
    } else {
      this.scratch1Size = trueVectorByteSize / 2;
    }
    this.maxOrd = values.size();
    this.constMultiplier = constMultiplier;
  }

  static void checkInvariants(int maxOrd, int vectorByteLength, IndexInput input) {
    if (input.length() < (long) vectorByteLength * maxOrd) {
      throw new IllegalArgumentException("input length is less than expected vector data");
    }
  }

  static void decompressBytes(byte[] compressed, int numBytes) {
    if (numBytes == compressed.length) {
      return;
    }
    if (numBytes << 1 != compressed.length) {
      throw new IllegalArgumentException(
          "numBytes: " + numBytes + " does not match compressed length: " + compressed.length);
    }
    for (int i = 0; i < numBytes; ++i) {
      compressed[numBytes + i] = (byte) (compressed[i] & 0x0F);
      compressed[i] = (byte) ((compressed[i] & 0xFF) >> 4);
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
    // we always read and decompress the full vector if the value is compressed
    // Generally, this is OK, as the scorer is used many times after the initial decompression
    if (seg == null || values.dimension() != trueVectorByteSize) {
      if (scratch1 == null) {
        scratch1 = new byte[this.scratch1Size];
      }
      input.readBytes(byteOffset, scratch1, 0, trueVectorByteSize);
      if (values.dimension() != trueVectorByteSize) {
        decompressBytes(scratch1, scratch1.length);
      }
      seg = MemorySegment.ofArray(scratch1);
    }
    return seg;
  }

  final MemorySegment getSecondSegment(int ord) throws IOException {
    long byteOffset = (long) ord * vectorByteSize;
    MemorySegment seg = input.segmentSliceOrNull(byteOffset, vectorByteSize);
    if (seg == null) {
      if (scratch2 == null) {
        scratch2 = new byte[values.dimension()];
      }
      input.readBytes(byteOffset, scratch2, 0, trueVectorByteSize);
      seg = MemorySegment.ofArray(scratch2);
    }
    return seg;
  }

  final float getOffsetCorrection(int ord) throws IOException {
    checkOrdinal(ord);
    long byteOffset = ((long) ord * vectorByteSize) + trueVectorByteSize;
    int floatInts = input.readInt(byteOffset);
    return Float.intBitsToFloat(floatInts);
  }

  static final class DotProductSupplier
      extends Lucene99MemorySegmentScalarQuantizedVectorScorerSupplier {

    DotProductSupplier(
        MemorySegmentAccessInput input,
        RandomAccessQuantizedByteVectorValues values,
        float constMultiplier) {
      super(input, values, constMultiplier);
    }

    @Override
    public RandomVectorScorer scorer(int ord) throws IOException {
      checkOrdinal(ord);
      MemorySegment querySegment = getFirstSegment(ord);
      float offsetCorrection = getOffsetCorrection(ord);
      return new RandomVectorScorer.AbstractRandomVectorScorer(values) {
        @Override
        public float score(int node) throws IOException {
          checkOrdinal(node);
          MemorySegment nodeSegment = getSecondSegment(node);
          float dotProduct = PanamaVectorUtilSupport.dotProduct(querySegment, nodeSegment);
          float nodeOffsetCorrection = getOffsetCorrection(node);
          assert dotProduct >= 0;
          float adjustedDistance =
              dotProduct * constMultiplier + offsetCorrection + nodeOffsetCorrection;
          return Math.max((1 + adjustedDistance) / 2, 0);
        }
      };
    }

    @Override
    public DotProductSupplier copy() throws IOException {
      return new DotProductSupplier(input.clone(), values, constMultiplier);
    }
  }

  static final class Int4DotProductSupplier
      extends Lucene99MemorySegmentScalarQuantizedVectorScorerSupplier {

    private final boolean compressed;

    Int4DotProductSupplier(
        MemorySegmentAccessInput input,
        RandomAccessQuantizedByteVectorValues values,
        float constMultiplier,
        boolean compressed) {
      super(input, values, constMultiplier);
      this.compressed = compressed;
    }

    @Override
    public RandomVectorScorer scorer(int ord) throws IOException {
      checkOrdinal(ord);
      MemorySegment querySegment = getFirstSegment(ord);
      float offsetCorrection = getOffsetCorrection(ord);
      return new RandomVectorScorer.AbstractRandomVectorScorer(values) {
        @Override
        public float score(int node) throws IOException {
          checkOrdinal(node);
          MemorySegment nodeSegment = getSecondSegment(node);
          float dotProduct =
              PanamaVectorUtilSupport.int4DotProduct(querySegment, false, nodeSegment, compressed);
          float nodeOffsetCorrection = getOffsetCorrection(node);
          assert dotProduct >= 0;
          float adjustedDistance =
              dotProduct * constMultiplier + offsetCorrection + nodeOffsetCorrection;
          return Math.max((1 + adjustedDistance) / 2, 0);
        }
      };
    }

    @Override
    public Int4DotProductSupplier copy() throws IOException {
      return new Int4DotProductSupplier(input.clone(), values, constMultiplier, compressed);
    }
  }

  static final class EuclideanSupplier
      extends Lucene99MemorySegmentScalarQuantizedVectorScorerSupplier {

    EuclideanSupplier(
        MemorySegmentAccessInput input,
        RandomAccessQuantizedByteVectorValues values,
        float constMultiplier) {
      super(input, values, constMultiplier);
    }

    @Override
    public RandomVectorScorer scorer(int ord) throws IOException {
      checkOrdinal(ord);
      MemorySegment querySegment = getFirstSegment(ord);
      return new RandomVectorScorer.AbstractRandomVectorScorer(values) {
        @Override
        public float score(int node) throws IOException {
          checkOrdinal(node);
          float raw = PanamaVectorUtilSupport.squareDistance(querySegment, getSecondSegment(node));
          float adjustedDistance = raw * constMultiplier;
          return 1 / (1f + adjustedDistance);
        }
      };
    }

    @Override
    public EuclideanSupplier copy() throws IOException {
      return new EuclideanSupplier(input.clone(), values, constMultiplier);
    }
  }

  static final class MaxInnerProductSupplier
      extends Lucene99MemorySegmentScalarQuantizedVectorScorerSupplier {

    MaxInnerProductSupplier(
        MemorySegmentAccessInput input,
        RandomAccessQuantizedByteVectorValues values,
        float constMultiplier) {
      super(input, values, constMultiplier);
    }

    @Override
    public RandomVectorScorer scorer(int ord) throws IOException {
      checkOrdinal(ord);
      MemorySegment querySegment = getFirstSegment(ord);
      float offsetCorrection = getOffsetCorrection(ord);
      return new RandomVectorScorer.AbstractRandomVectorScorer(values) {
        @Override
        public float score(int node) throws IOException {
          checkOrdinal(node);
          MemorySegment nodeSegment = getSecondSegment(node);
          float dotProduct = PanamaVectorUtilSupport.dotProduct(querySegment, nodeSegment);
          float nodeOffsetCorrection = getOffsetCorrection(node);
          assert dotProduct >= 0;
          float adjustedDistance =
              dotProduct * constMultiplier + offsetCorrection + nodeOffsetCorrection;
          if (adjustedDistance < 0) {
            return 1 / (1 + -1 * adjustedDistance);
          }
          return adjustedDistance + 1;
        }
      };
    }

    @Override
    public MaxInnerProductSupplier copy() throws IOException {
      return new MaxInnerProductSupplier(input.clone(), values, constMultiplier);
    }
  }

  static final class Int4MaxInnerProductSupplier
      extends Lucene99MemorySegmentScalarQuantizedVectorScorerSupplier {

    private final boolean compressed;

    Int4MaxInnerProductSupplier(
        MemorySegmentAccessInput input,
        RandomAccessQuantizedByteVectorValues values,
        float constMultiplier,
        boolean compressed) {
      super(input, values, constMultiplier);
      this.compressed = compressed;
    }

    @Override
    public RandomVectorScorer scorer(int ord) throws IOException {
      checkOrdinal(ord);
      MemorySegment querySegment = getFirstSegment(ord);
      float offsetCorrection = getOffsetCorrection(ord);
      return new RandomVectorScorer.AbstractRandomVectorScorer(values) {
        @Override
        public float score(int node) throws IOException {
          checkOrdinal(node);
          MemorySegment nodeSegment = getSecondSegment(node);
          float dotProduct =
              PanamaVectorUtilSupport.int4DotProduct(querySegment, false, nodeSegment, compressed);
          float nodeOffsetCorrection = getOffsetCorrection(node);
          assert dotProduct >= 0;
          float adjustedDistance =
              dotProduct * constMultiplier + offsetCorrection + nodeOffsetCorrection;
          if (adjustedDistance < 0) {
            return 1 / (1 + -1 * adjustedDistance);
          }
          return adjustedDistance + 1;
        }
      };
    }

    @Override
    public Int4MaxInnerProductSupplier copy() throws IOException {
      return new Int4MaxInnerProductSupplier(input.clone(), values, constMultiplier, compressed);
    }
  }
}
