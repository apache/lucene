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
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;

public class Lucene99MemorySegmentScalarQuantizedVectorScorer implements FlatVectorsScorer {

  public static final Lucene99MemorySegmentScalarQuantizedVectorScorer INSTANCE =
      new Lucene99MemorySegmentScalarQuantizedVectorScorer();

  private static final FlatVectorsScorer NON_QUANTIZED_DELEGATE =
      Lucene99MemorySegmentFlatVectorsScorer.INSTANCE;

  @Override
  public RandomVectorScorerSupplier getRandomVectorScorerSupplier(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues)
      throws IOException {
    if (vectorValues instanceof QuantizedByteVectorValues values
        && values.getSlice() instanceof MemorySegmentAccessInput input) {
      return new Lucene99MemorySegmentScalarQuantizedScorerSupplier(
          similarityFunction, values, input);
    }
    // It is possible to get to this branch during initial indexing and flush
    return NON_QUANTIZED_DELEGATE.getRandomVectorScorerSupplier(similarityFunction, vectorValues);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues, float[] target)
      throws IOException {
    if (vectorValues instanceof QuantizedByteVectorValues values
        && values.getSlice() instanceof MemorySegmentAccessInput input) {
      checkDimensions(target.length, vectorValues.dimension());
      return new Lucene99MemorySegmentScalarQuantizedScorer(
          similarityFunction, values, input, target);
    }
    // It is possible to get to this branch during initial indexing and flush
    return NON_QUANTIZED_DELEGATE.getRandomVectorScorer(similarityFunction, vectorValues, target);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues, byte[] target)
      throws IOException {
    return NON_QUANTIZED_DELEGATE.getRandomVectorScorer(similarityFunction, vectorValues, target);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "()";
  }

  private static void checkDimensions(int queryLen, int fieldLen) {
    if (queryLen != fieldLen) {
      throw new IllegalArgumentException(
          "vector query dimension: " + queryLen + " differs from field dimension: " + fieldLen);
    }
  }

  static MemorySegment getSegment(
      MemorySegmentAccessInput input, int entrySize, int node, byte[][] scratch)
      throws IOException {
    long pos = (long) entrySize * node;
    MemorySegment segment = input.segmentSliceOrNull(pos, entrySize);
    if (segment == null) {
      if (scratch[0] == null) {
        scratch[0] = new byte[entrySize];
      }
      input.readBytes(pos, scratch[0], 0, entrySize);
      segment = MemorySegment.ofArray(scratch[0]);
    }
    return segment;
  }

  @FunctionalInterface
  interface MemorySegmentScorer {
    float score(MemorySegment query, MemorySegment doc);
  }

  @FunctionalInterface
  interface FloatToFloatFunction {
    float scale(float score);
  }

  static MemorySegmentScorer factory(
      VectorSimilarityFunction function,
      QuantizedByteVectorValues values,
      boolean isScorerSupplier) {
    return switch (function) {
      case EUCLIDEAN -> {
        if (values.getScalarQuantizer().getBits() < 7) {
          // TODO
          throw new UnsupportedOperationException();
        }
        yield PanamaVectorUtilSupport::squareDistance;
      }
      case DOT_PRODUCT, COSINE, MAXIMUM_INNER_PRODUCT -> {
        if (values.getScalarQuantizer().getBits() <= 4) {
          if (values.getVectorByteLength() != values.dimension()) {
            if (isScorerSupplier) {
              yield (query, doc) -> PanamaVectorUtilSupport.int4DotProduct(query, true, doc, true);
            }
            yield (query, doc) -> PanamaVectorUtilSupport.int4DotProduct(query, false, doc, true);
          }
          yield (query, doc) -> PanamaVectorUtilSupport.int4DotProduct(query, false, doc, false);
        }
        yield PanamaVectorUtilSupport::dotProduct;
      }
    };
  }

  static FloatToFloatFunction factory(VectorSimilarityFunction function) {
    return switch (function) {
      case EUCLIDEAN -> score -> (1 / (1f + score));
      case DOT_PRODUCT, COSINE -> score -> Math.max((1f + score) / 2, 0);
      case MAXIMUM_INNER_PRODUCT -> VectorUtil::scaleMaxInnerProductScore;
    };
  }
}
