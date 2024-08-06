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

package org.apache.lucene.codecs.hnsw;

import java.io.IOException;
import org.apache.lucene.index.MultiVectorSimilarityFunction;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;

/**
 * Default multi-vector implementation of {@link FlatVectorsScorer}.
 *
 * @lucene.experimental
 */
// noCommit - pending tests
public class DefaultFlatMultiVectorScorer extends DefaultFlatVectorScorer {

  public static final DefaultFlatMultiVectorScorer INSTANCE = new DefaultFlatMultiVectorScorer();

  @Override
  public RandomVectorScorerSupplier getRandomMultiVectorScorerSupplier(
      MultiVectorSimilarityFunction similarityFunction, RandomAccessVectorValues values)
      throws IOException {
    if (values instanceof RandomAccessVectorValues.Floats floatMultiVectorValues) {
      return new FloatScoringSupplier(floatMultiVectorValues, similarityFunction);
    } else if (values instanceof RandomAccessVectorValues.Bytes byteMultiVectorValues) {
      return new ByteScoringSupplier(byteMultiVectorValues, similarityFunction);
    }
    throw new IllegalArgumentException(
        "MultiVector values must be an instance of RandomAccessVectorValues.Floats or RandomAccessVectorValues.Bytes");
  }

  @Override
  public RandomVectorScorer getRandomMultiVectorScorer(
      MultiVectorSimilarityFunction similarityFunction,
      RandomAccessVectorValues values,
      float[] target)
      throws IOException {
    assert values instanceof RandomAccessVectorValues.Floats;
    if (target.length % values.dimension() != 0) {
      throw new IllegalArgumentException(
          "query multiVector dimension differs from multiVector field dimension: "
              + values.dimension());
    }
    return new FloatMultiVectorScorer(
        (RandomAccessVectorValues.Floats) values, target, similarityFunction);
  }

  @Override
  public RandomVectorScorer getRandomMultiVectorScorer(
      MultiVectorSimilarityFunction similarityFunction,
      RandomAccessVectorValues values,
      byte[] target)
      throws IOException {
    assert values instanceof RandomAccessVectorValues.Bytes;
    if (target.length % values.dimension() != 0) {
      throw new IllegalArgumentException(
          "query multiVector dimension differs from multiVector field dimension: "
              + values.dimension());
    }
    return new ByteMultiVectorScorer(
        (RandomAccessVectorValues.Bytes) values, target, similarityFunction);
  }

  @Override
  public String toString() {
    return "DefaultFlatMultiVectorScorer()";
  }

  /** RandomVectorScorerSupplier for bytes multiVectors */
  private static final class ByteScoringSupplier implements RandomVectorScorerSupplier {
    private final RandomAccessVectorValues.Bytes multiVectors;
    private final RandomAccessVectorValues.Bytes multiVectors1;
    private final RandomAccessVectorValues.Bytes multiVectors2;
    private final MultiVectorSimilarityFunction similarityFunction;
    private final int dimension;

    private ByteScoringSupplier(
        RandomAccessVectorValues.Bytes multiVectors,
        MultiVectorSimilarityFunction similarityFunction)
        throws IOException {
      this.multiVectors = multiVectors;
      multiVectors1 = multiVectors.copy();
      multiVectors2 = multiVectors.copy();
      this.similarityFunction = similarityFunction;
      this.dimension = multiVectors.dimension();
    }

    @Override
    public RandomVectorScorer scorer(int ord) {
      return new RandomVectorScorer.AbstractRandomVectorScorer(multiVectors) {
        @Override
        public float score(int node) throws IOException {
          return similarityFunction.compare(
              multiVectors1.vectorValue(ord), multiVectors2.vectorValue(node), dimension);
        }
      };
    }

    @Override
    public RandomVectorScorerSupplier copy() throws IOException {
      return new ByteScoringSupplier(multiVectors, similarityFunction);
    }
  }

  /** RandomVectorScorerSupplier for Float vector */
  private static final class FloatScoringSupplier implements RandomVectorScorerSupplier {
    private final RandomAccessVectorValues.Floats multiVectors;
    private final RandomAccessVectorValues.Floats multiVectors1;
    private final RandomAccessVectorValues.Floats multiVectors2;
    private final MultiVectorSimilarityFunction similarityFunction;
    private final int dimension;

    private FloatScoringSupplier(
        RandomAccessVectorValues.Floats multiVectors,
        MultiVectorSimilarityFunction similarityFunction)
        throws IOException {
      this.multiVectors = multiVectors;
      multiVectors1 = multiVectors.copy();
      multiVectors2 = multiVectors.copy();
      this.similarityFunction = similarityFunction;
      this.dimension = multiVectors.dimension();
    }

    @Override
    public RandomVectorScorer scorer(int ord) {
      return new RandomVectorScorer.AbstractRandomVectorScorer(multiVectors) {
        @Override
        public float score(int node) throws IOException {
          return similarityFunction.compare(
              multiVectors1.vectorValue(ord), multiVectors2.vectorValue(node), dimension);
        }
      };
    }

    @Override
    public RandomVectorScorerSupplier copy() throws IOException {
      return new FloatScoringSupplier(multiVectors, similarityFunction);
    }
  }

  /** A {@link RandomVectorScorer} for float multiVectors. */
  private static class FloatMultiVectorScorer
      extends RandomVectorScorer.AbstractRandomVectorScorer {
    private final RandomAccessVectorValues.Floats values;
    private final float[] query;
    private final MultiVectorSimilarityFunction similarityFunction;
    private final int dimension;

    public FloatMultiVectorScorer(
        RandomAccessVectorValues.Floats values,
        float[] query,
        MultiVectorSimilarityFunction similarityFunction) {
      super(values);
      this.values = values;
      this.query = query;
      this.similarityFunction = similarityFunction;
      this.dimension = values.dimension();
    }

    @Override
    public float score(int node) throws IOException {
      return similarityFunction.compare(query, values.vectorValue(node), dimension);
    }
  }

  /** A {@link RandomVectorScorer} for byte multiVectors. */
  private static class ByteMultiVectorScorer extends RandomVectorScorer.AbstractRandomVectorScorer {
    private final RandomAccessVectorValues.Bytes values;
    private final byte[] query;
    private final MultiVectorSimilarityFunction similarityFunction;
    private final int dimension;

    public ByteMultiVectorScorer(
        RandomAccessVectorValues.Bytes values,
        byte[] query,
        MultiVectorSimilarityFunction similarityFunction) {
      super(values);
      this.values = values;
      this.query = query;
      this.similarityFunction = similarityFunction;
      this.dimension = values.dimension();
    }

    @Override
    public float score(int node) throws IOException {
      return similarityFunction.compare(query, values.vectorValue(node), dimension);
    }
  }
}
