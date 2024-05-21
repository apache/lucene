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
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;

/**
 * Default implementation of {@link FlatVectorsScorer}.
 *
 * @lucene.experimental
 */
public class DefaultFlatVectorScorer implements FlatVectorsScorer {

  public static final DefaultFlatVectorScorer INSTANCE = new DefaultFlatVectorScorer();

  @Override
  public RandomVectorScorerSupplier getRandomVectorScorerSupplier(
      VectorSimilarityFunction similarityFunction, RandomAccessVectorValues vectorValues)
      throws IOException {
    if (vectorValues instanceof RandomAccessVectorValues.Floats) {
      return new FloatScoringSupplier(
          (RandomAccessVectorValues.Floats) vectorValues, similarityFunction);
    } else if (vectorValues instanceof RandomAccessVectorValues.Bytes) {
      return new ByteScoringSupplier(
          (RandomAccessVectorValues.Bytes) vectorValues, similarityFunction);
    }
    throw new IllegalArgumentException(
        "vectorValues must be an instance of RandomAccessVectorValues.Floats or RandomAccessVectorValues.Bytes");
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction,
      RandomAccessVectorValues vectorValues,
      float[] target)
      throws IOException {
    assert vectorValues instanceof RandomAccessVectorValues.Floats;
    if (target.length != vectorValues.dimension()) {
      throw new IllegalArgumentException(
          "vector query dimension: "
              + target.length
              + " differs from field dimension: "
              + vectorValues.dimension());
    }
    return new FloatVectorScorer(
        (RandomAccessVectorValues.Floats) vectorValues, target, similarityFunction);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction,
      RandomAccessVectorValues vectorValues,
      byte[] target)
      throws IOException {
    assert vectorValues instanceof RandomAccessVectorValues.Bytes;
    if (target.length != vectorValues.dimension()) {
      throw new IllegalArgumentException(
          "vector query dimension: "
              + target.length
              + " differs from field dimension: "
              + vectorValues.dimension());
    }
    return new ByteVectorScorer(
        (RandomAccessVectorValues.Bytes) vectorValues, target, similarityFunction);
  }

  @Override
  public String toString() {
    return "DefaultFlatVectorScorer()";
  }

  /** RandomVectorScorerSupplier for bytes vector */
  private static final class ByteScoringSupplier implements RandomVectorScorerSupplier {
    private final RandomAccessVectorValues.Bytes vectors;
    private final RandomAccessVectorValues.Bytes vectors1;
    private final RandomAccessVectorValues.Bytes vectors2;
    private final VectorSimilarityFunction similarityFunction;

    private ByteScoringSupplier(
        RandomAccessVectorValues.Bytes vectors, VectorSimilarityFunction similarityFunction)
        throws IOException {
      this.vectors = vectors;
      vectors1 = vectors.copy();
      vectors2 = vectors.copy();
      this.similarityFunction = similarityFunction;
    }

    @Override
    public RandomVectorScorer scorer(int ord) {
      return new RandomVectorScorer.AbstractRandomVectorScorer(vectors) {
        @Override
        public float score(int node) throws IOException {
          return similarityFunction.compare(vectors1.vectorValue(ord), vectors2.vectorValue(node));
        }
      };
    }

    @Override
    public RandomVectorScorerSupplier copy() throws IOException {
      return new ByteScoringSupplier(vectors, similarityFunction);
    }
  }

  /** RandomVectorScorerSupplier for Float vector */
  private static final class FloatScoringSupplier implements RandomVectorScorerSupplier {
    private final RandomAccessVectorValues.Floats vectors;
    private final RandomAccessVectorValues.Floats vectors1;
    private final RandomAccessVectorValues.Floats vectors2;
    private final VectorSimilarityFunction similarityFunction;

    private FloatScoringSupplier(
        RandomAccessVectorValues.Floats vectors, VectorSimilarityFunction similarityFunction)
        throws IOException {
      this.vectors = vectors;
      vectors1 = vectors.copy();
      vectors2 = vectors.copy();
      this.similarityFunction = similarityFunction;
    }

    @Override
    public RandomVectorScorer scorer(int ord) {
      return new RandomVectorScorer.AbstractRandomVectorScorer(vectors) {
        @Override
        public float score(int node) throws IOException {
          return similarityFunction.compare(vectors1.vectorValue(ord), vectors2.vectorValue(node));
        }
      };
    }

    @Override
    public RandomVectorScorerSupplier copy() throws IOException {
      return new FloatScoringSupplier(vectors, similarityFunction);
    }
  }

  /** A {@link RandomVectorScorer} for float vectors. */
  private static class FloatVectorScorer extends RandomVectorScorer.AbstractRandomVectorScorer {
    private final RandomAccessVectorValues.Floats values;
    private final float[] query;
    private final VectorSimilarityFunction similarityFunction;

    public FloatVectorScorer(
        RandomAccessVectorValues.Floats values,
        float[] query,
        VectorSimilarityFunction similarityFunction) {
      super(values);
      this.values = values;
      this.query = query;
      this.similarityFunction = similarityFunction;
    }

    @Override
    public float score(int node) throws IOException {
      return similarityFunction.compare(query, values.vectorValue(node));
    }
  }

  /** A {@link RandomVectorScorer} for byte vectors. */
  private static class ByteVectorScorer extends RandomVectorScorer.AbstractRandomVectorScorer {
    private final RandomAccessVectorValues.Bytes values;
    private final byte[] query;
    private final VectorSimilarityFunction similarityFunction;

    public ByteVectorScorer(
        RandomAccessVectorValues.Bytes values,
        byte[] query,
        VectorSimilarityFunction similarityFunction) {
      super(values);
      this.values = values;
      this.query = query;
      this.similarityFunction = similarityFunction;
    }

    @Override
    public float score(int node) throws IOException {
      return similarityFunction.compare(query, values.vectorValue(node));
    }
  }
}
