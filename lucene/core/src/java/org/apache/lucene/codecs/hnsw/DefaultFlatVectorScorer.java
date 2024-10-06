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
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
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
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues)
      throws IOException {
    switch (vectorValues.getEncoding()) {
      case FLOAT32 -> {
        return new FloatScoringSupplier((FloatVectorValues) vectorValues, similarityFunction);
      }
      case BYTE -> {
        return new ByteScoringSupplier((ByteVectorValues) vectorValues, similarityFunction);
      }
    }
    throw new IllegalArgumentException(
        "vectorValues must be an instance of FloatVectorValues or ByteVectorValues, got a "
            + vectorValues.getClass().getName());
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues, float[] target)
      throws IOException {
    assert vectorValues instanceof FloatVectorValues;
    if (target.length != vectorValues.dimension()) {
      throw new IllegalArgumentException(
          "vector query dimension: "
              + target.length
              + " differs from field dimension: "
              + vectorValues.dimension());
    }
    return new FloatVectorScorer((FloatVectorValues) vectorValues, target, similarityFunction);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues, byte[] target)
      throws IOException {
    assert vectorValues instanceof ByteVectorValues;
    if (target.length != vectorValues.dimension()) {
      throw new IllegalArgumentException(
          "vector query dimension: "
              + target.length
              + " differs from field dimension: "
              + vectorValues.dimension());
    }
    return new ByteVectorScorer((ByteVectorValues) vectorValues, target, similarityFunction);
  }

  @Override
  public String toString() {
    return "DefaultFlatVectorScorer()";
  }

  /** RandomVectorScorerSupplier for bytes vector */
  private static final class ByteScoringSupplier implements RandomVectorScorerSupplier {
    private final ByteVectorValues vectors;
    private final ByteVectorValues.Bytes vectors1;
    private final ByteVectorValues.Bytes vectors2;
    private final VectorSimilarityFunction similarityFunction;

    private ByteScoringSupplier(
        ByteVectorValues vectors, VectorSimilarityFunction similarityFunction) throws IOException {
      this.vectors = vectors;
      vectors1 = vectors.values();
      vectors2 = vectors.values();
      this.similarityFunction = similarityFunction;
    }

    @Override
    public RandomVectorScorer scorer(int ord) {
      return new RandomVectorScorer.AbstractRandomVectorScorer(vectors) {
        @Override
        public float score(int node) throws IOException {
          return similarityFunction.compare(vectors1.get(ord), vectors2.get(node));
        }
      };
    }

    @Override
    public String toString() {
      return "ByteScoringSupplier(similarityFunction=" + similarityFunction + ")";
    }
  }

  /** RandomVectorScorerSupplier for Float vector */
  private static final class FloatScoringSupplier implements RandomVectorScorerSupplier {
    private final FloatVectorValues vectors;
    private final FloatVectorValues.Floats vectors1;
    private final FloatVectorValues.Floats vectors2;
    private final VectorSimilarityFunction similarityFunction;

    private FloatScoringSupplier(
        FloatVectorValues vectors, VectorSimilarityFunction similarityFunction) throws IOException {
      this.vectors = vectors;
      vectors1 = vectors.values();
      vectors2 = vectors.values();
      this.similarityFunction = similarityFunction;
    }

    @Override
    public RandomVectorScorer scorer(int ord) {
      return new RandomVectorScorer.AbstractRandomVectorScorer(vectors) {
        @Override
        public float score(int node) throws IOException {
          return similarityFunction.compare(vectors1.get(ord), vectors2.get(node));
        }
      };
    }

    @Override
    public String toString() {
      return "FloatScoringSupplier(similarityFunction=" + similarityFunction + ")";
    }
  }

  /** A {@link RandomVectorScorer} for float vectors. */
  private static class FloatVectorScorer extends RandomVectorScorer.AbstractRandomVectorScorer {
    private final FloatVectorValues.Floats floats;
    private final float[] query;
    private final VectorSimilarityFunction similarityFunction;

    public FloatVectorScorer(
        FloatVectorValues values, float[] query, VectorSimilarityFunction similarityFunction)
        throws IOException {
      super(values);
      this.floats = values.values();
      this.query = query;
      this.similarityFunction = similarityFunction;
    }

    @Override
    public float score(int node) throws IOException {
      return similarityFunction.compare(query, floats.get(node));
    }
  }

  /** A {@link RandomVectorScorer} for byte vectors. */
  private static class ByteVectorScorer extends RandomVectorScorer.AbstractRandomVectorScorer {
    private final ByteVectorValues.Bytes vectors;
    private final byte[] query;
    private final VectorSimilarityFunction similarityFunction;

    public ByteVectorScorer(
        ByteVectorValues values, byte[] query, VectorSimilarityFunction similarityFunction)
        throws IOException {
      super(values);
      vectors = values.values();
      this.query = query;
      this.similarityFunction = similarityFunction;
    }

    @Override
    public float score(int node) throws IOException {
      return similarityFunction.compare(query, vectors.get(node));
    }
  }
}
