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

import org.apache.lucene.index.TensorSimilarityFunction;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;

import java.io.IOException;

/**
 * Default implementation of {@link FlatTensorsScorer}.
 *
 * @lucene.experimental
 */
public class DefaultFlatTensorScorer implements FlatTensorsScorer {
  @Override
  public RandomVectorScorerSupplier getRandomTensorScorerSupplier(
      TensorSimilarityFunction similarityFunction, RandomAccessVectorValues values)
      throws IOException {
    if (values instanceof RandomAccessVectorValues.Floats floatTensorValues) {
      return new FloatScoringSupplier(floatTensorValues, similarityFunction);
    } else if (values instanceof RandomAccessVectorValues.Bytes byteTensorValues) {
      return new ByteScoringSupplier(byteTensorValues, similarityFunction);
    }
    throw new IllegalArgumentException(
        "tensorValues must be an instance of RandomAccessVectorValues.Floats or RandomAccessVectorValues.Bytes");
  }

  @Override
  public RandomVectorScorer getRandomTensorScorer(
      TensorSimilarityFunction similarityFunction,
      RandomAccessVectorValues values,
      float[] target)
      throws IOException {
    assert values instanceof RandomAccessVectorValues.Floats;
    if (target.length % values.dimension() != 0) {
      throw new IllegalArgumentException(
          "query tensor dimension differs from tensor field dimension: " + values.dimension());
    }
    return new FloatTensorScorer((RandomAccessVectorValues.Floats) values, target, similarityFunction);
  }

  @Override
  public RandomVectorScorer getRandomTensorScorer(
      TensorSimilarityFunction similarityFunction,
      RandomAccessVectorValues values,
      byte[] target)
      throws IOException {
    assert values instanceof RandomAccessVectorValues.Bytes;
    if (target.length % values.dimension() != 0) {
      throw new IllegalArgumentException(
          "query tensor dimension differs from tensor field dimension: " + values.dimension());
    }
    return new ByteTensorScorer(
        (RandomAccessVectorValues.Bytes) values, target, similarityFunction);
  }

  @Override
  public String toString() {
    return "DefaultFlatTensorScorer()";
  }

  /** RandomVectorScorerSupplier for bytes tensors */
  private static final class ByteScoringSupplier implements RandomVectorScorerSupplier {
    private final RandomAccessVectorValues.Bytes tensors;
    private final RandomAccessVectorValues.Bytes tensors1;
    private final RandomAccessVectorValues.Bytes tensors2;
    private final TensorSimilarityFunction similarityFunction;
    private final int dimension;

    private ByteScoringSupplier(
        RandomAccessVectorValues.Bytes tensors, TensorSimilarityFunction similarityFunction)
        throws IOException {
      this.tensors = tensors;
      tensors1 = tensors.copy();
      tensors2 = tensors.copy();
      this.similarityFunction = similarityFunction;
      this.dimension = tensors.dimension();
    }

    @Override
    public RandomVectorScorer scorer(int ord) {
      return new RandomVectorScorer.AbstractRandomVectorScorer(tensors) {
        @Override
        public float score(int node) throws IOException {
          return similarityFunction.compare(tensors1.vectorValue(ord), tensors2.vectorValue(node), dimension);
        }
      };
    }

    @Override
    public RandomVectorScorerSupplier copy() throws IOException {
      return new ByteScoringSupplier(tensors, similarityFunction);
    }
  }

  /** RandomVectorScorerSupplier for Float vector */
  private static final class FloatScoringSupplier implements RandomVectorScorerSupplier {
    private final RandomAccessVectorValues.Floats tensors;
    private final RandomAccessVectorValues.Floats tensors1;
    private final RandomAccessVectorValues.Floats tensors2;
    private final TensorSimilarityFunction similarityFunction;
    private final int dimension;

    private FloatScoringSupplier(
        RandomAccessVectorValues.Floats tensors, TensorSimilarityFunction similarityFunction)
        throws IOException {
      this.tensors = tensors;
      tensors1 = tensors.copy();
      tensors2 = tensors.copy();
      this.similarityFunction = similarityFunction;
      this.dimension = tensors.dimension();
    }

    @Override
    public RandomVectorScorer scorer(int ord) {
      return new RandomVectorScorer.AbstractRandomVectorScorer(tensors) {
        @Override
        public float score(int node) throws IOException {
          return similarityFunction.compare(tensors1.vectorValue(ord), tensors2.vectorValue(node), dimension);
        }
      };
    }

    @Override
    public RandomVectorScorerSupplier copy() throws IOException {
      return new FloatScoringSupplier(tensors, similarityFunction);
    }
  }

  /** A {@link RandomVectorScorer} for float tensors. */
  private static class FloatTensorScorer extends RandomVectorScorer.AbstractRandomVectorScorer {
    private final RandomAccessVectorValues.Floats values;
    private final float[] query;
    private final TensorSimilarityFunction similarityFunction;
    private final int dimension;

    public FloatTensorScorer(
        RandomAccessVectorValues.Floats values,
        float[] query,
        TensorSimilarityFunction similarityFunction) {
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

  /** A {@link RandomVectorScorer} for byte tensors. */
  private static class ByteTensorScorer extends RandomVectorScorer.AbstractRandomVectorScorer {
    private final RandomAccessVectorValues.Bytes values;
    private final byte[] query;
    private final TensorSimilarityFunction similarityFunction;
    private final int dimension;

    public ByteTensorScorer(
        RandomAccessVectorValues.Bytes values,
        byte[] query,
        TensorSimilarityFunction similarityFunction) {
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
