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

package org.apache.lucene.util.hnsw;

import java.io.IOException;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.Bits;

/** A {@link RandomVectorScorer} for scoring random nodes in batches against an abstract query. */
public interface RandomVectorScorer {
  /**
   * Returns the score between the query and the provided node.
   *
   * @param node a random node in the graph
   * @return the computed score
   */
  float score(int node) throws IOException;

  /**
   * @return the maximum possible ordinal for this scorer
   */
  int maxOrd();

  /**
   * Translates vector ordinal to the correct document ID. By default, this is an identity function.
   *
   * @param ord the vector ordinal
   * @return the document Id for that vector ordinal
   */
  default int ordToDoc(int ord) {
    return ord;
  }

  /**
   * Returns the {@link Bits} representing live documents. By default, this is an identity function.
   *
   * @param acceptDocs the accept docs
   * @return the accept docs
   */
  default Bits getAcceptOrds(Bits acceptDocs) {
    return acceptDocs;
  }

  /**
   * Creates a default scorer for float vectors.
   *
   * <p>WARNING: The {@link RandomAccessVectorValues} given can contain stateful buffers. Avoid
   * using it after calling this function. If you plan to use it again outside the returned {@link
   * RandomVectorScorer}, think about passing a copied version ({@link
   * RandomAccessVectorValues#copy}).
   *
   * @param vectors the underlying storage for vectors
   * @param similarityFunction the similarity function to score vectors
   * @param query the actual query
   */
  static FloatVectorScorer createFloats(
      final RandomAccessVectorValues.Floats vectors,
      final VectorSimilarityFunction similarityFunction,
      final float[] query) {
    if (query.length != vectors.dimension()) {
      throw new IllegalArgumentException(
          "vector query dimension: "
              + query.length
              + " differs from field dimension: "
              + vectors.dimension());
    }
    return new FloatVectorScorer(vectors, query, similarityFunction);
  }

  /**
   * Creates a default scorer for byte vectors.
   *
   * <p>WARNING: The {@link RandomAccessVectorValues} given can contain stateful buffers. Avoid
   * using it after calling this function. If you plan to use it again outside the returned {@link
   * RandomVectorScorer}, think about passing a copied version ({@link
   * RandomAccessVectorValues#copy}).
   *
   * @param vectors the underlying storage for vectors
   * @param similarityFunction the similarity function to use to score vectors
   * @param query the actual query
   */
  static ByteVectorScorer createBytes(
      final RandomAccessVectorValues.Bytes vectors,
      final VectorSimilarityFunction similarityFunction,
      final byte[] query) {
    if (query.length != vectors.dimension()) {
      throw new IllegalArgumentException(
          "vector query dimension: "
              + query.length
              + " differs from field dimension: "
              + vectors.dimension());
    }
    return new ByteVectorScorer(vectors, query, similarityFunction);
  }

  /** Creates a default scorer for random access vectors. */
  abstract class AbstractRandomVectorScorer implements RandomVectorScorer {
    private final RandomAccessVectorValues values;

    /**
     * Creates a new scorer for the given vector values.
     *
     * @param values the vector values
     */
    public AbstractRandomVectorScorer(RandomAccessVectorValues values) {
      this.values = values;
    }

    @Override
    public int maxOrd() {
      return values.size();
    }

    @Override
    public int ordToDoc(int ord) {
      return values.ordToDoc(ord);
    }

    @Override
    public Bits getAcceptOrds(Bits acceptDocs) {
      return values.getAcceptOrds(acceptDocs);
    }
  }

  /** A {@link RandomVectorScorer} for float vectors. */
  class FloatVectorScorer extends AbstractRandomVectorScorer {
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
  class ByteVectorScorer extends AbstractRandomVectorScorer {
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
