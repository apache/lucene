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
import java.util.function.Function;
import org.apache.lucene.codecs.ByteVectorProvider;
import org.apache.lucene.codecs.FloatVectorProvider;
import org.apache.lucene.codecs.VectorSimilarity;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.Bits;

/** A {@link RandomVectorScorer} for scoring random nodes in batches against an abstract query. */
public class RandomVectorScorer implements VectorSimilarity.VectorScorer {

  private final VectorSimilarity.VectorScorer scorer;
  private final int size;
  private final IntToIntFunction ordToDoc;
  private final Function<Bits, Bits> acceptOrds;

  public RandomVectorScorer(
      RandomAccessVectorValues<?> values, VectorSimilarity.VectorScorer scorer) {
    this.scorer = scorer;
    this.size = values.size();
    this.ordToDoc = values::ordToDoc;
    this.acceptOrds = values::getAcceptOrds;
  }

  /**
   * @return the maximum possible ordinal for this scorer
   */
  public int maxOrd() {
    return size;
  }

  /**
   * Translates vector ordinal to the correct document ID. By default, this is an identity function.
   *
   * @param ord the vector ordinal
   * @return the document Id for that vector ordinal
   */
  public int ordToDoc(int ord) {
    return ordToDoc.apply(ord);
  }

  /**
   * Returns the {@link Bits} representing live documents. By default, this is an identity function.
   *
   * @param acceptDocs the accept docs
   * @return the accept docs
   */
  public Bits getAcceptOrds(Bits acceptDocs) {
    return acceptOrds.apply(acceptDocs);
  }

  @Override
  public float scaleScore(float comparisonResult) {
    return scorer.scaleScore(comparisonResult);
  }

  @Override
  public float compare(int node) throws IOException {
    return scorer.compare(node);
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
  public static RandomVectorScorer createFloats(
      final RandomAccessVectorValues<float[]> vectors,
      final VectorSimilarity similarityFunction,
      final float[] query)
      throws IOException {
    if (query.length != vectors.dimension()) {
      throw new IllegalArgumentException(
          "vector query dimension: "
              + query.length
              + " differs from field dimension: "
              + vectors.dimension());
    }
    VectorSimilarity.VectorScorer scorer =
        similarityFunction.getVectorScorer(
            FloatVectorProvider.fromRandomAccessVectorValues(vectors), query);
    return new RandomVectorScorer(vectors, scorer);
  }

  /**
   * Creates a default scorer for float vectors. See {@link #createFloats(RandomAccessVectorValues,
   * VectorSimilarity, float[])}.
   */
  @Deprecated
  public static RandomVectorScorer createFloats(
      final RandomAccessVectorValues<float[]> vectors,
      final VectorSimilarityFunction similarityFunction,
      final float[] query)
      throws IOException {
    return createFloats(
        vectors, VectorSimilarity.fromVectorSimilarityFunction(similarityFunction), query);
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
  public static RandomVectorScorer createBytes(
      final RandomAccessVectorValues<byte[]> vectors,
      final VectorSimilarity similarityFunction,
      final byte[] query)
      throws IOException {
    if (query.length != vectors.dimension()) {
      throw new IllegalArgumentException(
          "vector query dimension: "
              + query.length
              + " differs from field dimension: "
              + vectors.dimension());
    }
    VectorSimilarity.VectorScorer scorer =
        similarityFunction.getVectorScorer(
            ByteVectorProvider.fromRandomAccessVectorValues(vectors), query);
    return new RandomVectorScorer(vectors, scorer);
  }

  /**
   * Creates a default scorer for byte vectors. See {@link #createBytes(RandomAccessVectorValues,
   * VectorSimilarity, byte[])}.
   */
  @Deprecated
  public static RandomVectorScorer createBytes(
      final RandomAccessVectorValues<byte[]> vectors,
      final VectorSimilarityFunction similarityFunction,
      final byte[] query)
      throws IOException {
    return createBytes(
        vectors, VectorSimilarity.fromVectorSimilarityFunction(similarityFunction), query);
  }
}
