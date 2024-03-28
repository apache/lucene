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
package org.apache.lucene.codecs;

import static org.apache.lucene.util.VectorUtil.dotProduct;
import static org.apache.lucene.util.VectorUtil.dotProductScore;
import static org.apache.lucene.util.VectorUtil.scaleMaxInnerProductScore;

import java.io.IOException;
import java.util.Objects;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.NamedSPILoader;
import org.apache.lucene.util.VectorUtil;

/**
 * Vector similarity function interface. Used in search to compare vectors.
 *
 * @lucene.experimental
 */
public abstract class VectorSimilarity implements NamedSPILoader.NamedSPI {

  /**
   * Returns the new vector similarity function from the legacy given vector similarity function.
   */
  public static VectorSimilarity fromVectorSimilarityFunction(VectorSimilarityFunction function) {
    if (function == null) {
      return null;
    }
    switch (function) {
      case DOT_PRODUCT:
        return DotProductSimilarity.INSTANCE;
      case COSINE:
        return CosineSimilarity.INSTANCE;
      case EUCLIDEAN:
        return EuclideanDistanceSimilarity.INSTANCE;
      case MAXIMUM_INNER_PRODUCT:
        return MaxInnerProductSimilarity.INSTANCE;
      default:
        throw new IllegalArgumentException("Unknown vector similarity function: " + function);
    }
  }

  /** Returns the legacy vector similarity function from the given vector similarity. */
  @Deprecated
  public static VectorSimilarityFunction toVectorSimilarityFunction(VectorSimilarity similarity) {
    if (similarity == null) {
      return null;
    }
    if (similarity instanceof DotProductSimilarity) {
      return VectorSimilarityFunction.DOT_PRODUCT;
    } else if (similarity instanceof CosineSimilarity) {
      return VectorSimilarityFunction.COSINE;
    } else if (similarity instanceof EuclideanDistanceSimilarity) {
      return VectorSimilarityFunction.EUCLIDEAN;
    } else if (similarity instanceof MaxInnerProductSimilarity) {
      return VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;
    } else {
      throw new IllegalArgumentException("Unknown vector similarity: " + similarity);
    }
  }

  private static final class Holder {
    private static final NamedSPILoader<VectorSimilarity> LOADER =
        new NamedSPILoader<>(VectorSimilarity.class);

    private Holder() {}

    static NamedSPILoader<VectorSimilarity> getLoader() {
      if (LOADER == null) {
        throw new IllegalStateException(
            "You tried to lookup a VectorSimilarity name before all formats could be initialized. "
                + "This likely happens if you call VectorSimilarity#forName from a VectorSimilarity's ctor.");
      }
      return LOADER;
    }
  }

  /** Looks up a vector similarity by name. */
  public static VectorSimilarity forName(String name) {
    return VectorSimilarity.Holder.getLoader().lookup(name);
  }

  private final String name;

  /** Creates a new vector similarity with the given name. */
  protected VectorSimilarity(String name) {
    NamedSPILoader.checkServiceName(name);
    this.name = name;
  }

  /** Returns the name of the vector similarity. */
  @Override
  public String getName() {
    return name;
  }

  /**
   * Returns true if the vector similarity supports the given vector encoding.
   *
   * @param encoding the vector encoding
   * @return true if the vector similarity supports the given vector encoding
   */
  public boolean supportsVectorEncoding(VectorEncoding encoding) {
    return true;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    VectorSimilarity that = (VectorSimilarity) o;
    return Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }

  /**
   * Returns a vector scorer for the given target vector.
   *
   * @param vectorProvider the vector provider for gathering stored vectors
   * @param target the query target vector
   * @return a vector scorer
   * @throws IOException if an error occurs
   */
  public abstract VectorScorer getVectorScorer(FloatVectorProvider vectorProvider, float[] target)
      throws IOException;

  /**
   * Returns a vector comparator for comparing two float vectors.
   *
   * @param vectorProvider the vector provider for gathering stored vectors
   * @return a vector comparator for comparing two stored vectors given their ordinals
   * @throws IOException if an error occurs
   */
  public abstract VectorComparator getFloatVectorComparator(FloatVectorProvider vectorProvider)
      throws IOException;

  /**
   * Returns a vector scorer for the given target vector.
   *
   * @param byteVectorProvider the byte vector provider for gathering stored vectors
   * @param target the query target vector
   * @return a vector scorer
   * @throws IOException if an error occurs
   */
  public abstract VectorScorer getVectorScorer(ByteVectorProvider byteVectorProvider, byte[] target)
      throws IOException;

  /**
   * Returns a vector comparator for comparing two byte vectors.
   *
   * @param byteVectorProvider the byte vector provider for gathering stored vectors
   * @return a vector comparator for comparing two stored vectors given their ordinals
   * @throws IOException if an error occurs
   */
  public abstract VectorComparator getByteVectorComparator(ByteVectorProvider byteVectorProvider)
      throws IOException;

  /** Vector scorer interface. Used to score a provided query vector with a given target vector. */
  public interface VectorScorer {
    /**
     * Scores the target vector with the query vector.
     *
     * @param targetVectorOrd the ordinal of the target vector
     * @return a similarity score between the two vectors
     */
    float score(int targetVectorOrd) throws IOException;
  }

  /** Vector comparator interface. Used to compare two vectors. */
  public interface VectorComparator {
    /**
     * Compares two vectors.
     *
     * @param vectorOrd1 the ordinal of the first vector
     * @param vectorOrd2 the ordinal of the second vector
     * @return a similarity score between the two vectors
     */
    float compare(int vectorOrd1, int vectorOrd2) throws IOException;
  }

  /** Dot product similarity function. */
  public static class DotProductSimilarity extends VectorSimilarity {
    /** The name of the dot product similarity function. */
    public static final String NAME = "dotProduct";

    /** The dot product similarity function instance. */
    public static final VectorSimilarity INSTANCE = new DotProductSimilarity();

    /** Creates a new dot product similarity function. */
    public DotProductSimilarity() {
      super(NAME);
    }

    @Override
    public VectorScorer getVectorScorer(FloatVectorProvider vectorProvider, float[] target) {
      return targetVectorOrd ->
          Math.max((1 + dotProduct(target, vectorProvider.vectorValue(targetVectorOrd))) / 2, 0);
    }

    @Override
    public VectorComparator getFloatVectorComparator(FloatVectorProvider vectorProvider) {
      return (vectorOrd1, vectorOrd2) ->
          Math.max(
              (1
                      + dotProduct(
                          vectorProvider.vectorValue(vectorOrd1),
                          vectorProvider.vectorValue(vectorOrd2)))
                  / 2,
              0);
    }

    @Override
    public VectorScorer getVectorScorer(ByteVectorProvider byteVectorProvider, byte[] target) {
      return targetVectorOrd ->
          dotProductScore(target, byteVectorProvider.vectorValue(targetVectorOrd));
    }

    @Override
    public VectorComparator getByteVectorComparator(ByteVectorProvider byteVectorProvider) {
      return (ord1, ord2) ->
          dotProductScore(
              byteVectorProvider.vectorValue(ord1), byteVectorProvider.vectorValue(ord2));
    }
  }

  /** Cosine similarity function. */
  public static class CosineSimilarity extends VectorSimilarity {
    /** The name of the cosine similarity function. */
    public static final String NAME = "cosine";

    /** The cosine similarity function instance. */
    public static final VectorSimilarity INSTANCE = new CosineSimilarity();

    /** Creates a new cosine similarity function. */
    public CosineSimilarity() {
      super(NAME);
    }

    @Override
    public VectorScorer getVectorScorer(FloatVectorProvider vectorProvider, float[] target) {
      return targetVectorOrd ->
          Math.max(
              (1 + VectorUtil.cosine(target, vectorProvider.vectorValue(targetVectorOrd))) / 2, 0);
    }

    @Override
    public VectorComparator getFloatVectorComparator(FloatVectorProvider vectorProvider) {
      return (vectorOrd1, vectorOrd2) ->
          Math.max(
              (1
                      + VectorUtil.cosine(
                          vectorProvider.vectorValue(vectorOrd1),
                          vectorProvider.vectorValue(vectorOrd2)))
                  / 2,
              0);
    }

    @Override
    public VectorScorer getVectorScorer(ByteVectorProvider byteVectorProvider, byte[] target) {
      return targetVectorOrd ->
          (1 + VectorUtil.cosine(target, byteVectorProvider.vectorValue(targetVectorOrd))) / 2;
    }

    @Override
    public VectorComparator getByteVectorComparator(ByteVectorProvider byteVectorProvider) {
      return (ord1, ord2) ->
          (1
                  + VectorUtil.cosine(
                      byteVectorProvider.vectorValue(ord1), byteVectorProvider.vectorValue(ord2)))
              / 2;
    }
  }

  /** Euclidean distance similarity function. */
  public static class EuclideanDistanceSimilarity extends VectorSimilarity {
    /** The name of the Euclidean distance similarity function. */
    public static final String NAME = "euclidean";

    /** The Euclidean distance similarity function instance. */
    public static final VectorSimilarity INSTANCE = new EuclideanDistanceSimilarity();

    /** Creates a new Euclidean distance similarity function. */
    public EuclideanDistanceSimilarity() {
      super(NAME);
    }

    @Override
    public VectorScorer getVectorScorer(FloatVectorProvider vectorProvider, float[] target) {
      return targetVectorOrd ->
          1 / (1 + VectorUtil.squareDistance(target, vectorProvider.vectorValue(targetVectorOrd)));
    }

    @Override
    public VectorComparator getFloatVectorComparator(FloatVectorProvider vectorProvider) {
      return (vectorOrd1, vectorOrd2) ->
          1
              / (1
                  + VectorUtil.squareDistance(
                      vectorProvider.vectorValue(vectorOrd1),
                      vectorProvider.vectorValue(vectorOrd2)));
    }

    @Override
    public VectorScorer getVectorScorer(ByteVectorProvider byteVectorProvider, byte[] target) {
      return targetVectorOrd ->
          1
              / (1f
                  + VectorUtil.squareDistance(
                      target, byteVectorProvider.vectorValue(targetVectorOrd)));
    }

    @Override
    public VectorComparator getByteVectorComparator(ByteVectorProvider byteVectorProvider) {
      return (ord1, ord2) ->
          1
              / (1f
                  + VectorUtil.squareDistance(
                      byteVectorProvider.vectorValue(ord1), byteVectorProvider.vectorValue(ord2)));
    }
  }

  /** Max-inner product similarity function. */
  public static class MaxInnerProductSimilarity extends VectorSimilarity {
    /** The name of the max-inner product similarity function. */
    public static final String NAME = "maxInnerProduct";

    /** The max-inner product similarity function instance. */
    public static final VectorSimilarity INSTANCE = new MaxInnerProductSimilarity();

    /** Creates a new max-inner product similarity function. */
    public MaxInnerProductSimilarity() {
      super(NAME);
    }

    @Override
    public VectorScorer getVectorScorer(FloatVectorProvider vectorProvider, float[] target) {
      return targetVectorOrd ->
          scaleMaxInnerProductScore(
              dotProduct(target, vectorProvider.vectorValue(targetVectorOrd)));
    }

    @Override
    public VectorComparator getFloatVectorComparator(FloatVectorProvider vectorProvider) {
      return (vectorOrd1, vectorOrd2) ->
          scaleMaxInnerProductScore(
              dotProduct(
                  vectorProvider.vectorValue(vectorOrd1), vectorProvider.vectorValue(vectorOrd2)));
    }

    @Override
    public VectorScorer getVectorScorer(ByteVectorProvider byteVectorProvider, byte[] target) {
      return targetVectorOrd ->
          scaleMaxInnerProductScore(
              dotProduct(target, byteVectorProvider.vectorValue(targetVectorOrd)));
    }

    @Override
    public VectorComparator getByteVectorComparator(ByteVectorProvider byteVectorProvider) {
      return (ord1, ord2) ->
          scaleMaxInnerProductScore(
              dotProduct(
                  byteVectorProvider.vectorValue(ord1), byteVectorProvider.vectorValue(ord2)));
    }
  }
}
