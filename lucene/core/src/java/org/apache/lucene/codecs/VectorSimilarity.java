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

import static org.apache.lucene.util.VectorUtil.cosine;
import static org.apache.lucene.util.VectorUtil.dotProduct;
import static org.apache.lucene.util.VectorUtil.scaleMaxInnerProductScore;
import static org.apache.lucene.util.VectorUtil.squareDistance;

import java.io.IOException;
import java.util.Objects;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.NamedSPILoader;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;

/**
 * Vector similarity function interface. Used in search to compare vectors.
 *
 * @lucene.experimental
 */
public abstract class VectorSimilarity implements NamedSPILoader.NamedSPI {

  /** The number of the legacy vector function values. */
  public static final int LEGACY_VALUE_LENGTH = 4;

  /**
   * Returns the new vector similarity function from the legacy given vector similarity function.
   */
  public static VectorSimilarity fromVectorSimilarityFunction(byte vectorSimilarityOrdinal) {
    switch (vectorSimilarityOrdinal) {
      case 0:
        return EuclideanDistanceSimilarity.INSTANCE;
      case 1:
        return DotProductSimilarity.INSTANCE;
      case 2:
        return CosineSimilarity.INSTANCE;
      case 3:
        return MaxInnerProductSimilarity.INSTANCE;
      default:
        throw new IllegalArgumentException(
            "Unknown vector similarity function ordinal " + vectorSimilarityOrdinal);
    }
  }

  /** Returns the legacy vector similarity function from the given vector similarity. */
  @Deprecated
  public static VectorSimilarityFunction toVectorSimilarityFunction(VectorSimilarity similarity) {
    if (similarity == null) {
      throw new IllegalArgumentException("VectorSimilarity cannot be null");
    }
    return similarity.toLegacyVectorSimilarityFunction();
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

  /** Returns the legacy vector similarity function from the given vector similarity. */
  protected VectorSimilarityFunction toLegacyVectorSimilarityFunction() {
    return null;
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

  /** Scales the score of a vector comparison. */
  public abstract float scaleVectorScore(float score);

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
   * @param vectorValues the vector provider for gathering stored vectors
   * @param target the query target vector
   * @return a vector scorer
   * @throws IOException if an error occurs
   */
  public abstract VectorScorer getVectorScorer(
      RandomAccessVectorValues<float[]> vectorValues, float[] target) throws IOException;

  /**
   * Returns a vector comparator for comparing two float vectors.
   *
   * @param vectorValues the vector provider for gathering stored vectors
   * @return a vector comparator for comparing two stored vectors given their ordinals
   * @throws IOException if an error occurs
   */
  public abstract VectorComparator getFloatVectorComparator(
      RandomAccessVectorValues<float[]> vectorValues) throws IOException;

  /**
   * Returns a vector scorer for the given target vector.
   *
   * @param vectorValues the byte vector provider for gathering stored vectors
   * @param target the query target vector
   * @return a vector scorer
   * @throws IOException if an error occurs
   */
  public abstract VectorScorer getVectorScorer(
      RandomAccessVectorValues<byte[]> vectorValues, byte[] target) throws IOException;

  /**
   * Returns a vector comparator for comparing two byte vectors.
   *
   * @param vectorValues the byte vector provider for gathering stored vectors
   * @return a vector comparator for comparing two stored vectors given their ordinals
   * @throws IOException if an error occurs
   */
  public abstract VectorComparator getByteVectorComparator(
      RandomAccessVectorValues<byte[]> vectorValues) throws IOException;

  /** Vector scorer interface. Used to score a provided query vector with a given target vector. */
  public interface VectorScorer {
    /**
     * Scores the given vector ordinal, the nuance here is that scores may be scaled differently
     * than the compare method.
     *
     * @param targetVectorOrd the ordinal of the target vector
     * @return a similarity score between the two vectors
     */
    default float score(int targetVectorOrd) throws IOException {
      return scaleScore(compare(targetVectorOrd));
    }

    /**
     * Scales the comparison result to a score.
     *
     * @param comparisonResult the comparison result
     * @return the scaled score
     */
    float scaleScore(float comparisonResult);

    /** Compares the two vectors */
    float compare(int targetVectorOrd) throws IOException;
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

    /**
     * Scales the comparison result to a score.
     *
     * @param comparisonResult the comparison result
     * @return the scaled score
     */
    float scaleScore(float comparisonResult);

    /**
     * Scores the two vectors, the nuance here is that scores may be scaled differently than the
     * compare method.
     *
     * @param vectorOrd1 the ordinal of the first vector
     * @param vectorOrd2 the ordinal of the second vector
     * @return a similarity score between the two vectors
     * @throws IOException if an error occurs
     */
    default float score(int vectorOrd1, int vectorOrd2) throws IOException {
      return scaleScore(compare(vectorOrd1, vectorOrd2));
    }

    /**
     * Returns a vector scorer for the given target vector ordinal.
     *
     * @param leftOrd the ordinal of the "query" vector
     * @return a vector scorer where `leftOrd` is the query vector
     * @throws IOException if an error occurs
     */
    default VectorScorer asScorer(int leftOrd) throws IOException {
      return new VectorScorer() {
        @Override
        public float scaleScore(float comparisonResult) {
          return VectorComparator.this.scaleScore(comparisonResult);
        }

        @Override
        public float compare(int targetVectorOrd) throws IOException {
          return VectorComparator.this.compare(leftOrd, targetVectorOrd);
        }
      };
    }
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
    public float scaleVectorScore(float comparisonResult) {
      return Math.max((1 + comparisonResult) / 2, 0);
    }

    @Override
    public VectorSimilarityFunction toLegacyVectorSimilarityFunction() {
      return VectorSimilarityFunction.DOT_PRODUCT;
    }

    /** Allow direct comparison of byte vectors. This is only used for testing purposes. */
    public float score(byte[] v1, byte[] v2) {
      float denom = (float) (v1.length * (1 << 15));
      return 0.5f + dotProduct(v1, v2) / denom;
    }

    /** Allow direct comparison of byte vectors. This is only used for testing purposes. */
    public float score(float[] v1, float[] v2) {
      return scaleVectorScore(dotProduct(v1, v2));
    }

    @Override
    public VectorScorer getVectorScorer(
        RandomAccessVectorValues<float[]> vectorProvider, float[] target) {
      return new VectorScorer() {
        @Override
        public float scaleScore(float comparisonResult) {
          return DotProductSimilarity.this.scaleVectorScore(comparisonResult);
        }

        @Override
        public float compare(int targetVectorOrd) throws IOException {
          return dotProduct(target, vectorProvider.vectorValue(targetVectorOrd));
        }
      };
    }

    @Override
    public VectorComparator getFloatVectorComparator(
        RandomAccessVectorValues<float[]> vectorProvider) throws IOException {
      return new VectorComparator() {
        final RandomAccessVectorValues<float[]> vectorProviderCopy = vectorProvider.copy();

        @Override
        public float compare(int vectorOrd1, int vectorOrd2) throws IOException {
          return dotProduct(
              vectorProviderCopy.vectorValue(vectorOrd1), vectorProvider.vectorValue(vectorOrd2));
        }

        @Override
        public float scaleScore(float comparisonResult) {
          return DotProductSimilarity.this.scaleVectorScore(comparisonResult);
        }
      };
    }

    @Override
    public VectorScorer getVectorScorer(
        RandomAccessVectorValues<byte[]> vectorValues, byte[] target) {
      return new VectorScorer() {
        // TODO: this is only needed because of the weird way the legacy similarity function works
        // for byte
        // vectors. We should fix this.
        private final float denom = (float) (vectorValues.dimension() * (1 << 15));

        @Override
        public float scaleScore(float comparisonResult) {
          return 0.5f + comparisonResult / denom;
        }

        @Override
        public float compare(int targetVectorOrd) throws IOException {
          return dotProduct(target, vectorValues.vectorValue(targetVectorOrd));
        }
      };
    }

    @Override
    public VectorComparator getByteVectorComparator(RandomAccessVectorValues<byte[]> vectorValues)
        throws IOException {
      return new VectorComparator() {
        final RandomAccessVectorValues<byte[]> vectorValuesCopy = vectorValues.copy();
        // TODO: this is only needed because of the weird way the legacy similarity function works
        // for byte
        // vectors. We should fix this.
        private final float denom = (float) (vectorValues.dimension() * (1 << 15));

        @Override
        public float compare(int ord1, int ord2) throws IOException {
          return dotProduct(vectorValuesCopy.vectorValue(ord1), vectorValues.vectorValue(ord2));
        }

        @Override
        public float scaleScore(float comparisonResult) {
          return 0.5f + comparisonResult / denom;
        }
      };
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
    public VectorSimilarityFunction toLegacyVectorSimilarityFunction() {
      return VectorSimilarityFunction.COSINE;
    }

    @Override
    public float scaleVectorScore(float comparisonResult) {
      return (1f + comparisonResult) / 2f;
    }

    /** Allow direct comparison of byte vectors. This is only used for testing purposes. */
    public float score(byte[] v1, byte[] v2) {
      return scaleVectorScore(cosine(v1, v2));
    }

    /** Allow direct comparison of byte vectors. This is only used for testing purposes. */
    public float score(float[] v1, float[] v2) {
      return scaleVectorScore(cosine(v1, v2));
    }

    @Override
    public VectorScorer getVectorScorer(
        RandomAccessVectorValues<float[]> vectorProvider, float[] target) {
      return new VectorScorer() {
        @Override
        public float scaleScore(float comparisonResult) {
          return scaleVectorScore(comparisonResult);
        }

        @Override
        public float compare(int targetVectorOrd) throws IOException {
          return VectorUtil.cosine(target, vectorProvider.vectorValue(targetVectorOrd));
        }
      };
    }

    @Override
    public VectorComparator getFloatVectorComparator(
        RandomAccessVectorValues<float[]> vectorProvider) throws IOException {
      return new VectorComparator() {
        final RandomAccessVectorValues<float[]> vectorProviderCopy = vectorProvider.copy();

        @Override
        public float compare(int vectorOrd1, int vectorOrd2) throws IOException {
          return VectorUtil.cosine(
              vectorProviderCopy.vectorValue(vectorOrd1), vectorProvider.vectorValue(vectorOrd2));
        }

        @Override
        public float scaleScore(float comparisonResult) {
          return scaleVectorScore(comparisonResult);
        }
      };
    }

    @Override
    public VectorScorer getVectorScorer(
        RandomAccessVectorValues<byte[]> vectorValues, byte[] target) {
      return new VectorScorer() {
        @Override
        public float scaleScore(float comparisonResult) {
          return scaleVectorScore(comparisonResult);
        }

        @Override
        public float compare(int targetVectorOrd) throws IOException {
          return VectorUtil.cosine(target, vectorValues.vectorValue(targetVectorOrd));
        }
      };
    }

    @Override
    public VectorComparator getByteVectorComparator(RandomAccessVectorValues<byte[]> vectorValues)
        throws IOException {
      return new VectorComparator() {
        final RandomAccessVectorValues<byte[]> vectorValuesCopy = vectorValues.copy();

        @Override
        public float compare(int ord1, int ord2) throws IOException {
          return VectorUtil.cosine(
              vectorValuesCopy.vectorValue(ord1), vectorValues.vectorValue(ord2));
        }

        @Override
        public float scaleScore(float comparisonResult) {
          return scaleVectorScore(comparisonResult);
        }
      };
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
    public VectorSimilarityFunction toLegacyVectorSimilarityFunction() {
      return VectorSimilarityFunction.EUCLIDEAN;
    }

    @Override
    public float scaleVectorScore(float comparisonResult) {
      return 1 / (1 + comparisonResult);
    }

    /** Allow direct comparison of byte vectors. This is only used for testing purposes. */
    public float score(byte[] v1, byte[] v2) {
      return scaleVectorScore(squareDistance(v1, v2));
    }

    /** Allow direct comparison of byte vectors. This is only used for testing purposes. */
    public float score(float[] v1, float[] v2) {
      return scaleVectorScore(squareDistance(v1, v2));
    }

    @Override
    public VectorScorer getVectorScorer(
        RandomAccessVectorValues<float[]> vectorProvider, float[] target) {
      return new VectorScorer() {

        @Override
        public float scaleScore(float comparisonResult) {
          return scaleVectorScore(comparisonResult);
        }

        @Override
        public float compare(int targetVectorOrd) throws IOException {
          return squareDistance(target, vectorProvider.vectorValue(targetVectorOrd));
        }
      };
    }

    @Override
    public VectorComparator getFloatVectorComparator(
        RandomAccessVectorValues<float[]> vectorProvider) throws IOException {
      return new VectorComparator() {
        private final RandomAccessVectorValues<float[]> vectorProviderCopy = vectorProvider.copy();

        @Override
        public float compare(int vectorOrd1, int vectorOrd2) throws IOException {
          return squareDistance(
              vectorProviderCopy.vectorValue(vectorOrd1), vectorProvider.vectorValue(vectorOrd2));
        }

        @Override
        public float scaleScore(float comparisonResult) {
          return scaleVectorScore(comparisonResult);
        }
      };
    }

    @Override
    public VectorScorer getVectorScorer(
        RandomAccessVectorValues<byte[]> vectorValues, byte[] target) {
      return new VectorScorer() {
        @Override
        public float scaleScore(float comparisonResult) {
          return scaleVectorScore(comparisonResult);
        }

        @Override
        public float compare(int targetVectorOrd) throws IOException {
          return squareDistance(target, vectorValues.vectorValue(targetVectorOrd));
        }
      };
    }

    @Override
    public VectorComparator getByteVectorComparator(RandomAccessVectorValues<byte[]> vectorValues)
        throws IOException {
      return new VectorComparator() {
        private final RandomAccessVectorValues<byte[]> vectorValuesCopy = vectorValues.copy();

        @Override
        public float compare(int ord1, int ord2) throws IOException {
          return squareDistance(vectorValuesCopy.vectorValue(ord1), vectorValues.vectorValue(ord2));
        }

        @Override
        public float scaleScore(float comparisonResult) {
          return scaleVectorScore(comparisonResult);
        }
      };
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
    public VectorSimilarityFunction toLegacyVectorSimilarityFunction() {
      return VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;
    }

    /** Allow direct comparison of byte vectors. This is only used for testing purposes. */
    public float score(byte[] v1, byte[] v2) {
      return scaleVectorScore(dotProduct(v1, v2));
    }

    /** Allow direct comparison of byte vectors. This is only used for testing purposes. */
    public float score(float[] v1, float[] v2) {
      return scaleVectorScore(dotProduct(v1, v2));
    }

    @Override
    public float scaleVectorScore(float comparisonResult) {
      return scaleMaxInnerProductScore(comparisonResult);
    }

    @Override
    public VectorScorer getVectorScorer(
        RandomAccessVectorValues<float[]> vectorProvider, float[] target) {
      return new VectorScorer() {
        @Override
        public float scaleScore(float comparisonResult) {
          return scaleVectorScore(comparisonResult);
        }

        @Override
        public float compare(int targetVectorOrd) throws IOException {
          return dotProduct(target, vectorProvider.vectorValue(targetVectorOrd));
        }
      };
    }

    @Override
    public VectorComparator getFloatVectorComparator(
        RandomAccessVectorValues<float[]> vectorProvider) throws IOException {
      return new VectorComparator() {
        private final RandomAccessVectorValues<float[]> vectorProviderCopy = vectorProvider.copy();

        @Override
        public float compare(int vectorOrd1, int vectorOrd2) throws IOException {
          return dotProduct(
              vectorProviderCopy.vectorValue(vectorOrd1), vectorProvider.vectorValue(vectorOrd2));
        }

        @Override
        public float scaleScore(float comparisonResult) {
          return scaleVectorScore(comparisonResult);
        }
      };
    }

    @Override
    public VectorScorer getVectorScorer(
        RandomAccessVectorValues<byte[]> vectorValues, byte[] target) {
      return new VectorScorer() {
        @Override
        public float scaleScore(float comparisonResult) {
          return scaleVectorScore(comparisonResult);
        }

        @Override
        public float compare(int targetVectorOrd) throws IOException {
          return dotProduct(target, vectorValues.vectorValue(targetVectorOrd));
        }
      };
    }

    @Override
    public VectorComparator getByteVectorComparator(RandomAccessVectorValues<byte[]> vectorValues)
        throws IOException {
      return new VectorComparator() {
        private final RandomAccessVectorValues<byte[]> vectorValuesCopy = vectorValues.copy();

        @Override
        public float compare(int ord1, int ord2) throws IOException {
          return dotProduct(vectorValuesCopy.vectorValue(ord1), vectorValues.vectorValue(ord2));
        }

        @Override
        public float scaleScore(float comparisonResult) {
          return scaleVectorScore(comparisonResult);
        }
      };
    }
  }
}
