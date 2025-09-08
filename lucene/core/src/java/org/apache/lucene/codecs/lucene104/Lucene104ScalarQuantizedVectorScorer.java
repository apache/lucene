package org.apache.lucene.codecs.lucene104;

import static org.apache.lucene.index.VectorSimilarityFunction.COSINE;
import static org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;
import static org.apache.lucene.index.VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;

import java.io.IOException;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.apache.lucene.util.quantization.OptimizedScalarQuantizer;

/** Vector scorer over OptimizedScalarQuantized vectors */
public class Lucene104ScalarQuantizedVectorScorer implements FlatVectorsScorer {
  private final FlatVectorsScorer nonQuantizedDelegate;

  public Lucene104ScalarQuantizedVectorScorer(FlatVectorsScorer nonQuantizedDelegate) {
    this.nonQuantizedDelegate = nonQuantizedDelegate;
  }

  @Override
  public RandomVectorScorerSupplier getRandomVectorScorerSupplier(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues)
      throws IOException {
    if (vectorValues instanceof QuantizedByteVectorValues qv) {
      return new ScalarQuantizedVectorScorerSupplier(qv, similarityFunction);
    }
    // It is possible to get to this branch during initial indexing and flush
    return nonQuantizedDelegate.getRandomVectorScorerSupplier(similarityFunction, vectorValues);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues, float[] target)
      throws IOException {
    if (vectorValues instanceof QuantizedByteVectorValues qv) {
      OptimizedScalarQuantizer quantizer = qv.getQuantizer();
      byte[] targetQuantized = new byte[target.length];
      // We make a copy as the quantization process mutates the input
      float[] copy = ArrayUtil.copyOfSubArray(target, 0, target.length);
      if (similarityFunction == COSINE) {
        VectorUtil.l2normalize(copy);
      }
      target = copy;
      // XXX parameterize the number of bits
      var targetCorrectiveTerms =
          quantizer.scalarQuantize(target, targetQuantized, (byte) 8, qv.getCentroid());
      return new RandomVectorScorer.AbstractRandomVectorScorer(qv) {
        @Override
        public float score(int node) throws IOException {
          return quantizedScore(
              targetQuantized, targetCorrectiveTerms, qv, node, similarityFunction);
        }
      };
    }
    // It is possible to get to this branch during initial indexing and flush
    return nonQuantizedDelegate.getRandomVectorScorer(similarityFunction, vectorValues, target);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues, byte[] target)
      throws IOException {
    return nonQuantizedDelegate.getRandomVectorScorer(similarityFunction, vectorValues, target);
  }

  @Override
  public String toString() {
    return "Lucene104ScalarQuantizedVectorScorer(nonQuantizedDelegate="
        + nonQuantizedDelegate
        + ")";
  }

  private static final class ScalarQuantizedVectorScorerSupplier
      implements RandomVectorScorerSupplier {
    private final QuantizedByteVectorValues targetValues;
    private final QuantizedByteVectorValues values;
    private final VectorSimilarityFunction similarity;

    public ScalarQuantizedVectorScorerSupplier(
        QuantizedByteVectorValues values, VectorSimilarityFunction similarity) throws IOException {
      this.targetValues = values.copy();
      this.values = values;
      this.similarity = similarity;
    }

    @Override
    public UpdateableRandomVectorScorer scorer() throws IOException {
      return new UpdateableRandomVectorScorer.AbstractUpdateableRandomVectorScorer(values) {
        private byte[] targetVector;
        private OptimizedScalarQuantizer.QuantizationResult targetCorrectiveTerms;

        @Override
        public float score(int node) throws IOException {
          return quantizedScore(targetVector, targetCorrectiveTerms, values, node, similarity);
        }

        @Override
        public void setScoringOrdinal(int node) throws IOException {
          targetVector = targetValues.vectorValue(node);
          targetCorrectiveTerms = targetValues.getCorrectiveTerms(node);
        }
      };
    }

    @Override
    public RandomVectorScorerSupplier copy() throws IOException {
      return new ScalarQuantizedVectorScorerSupplier(values.copy(), similarity);
    }
  }

  public static final float EIGHT_BIT_SCALE = 1f / ((1 << 8) - 1);

  // XXX factor this out to share with Lucene102BinaryFlatVectorsScorer
  // we need to know how many bits were used for both the query and index vector for scaling.
  static float quantizedScore(
      byte[] quantizedQuery,
      OptimizedScalarQuantizer.QuantizationResult queryCorrections,
      QuantizedByteVectorValues targetVectors,
      int targetOrd,
      VectorSimilarityFunction similarityFunction)
      throws IOException {
    byte[] binaryCode = targetVectors.vectorValue(targetOrd);
    float qcDist = VectorUtil.uint8DotProduct(quantizedQuery, binaryCode);
    OptimizedScalarQuantizer.QuantizationResult indexCorrections =
        targetVectors.getCorrectiveTerms(targetOrd);
    float x1 = indexCorrections.quantizedComponentSum();
    float ax = indexCorrections.lowerInterval();
    // Here we assume `lx` is simply bit vectors, so the scaling isn't necessary
    float lx = (indexCorrections.upperInterval() - ax) * EIGHT_BIT_SCALE;
    float ay = queryCorrections.lowerInterval();
    float ly = (queryCorrections.upperInterval() - ay) * EIGHT_BIT_SCALE;
    float y1 = queryCorrections.quantizedComponentSum();
    float score =
        ax * ay * targetVectors.dimension() + ay * lx * x1 + ax * ly * y1 + lx * ly * qcDist;
    // For euclidean, we need to invert the score and apply the additional correction, which is
    // assumed to be the squared l2norm of the centroid centered vectors.
    if (similarityFunction == EUCLIDEAN) {
      score =
          queryCorrections.additionalCorrection()
              + indexCorrections.additionalCorrection()
              - 2 * score;
      return Math.max(1 / (1f + score), 0);
    } else {
      // For cosine and max inner product, we need to apply the additional correction, which is
      // assumed to be the non-centered dot-product between the vector and the centroid
      score +=
          queryCorrections.additionalCorrection()
              + indexCorrections.additionalCorrection()
              - targetVectors.getCentroidDP();
      if (similarityFunction == MAXIMUM_INNER_PRODUCT) {
        return VectorUtil.scaleMaxInnerProductScore(score);
      }
      return Math.max((1f + score) / 2f, 0);
    }
  }
}
