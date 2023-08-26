package org.apache.lucene.util;

import org.apache.lucene.index.VectorSimilarityFunction;

import static org.apache.lucene.util.VectorUtil.scaleMaxInnerProductScore;

public interface ScalarQuantizedVectorSimilarity {
  static float scoreCorrectiveOffset(VectorSimilarityFunction sim, byte[] quantizedVector, float alpha, float scalarOffset) {
    if (sim == VectorSimilarityFunction.EUCLIDEAN) {
      return 0f;
    }
    int sum = 0;
    for (byte b : quantizedVector) {
      sum += b;
    }
    return sum * alpha * scalarOffset;
  }

  static ScalarQuantizedVectorSimilarity fromVectorSimilarity(VectorSimilarityFunction sim, float constMultiplier) {
    return switch (sim) {
      case EUCLIDEAN -> new Euclidean(constMultiplier);
      case COSINE, DOT_PRODUCT -> new DotProduct(constMultiplier);
      case MAXIMUM_INNER_PRODUCT -> new MaximumInnerProduct(constMultiplier);
    };
  }

  float score(byte[] queryVector, float queryVectorOffset, byte[] storedVector, float vectorOffset);

  class Euclidean implements ScalarQuantizedVectorSimilarity {
    private final float constMultiplier;

    public Euclidean(float constMultiplier) {
      this.constMultiplier = constMultiplier;
    }

    @Override
    public float score(byte[] queryVector, float queryVectorOffset, byte[] storedVector, float vectorOffset) {
      int squareDistance = VectorUtil.squareDistance(storedVector, queryVector);
      float adjustedDistance = squareDistance * constMultiplier;
      return 1 / (1f + adjustedDistance);
    }
  }

  class DotProduct implements ScalarQuantizedVectorSimilarity {
    private final float constMultiplier;

    public DotProduct(float constMultiplier) {
      this.constMultiplier = constMultiplier;
    }

    @Override
    public float score(byte[] queryVector, float queryOffset, byte[] storedVector, float vectorOffset) {
      int dotProduct = VectorUtil.dotProduct(storedVector, queryVector);
      float adjustedDistance = dotProduct * constMultiplier + queryOffset + vectorOffset;
      return (1 + adjustedDistance) / 2;
    }
  }

  class MaximumInnerProduct implements ScalarQuantizedVectorSimilarity {
    private final float constMultiplier;
    public MaximumInnerProduct(float constMultiplier) {
      this.constMultiplier = constMultiplier;
    }

    @Override
    public float score(byte[] queryVector, float queryOffset, byte[] storedVector, float vectorOffset) {
      int dotProduct = VectorUtil.dotProduct(storedVector, queryVector);
      float adjustedDistance = dotProduct * constMultiplier + queryOffset + vectorOffset;
      return scaleMaxInnerProductScore(adjustedDistance);
    }
  }
}
