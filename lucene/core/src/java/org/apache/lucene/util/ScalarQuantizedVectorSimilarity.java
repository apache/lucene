package org.apache.lucene.util;

import static org.apache.lucene.util.VectorUtil.scaleMaxInnerProductScore;

public interface ScalarQuantizedVectorSimilarity {

  float score(byte[] storedVector, float vectorOffset);

  float offset(byte[] quantizedVector, ScalarQuantizer scalarQuantizer);

  class Euclidean implements ScalarQuantizedVectorSimilarity {
    private final byte[] quantizedVector;
    private final float constMultiplier;

    public Euclidean(float[] queryVector, ScalarQuantizer scalarQuantizer) {
      this.quantizedVector = scalarQuantizer.quantize(queryVector);
      this.constMultiplier = scalarQuantizer.getAlpha() * scalarQuantizer.getAlpha();
    }

    @Override
    public float score(byte[] storedVector, float unused) {
      int squareDistance = VectorUtil.squareDistance(storedVector, quantizedVector);
      float adjustedDistance = squareDistance * constMultiplier;
      return 1 / (1f + adjustedDistance);
    }

    @Override
    public float offset(byte[] quantizedVector, ScalarQuantizer scalarQuantizer) {
      return 0f;
    }
  }

  class DotProduct implements ScalarQuantizedVectorSimilarity {
    protected final byte[] quantizedVector;
    protected final float vectorOffset;
    protected final float constMultiplier;

    public DotProduct(float[] queryVector, ScalarQuantizer scalarQuantizer) {
      this.quantizedVector = scalarQuantizer.quantize(queryVector);
      this.vectorOffset = offset(quantizedVector, scalarQuantizer);
      this.constMultiplier = scalarQuantizer.getAlpha() * scalarQuantizer.getAlpha();
    }

    @Override
    public float score(byte[] storedVector, float vectorOffset) {
      int dotProduct = VectorUtil.dotProduct(storedVector, quantizedVector);
      float adjustedDistance = dotProduct * constMultiplier + this.vectorOffset + vectorOffset;
      return (1 + adjustedDistance) / 2;
    }

    @Override
    public float offset(byte[] quantizedVector, ScalarQuantizer scalarQuantizer) {
      int sum = 0;
      for (byte b : quantizedVector) {
        sum += b;
      }
      return sum * scalarQuantizer.getAlpha() * scalarQuantizer.getOffset();
    }
  }

  class Cosine extends DotProduct {
    public Cosine(float[] queryVector, ScalarQuantizer scalarQuantizer) {
      super(
          VectorUtil.l2normalize(ArrayUtil.copyOfSubArray(queryVector, 0, queryVector.length)),
          scalarQuantizer);
    }
  }

  class MaximumInnerProduct extends DotProduct {
    public MaximumInnerProduct(float[] queryVector, ScalarQuantizer scalarQuantizer) {
      super(queryVector, scalarQuantizer);
    }

    @Override
    public float score(byte[] storedVector, float vectorOffset) {
      int dotProduct = VectorUtil.dotProduct(storedVector, quantizedVector);
      float adjustedDistance = dotProduct * constMultiplier + this.vectorOffset + vectorOffset;
      return scaleMaxInnerProductScore(adjustedDistance);
    }
  }
}
