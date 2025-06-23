package org.apache.lucene.search;

import org.apache.lucene.index.VectorSimilarityFunction;

public class LateInteractionRescorer extends DoubleValuesSourceRescorer {

  public LateInteractionRescorer(LateInteractionFloatValuesSource valuesSource) {
    super(valuesSource);
  }

  public LateInteractionRescorer(String fieldName, float[][] queryVector) {
    this(fieldName, queryVector, VectorSimilarityFunction.COSINE);
  }

  public LateInteractionRescorer(String fieldName, float[][] queryVector, VectorSimilarityFunction vectorSimilarityFunction) {
    final LateInteractionFloatValuesSource valuesSource = new LateInteractionFloatValuesSource(fieldName, queryVector, vectorSimilarityFunction);
    super(valuesSource);
    }

  @Override
  protected float combine(float firstPassScore, boolean valuePresent, double sourceValue) {
    return valuePresent ? (float) sourceValue : 0f;
  }

  public static LateInteractionRescorer withFallback(String fieldName, float[][] queryVector, VectorSimilarityFunction vectorSimilarityFunction) {
    final LateInteractionFloatValuesSource valuesSource = new LateInteractionFloatValuesSource(fieldName, queryVector, vectorSimilarityFunction);
    return new LateInteractionRescorer(valuesSource) {
      @Override
      protected float combine(float firstPassScore, boolean valuePresent, double sourceValue) {
        return valuePresent ? (float) sourceValue : firstPassScore;
      }
    };
  }
}
