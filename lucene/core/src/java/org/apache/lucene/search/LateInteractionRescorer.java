package org.apache.lucene.search;

import org.apache.lucene.index.VectorSimilarityFunction;

/**
 * Rescores top N results from a first pass query using a {@link LateInteractionFloatValuesSource}
 *
 * <p>Typically, you run a low-cost first pass query to collect results from across the index, then
 * use this rescorer to rerank top N hits using multi-vectors, usually from a late interaction model.
 * Multi-vectors should be indexed in the {@link org.apache.lucene.document.LateInteractionField}
 * provided to rescorer.
 *
 * @lucene.experimental
 */
public class LateInteractionRescorer extends DoubleValuesSourceRescorer {

  public LateInteractionRescorer(LateInteractionFloatValuesSource valuesSource) {
    super(valuesSource);
  }

  /**
   * Creates a LateInteractionRescorer for provided query vector.
   */
  public static LateInteractionRescorer create(String fieldName, float[][] queryVector) {
    return create(fieldName, queryVector, VectorSimilarityFunction.COSINE);
  }

  /**
   * Creates a LateInteractionRescorer for provided query vector.
   *
   * <p>Top N results from a first pass query are rescored based on the similarity between {@code queryVector} and
   * the multi-vector indexed in {@code fieldName}. If document does not have a value indexed in {@code fieldName},
   * a 0f score is assigned.
   *
   * @param fieldName the {@link org.apache.lucene.document.LateInteractionField} used for reranking.
   * @param queryVector query multi-vector to use for similarity comparison
   * @param vectorSimilarityFunction function used for vector similarity comparisons
   */
  public static LateInteractionRescorer create(String fieldName, float[][] queryVector, VectorSimilarityFunction vectorSimilarityFunction) {
    final LateInteractionFloatValuesSource valuesSource = new LateInteractionFloatValuesSource(fieldName, queryVector, vectorSimilarityFunction);
    return new LateInteractionRescorer(valuesSource);
  }

  @Override
  protected float combine(float firstPassScore, boolean valuePresent, double sourceValue) {
    return valuePresent ? (float) sourceValue : 0f;
  }

  /**
   * Creates a LateInteractionRescorer for provided query vector.
   *
   * <p>Top N results from a first pass query are rescored based on the similarity between {@code queryVector} and
   * the multi-vector indexed in {@code fieldName}. Falls back to score from the first pass query if a document
   * does not have a value indexed in {@code fieldName}.
   *
   * @param fieldName the {@link org.apache.lucene.document.LateInteractionField} used for reranking.
   * @param queryVector query multi-vector to use for similarity comparison
   * @param vectorSimilarityFunction function used for vector similarity comparisons.
   */
  public static LateInteractionRescorer withFallbackToFirstPassScore(String fieldName, float[][] queryVector, VectorSimilarityFunction vectorSimilarityFunction) {
    final LateInteractionFloatValuesSource valuesSource = new LateInteractionFloatValuesSource(fieldName, queryVector, vectorSimilarityFunction);
    return new LateInteractionRescorer(valuesSource) {
      @Override
      protected float combine(float firstPassScore, boolean valuePresent, double sourceValue) {
        return valuePresent ? (float) sourceValue : firstPassScore;
      }
    };
  }
}
