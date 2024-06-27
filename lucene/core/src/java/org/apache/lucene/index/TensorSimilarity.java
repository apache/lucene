package org.apache.lucene.index;

public interface TensorSimilarity {

  /**
   * Calculates a similarity score between the two tensors with a specified function. Higher
   * similarity scores correspond to closer vectors.
   *
   * @param t1 a tensor with non-empty vectors All vector values are concatenated in a single packed
   *     array.
   * @param t2 another tensor, vectors of the same dimension as t1. All vector values are
   *     concatenated in a single packed array.
   * @return the value of the similarity function applied to the two tensors
   */
  float compare(float[] t1, float[] t2, int dimension);

  /**
   * Calculates a similarity score between the two tensors with a specified function. Higher
   * similarity scores correspond to closer vectors.
   *
   * @param t1 a tensor with non-empty vectors. All vector values are concatenated in a single
   *     packed array.
   * @param t2 another tensor, vectors of the same dimension as t1. All vector values are
   *     concatenated in a single packed array.
   * @return the value of the similarity function applied to the two tensors
   */
  float compare(byte[] t1, byte[] t2, int dimension);
}
