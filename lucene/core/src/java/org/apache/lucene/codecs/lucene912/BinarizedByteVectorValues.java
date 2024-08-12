package org.apache.lucene.codecs.lucene912;

import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.VectorScorer;

import java.io.IOException;

/**
 * A version of {@link ByteVectorValues}, but additionally retrieving score correction values offset for
 * binarization quantization scores.
 *
 * @lucene.experimental
 */
public abstract class BinarizedByteVectorValues extends DocIdSetIterator {
  public abstract float getDistanceToCentroid() throws IOException;
  public abstract float getMagnitude() throws IOException;

  public abstract byte[] vectorValue() throws IOException;

  /** Return the dimension of the vectors */
  public abstract int dimension();

  /**
   * Return the number of vectors for this field.
   *
   * @return the number of vectors returned by this iterator
   */
  public abstract int size();

  @Override
  public final long cost() {
    return size();
  }

  /**
   * Return a {@link VectorScorer} for the given query vector.
   *
   * @param query the query vector
   * @return a {@link VectorScorer} instance or null
   */
  public abstract VectorScorer scorer(float[] query) throws IOException;
}
