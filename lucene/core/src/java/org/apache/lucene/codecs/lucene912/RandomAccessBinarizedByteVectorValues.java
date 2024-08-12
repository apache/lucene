package org.apache.lucene.codecs.lucene912;

import org.apache.lucene.util.hnsw.RandomAccessVectorValues;

import java.io.IOException;

/**
 * Gets access to the vector values stored in a binary format
 *
 * @lucene.experimental
 */
public interface RandomAccessBinarizedByteVectorValues  extends RandomAccessVectorValues.Bytes {
  float getCentroidDistance(int docID) throws IOException;
  float getVectorMagnitude(int docID) throws IOException;
  @Override
  RandomAccessBinarizedByteVectorValues copy() throws IOException;
}
