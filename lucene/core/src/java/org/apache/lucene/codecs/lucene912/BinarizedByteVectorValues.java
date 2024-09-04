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
package org.apache.lucene.codecs.lucene912;

import java.io.IOException;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.VectorScorer;

/**
 * A version of {@link ByteVectorValues}, but additionally retrieving score correction values offset
 * for binarization quantization scores.
 *
 * @lucene.experimental
 */
public abstract class BinarizedByteVectorValues extends DocIdSetIterator {
  /** Returns the cluster ID for the vector in the range [0, 255] */
  public abstract short clusterId() throws IOException;

  public static byte encodeClusterIdToByte(short clusterId) {
    byte bClusterId = clusterId <= 127 ? (byte) clusterId : (byte) (clusterId - 256);
    return bClusterId;
  }

  public static short decodeClusterIdFromByte(byte bClusterId) {
    short clusterId = bClusterId >= 0 ? (short) bClusterId : (short) (bClusterId + 256);
    return clusterId;
  }

  public abstract float getDistanceToCentroid() throws IOException;

  public abstract float getMagnitude() throws IOException;

  public abstract float getOOQ() throws IOException;

  public abstract float getNormOC() throws IOException;

  public abstract float getODotC() throws IOException;

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
