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
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;
import org.apache.lucene.util.quantization.BinaryQuantizer;

/**
 * Gets access to the vector values stored in a binary format
 *
 * @lucene.experimental
 */
public interface RandomAccessBinarizedByteVectorValues extends RandomAccessVectorValues.Bytes {
  /** Returns the centroid distance for the vector */
  float getCentroidDistance(int vectorOrd) throws IOException;

  /** Returns the vector magnitude for the vector */
  float getVectorMagnitude(int vectorOrd) throws IOException;

  /**
   * Returns the cluster ID for the vector in the range [-128 to 127]
   *
   * <p>Negative values should be added to 256 to get a proper cluster id.
   */
  byte getClusterId(int vectorOrd) throws IOException;

  /**
   * @return the quantizer used to quantize the vectors
   */
  BinaryQuantizer getQuantizer();

  /**
   * @return coarse grained centroids for the vectors
   */
  float[][] getCentroids() throws IOException;

  @Override
  RandomAccessBinarizedByteVectorValues copy() throws IOException;
}
