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

/**
 * Gets access to the query vector values stored in a binary format
 *
 * @lucene.experimental
 */
public interface RandomAccessBinarizedQueryByteVectorValues {
  float getCentroidDistance(int targetOrd, int centroidOrd) throws IOException;

  float getLower(int targetOrd, int centroidOrd) throws IOException;

  float getWidth(int targetOrd, int centroidOrd) throws IOException;

  float getNormVmC(int targetOrd, int centroidOrd) throws IOException;

  float getVDotC(int targetOrd, int centroidOrd) throws IOException;

  float getCDotC(int targetOrd, int centroidOrd) throws IOException;

  int sumQuantizedValues(int targetOrd, int centroidOrd) throws IOException;

  byte[] vectorValue(int targetOrd, int centroidOrd) throws IOException;

  int size() throws IOException;

  int getNumCentroids() throws IOException;

  int dimension();

  RandomAccessBinarizedQueryByteVectorValues copy() throws IOException;
}
