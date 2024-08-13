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
package org.apache.lucene.util.quantization;

import org.apache.lucene.index.VectorSimilarityFunction;

public class BinaryQuantizer {
  private static final int QUERY_PROJECTIONS = 4;

  public BinaryQuantizer() {}

  public float[] quantizeForIndex(
      float[] vector,
      byte[] destination,
      VectorSimilarityFunction similarityFunction,
      float[] centroid) {
    float[] corrections = new float[2];
    return corrections;
  }

  // TODO, need to return all the corrections
  public float[] quantizeForQuery(
      float[] vector,
      byte[] destination,
      VectorSimilarityFunction similarityFunction,
      float[] centroid) {
    float[] corrections = new float[2];
    return corrections;
  }
}
