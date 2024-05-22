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
package org.apache.lucene.index;

import static org.apache.lucene.util.VectorUtil.cosine;

/**
 * Cosine similarity. NOTE: the preferred way to perform cosine similarity is to normalize all
 * vectors to unit length, and instead use {@link DotProductVectorSimilarityFunction}. You should
 * only use this function if you need to preserve the original vectors and cannot normalize them in
 * advance. The similarity score is normalized to assure it is positive.
 *
 * @deprecated Use MAXIMUM_INNER_PRODUCT or DOT_PRODUCT instead
 */
@Deprecated
public class CosineVectorSimilarityFunction extends VectorSimilarityFunction {

  /** Construct object with function name and ordinal value */
  public CosineVectorSimilarityFunction() {
    super("COSINE", 2);
  }

  @Override
  public float compare(float[] v1, float[] v2) {
    return Math.max((1 + cosine(v1, v2)) / 2, 0);
  }

  @Override
  public float compare(byte[] v1, byte[] v2) {
    return (1 + cosine(v1, v2)) / 2;
  }
}
