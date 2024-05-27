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

import static org.apache.lucene.util.VectorUtil.dotProduct;
import static org.apache.lucene.util.VectorUtil.scaleMaxInnerProductScore;

/**
 * Maximum inner product. This is like {@link DotProductVectorSimilarityFunction}, but does not
 * require normalization of the inputs. Should be used when the embedding vectors store useful
 * information within the vector magnitude
 */
public class MaximumInnerProductVectorSimilarityFunction extends VectorSimilarityFunction {

  /** Construct object with function name and ordinal value */
  public MaximumInnerProductVectorSimilarityFunction() {
    super("MIP", 3);
  }

  @Override
  public float compare(float[] v1, float[] v2) {
    return scaleMaxInnerProductScore(dotProduct(v1, v2));
  }

  @Override
  public float compare(byte[] v1, byte[] v2) {
    return scaleMaxInnerProductScore(dotProduct(v1, v2));
  }
}
