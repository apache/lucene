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

package org.apache.lucene.internal.vectorization;

/**
 * Interface for implementations of VectorUtil support.
 *
 * @lucene.internal
 */
public interface VectorUtilSupport {

  /** Calculates the dot product of the given float arrays. */
  float dotProduct(float[] a, float[] b);

  /** Returns the cosine similarity between the two vectors. */
  float cosine(float[] v1, float[] v2);

  /** Returns the sum of squared differences of the two vectors. */
  float squareDistance(float[] a, float[] b);

  /** Returns the dot product computed over signed bytes. */
  int dotProduct(byte[] a, byte[] b);

  /** Returns the dot product over the computed bytes, assuming the values are int4 encoded. */
  int int4DotProduct(byte[] a, boolean apacked, byte[] b, boolean bpacked);

  /** Returns the cosine similarity between the two byte vectors. */
  float cosine(byte[] a, byte[] b);

  /** Returns the sum of squared differences of the two byte vectors. */
  int squareDistance(byte[] a, byte[] b);
}
