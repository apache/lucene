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

/** The numeric datatype of the vector values. */
public enum VectorEncoding {

  /**
   * Encodes vector using 8 bits of precision per sample. Values provided with higher precision (eg:
   * queries provided as float) *must* be in the range [-128, 127]. NOTE: this can enable
   * significant storage savings and faster searches, at the cost of some possible loss of
   * precision.
   */
  BYTE(1),

  /** Encodes vector using 32 bits of precision per sample in IEEE floating point format. */
  FLOAT32(4);

  /**
   * The number of bytes required to encode a scalar in this format. A vector will nominally require
   * dimension * byteSize bytes of storage.
   */
  public final int byteSize;

  VectorEncoding(int byteSize) {
    this.byteSize = byteSize;
  }
}
