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
package org.apache.lucene.codecs.lucene103.blocktree.art;

import java.util.Arrays;

public class ARTUtil {

  //find common prefix length
  public static int commonPrefixLength(byte[] key1, int aFromIndex, int aToIndex,
      byte[] key2, int bFromIndex, int bToIndex) {
    int mismatchIndex = Arrays.mismatch(key1, aFromIndex, aToIndex, key2, bFromIndex, bToIndex);
    if (mismatchIndex == -1) {
      return aToIndex - aFromIndex;
    }

    int aLength = aToIndex - aFromIndex;
    int bLength = bToIndex - bFromIndex;
    int minLength = Math.min(aLength, bLength);
    if (aLength != bLength && mismatchIndex >= minLength) {
      return minLength;
    }
    return mismatchIndex;
  }
}
