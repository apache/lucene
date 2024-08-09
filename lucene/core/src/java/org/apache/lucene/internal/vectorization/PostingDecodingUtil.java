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

import java.io.IOException;

/** Utility class to decode postings. */
public abstract class PostingDecodingUtil {

  /** Read {@code count} longs into {@code b}. This number must not exceed 64. */
  public abstract void readLongs(int count, long[] b) throws IOException;

  /**
   * Read {@code count} longs. This number must not exceed 64. Apply shift {@code bShift} and mask
   * {@code bMask} and store the result in {@code b} starting at offset 0. Apply mask {@code cMask}
   * and store the result in {@code c} starting at offset 0. As a side-effect, this method may
   * internally read up to 7 extra longs and write up to 7 extra longs in {@code b} and {@code c}.
   */
  public abstract void splitLongs(int count, long[] b, int bShift, long bMask, long[] c, long cMask)
      throws IOException;
}
