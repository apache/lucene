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

  /**
   * Core methods for decoding blocks of docs / freqs / positions / offsets.
   *
   * <ul>
   *   <li>Read {@code count} longs.
   *   <li>For all {@code i} &gt;= 0 so that {@code bShift - i * dec} &gt; 0, apply shift {@code
   *       bShift - i * dec} and store the result in {@code b} at offset {@code count * i}.
   *   <li>Apply mask {@code cMask} and store the result in {@code c} starting at offset {@code
   *       cIndex}.
   * </ul>
   */
  public abstract void splitLongs(
      int count, long[] b, int bShift, int dec, long bMask, long[] c, int cIndex, long cMask)
      throws IOException;
}
