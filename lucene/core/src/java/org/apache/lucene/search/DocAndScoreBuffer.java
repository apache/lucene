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
package org.apache.lucene.search;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IntsRef;

/**
 * Wrapper around parallel arrays storing doc IDs and their corresponding scores.
 *
 * @lucene.internal
 */
public final class DocAndScoreBuffer {

  private static final float[] EMPTY_FLOATS = new float[0];

  /** Doc IDs */
  public int[] docs = IntsRef.EMPTY_INTS;

  /** Scores */
  public float[] scores = EMPTY_FLOATS;

  /** Number of valid entries in the doc ID and score arrays. */
  public int size;

  /** Sole constructor. */
  public DocAndScoreBuffer() {}

  /** Grow both arrays to ensure that they can store at least the given number of entries. */
  public void growNoCopy(int minSize) {
    if (docs.length < minSize) {
      docs = ArrayUtil.growNoCopy(docs, minSize);
      scores = new float[docs.length];
    }
  }
}
