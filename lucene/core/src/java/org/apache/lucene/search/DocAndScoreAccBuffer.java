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
 * Wrapper around parallel arrays storing doc IDs and their corresponding score accumulators.
 *
 * @lucene.internal
 */
public final class DocAndScoreAccBuffer {

  private static final double[] EMPTY_DOUBLES = new double[0];

  /** Doc IDs */
  public int[] docs = IntsRef.EMPTY_INTS;

  /** Scores */
  public double[] scores = EMPTY_DOUBLES;

  /** Number of valid entries in the doc ID and score arrays. */
  public int size;

  /** Sole constructor. */
  public DocAndScoreAccBuffer() {}

  /**
   * Grow both arrays to ensure that they can store at least the given number of entries. Existing
   * content may be discarded.
   */
  public void growNoCopy(int minSize) {
    if (docs.length < minSize) {
      docs = ArrayUtil.growNoCopy(docs, minSize);
      scores = new double[docs.length];
    }
  }

  /**
   * Grow both arrays to ensure that they can store at least the given number of entries. Existing
   * content is preserved.
   */
  public void grow(int minSize) {
    if (docs.length < minSize) {
      docs = ArrayUtil.grow(docs, minSize);
      scores = ArrayUtil.growExact(scores, docs.length);
    }
  }

  /**
   * Copy content from the given {@link DocAndFloatFeatureBuffer}, expanding float scores to
   * doubles.
   */
  public void copyFrom(DocAndFloatFeatureBuffer buffer) {
    growNoCopy(buffer.size);
    System.arraycopy(buffer.docs, 0, docs, 0, buffer.size);
    for (int i = 0; i < buffer.size; ++i) {
      scores[i] = buffer.features[i];
    }
    this.size = buffer.size;
  }
}
