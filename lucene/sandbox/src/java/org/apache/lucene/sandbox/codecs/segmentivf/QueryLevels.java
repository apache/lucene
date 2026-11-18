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

package org.apache.lucene.sandbox.codecs.segmentivf;

/**
 * A search's query quantized to INT8 levels ({@link FineCodec.Int8#queryLevels}), on first use, and
 * shared by every tier that builds on them, so a search quantizes its query at most once.
 *
 * <p>The INT8 fine tier scores with them and the S1 coarse tier builds its tables from them ({@link
 * CoarseCodec.S1}). A tier is not required to use them, and nothing is quantized unless a tier
 * asks.
 *
 * <p>They are of the query at its own length, as the rerank scores it; the coarse tier compares the
 * unit query, whose levels are the same up to rounding, at {@link #unitScale}.
 */
final class QueryLevels {
  private final float[] rotated;
  private final float norm;
  private byte[] levels;
  private float scale;

  /** Levels of {@code rotated}, a rotated query at its own length {@code norm}. */
  QueryLevels(float[] rotated, float norm) {
    this.rotated = rotated;
    this.norm = norm;
  }

  /** The rotated query these are the levels of. */
  float[] rotated() {
    return rotated;
  }

  /** The levels, {@code dim} bytes; callers must not modify them. */
  byte[] levels() {
    if (levels == null) {
      levels = new byte[rotated.length];
      scale = FineCodec.Int8.queryLevels(rotated, rotated.length, levels);
    }
    return levels;
  }

  /** The scale of the levels for the query at its own length. */
  float scale() {
    levels();
    return scale;
  }

  /** The scale of the levels for the unit query: max-abs scales with the length. */
  float unitScale() {
    return norm == 0f ? scale() : scale() / norm;
  }
}
