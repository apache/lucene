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

import java.io.IOException;
import java.util.Set;

/**
 * Per-field doc values metadata and skipper access for a single segment. Global statistics are
 * available without IO. The {@link DocValuesSkipper} is created lazily via {@link #getSkipper()}.
 *
 * @lucene.experimental
 */
public abstract class DocValuesField {

  /** Sole constructor. */
  protected DocValuesField() {}

  /** Returns the global minimum value across all documents with this field in this segment. */
  public abstract long minValue();

  /** Returns the global maximum value across all documents with this field in this segment. */
  public abstract long maxValue();

  /** Returns the number of documents that have a value for this field in this segment. */
  public abstract int docCount();

  /**
   * Returns the maximum number of values any single document has for this field. Returns -1 if
   * unknown. Returns 0 if docCount is 0. A field is single-valued if this returns 1.
   */
  public int maxValueCount() {
    return docCount() == 0 ? 0 : -1;
  }

  /**
   * Returns the set of optional per-block statistics available from this field's skipper. This can
   * be checked without creating the skipper (no IO beyond what was done at segment open).
   */
  public Set<SkipStat<?>> availableBlockStats() {
    return Set.of();
  }

  /**
   * Returns a {@link DocValuesSkipper} for navigating per-block intervals. This may perform IO on
   * first call. The returned instance should be confined to the thread that created it.
   */
  public abstract DocValuesSkipper getSkipper() throws IOException;
}
