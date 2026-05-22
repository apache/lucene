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
package org.apache.lucene.document.column;

/**
 * A dense values cursor over a {@link DictionaryColumn}. Produces exactly {@link #size()} ordinals
 * for consecutive batch-local doc-ids starting at 0, one per call to {@link #nextOrd()}.
 *
 * <p>Each ordinal must be in {@code [0, column.dictionary().length)}.
 *
 * <p>Implementations must throw an exception if {@link #nextOrd()} is called more than {@link
 * #size()} times.
 *
 * @lucene.experimental
 */
public abstract class OrdinalsCursor {

  private final int size;

  /**
   * Creates a cursor that will produce exactly {@code size} ordinals, one per batch-local doc-id in
   * {@code [0, size)}.
   */
  protected OrdinalsCursor(int size) {
    this.size = size;
  }

  /** Total number of ordinals this cursor will produce. */
  public final int size() {
    return size;
  }

  /**
   * Returns the next ordinal. Must not be called more than {@link #size()} times.
   *
   * <p>The returned value must be in {@code [0, dictionary.length)} where {@code dictionary} is the
   * enclosing column's dictionary. The indexing path validates this on every call and throws {@link
   * IllegalArgumentException} on violation.
   */
  public abstract int nextOrd();
}
