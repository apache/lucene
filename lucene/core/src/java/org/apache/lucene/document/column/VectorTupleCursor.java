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

import org.apache.lucene.search.DocIdSetIterator;

/**
 * A tuple cursor over a {@link VectorColumn}. Yields {@code (docID, vectorValue)} pairs.
 * Batch-local doc-ids are returned in strictly increasing order (vectors are single-valued).
 *
 * @param <T> the vector array type, either {@code float[]} or {@code byte[]}
 * @lucene.experimental
 */
public abstract class VectorTupleCursor<T> {

  /** Sole constructor. */
  protected VectorTupleCursor() {}

  /**
   * Advances to the next doc-id that has a vector and returns it, or {@link
   * DocIdSetIterator#NO_MORE_DOCS} if exhausted. Doc-ids are batch-local (0 to {@code numDocs - 1})
   * and strictly increasing.
   */
  public abstract int nextDoc();

  /**
   * Returns the vector at the current cursor position. The returned array may be reused by the
   * cursor on subsequent calls to {@link #nextDoc()} — the indexing chain copies the value before
   * advancing. Only valid after a {@code nextDoc()} that returned a value other than {@link
   * DocIdSetIterator#NO_MORE_DOCS}.
   */
  public abstract T vectorValue();
}
