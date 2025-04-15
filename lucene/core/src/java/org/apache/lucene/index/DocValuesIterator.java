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
import org.apache.lucene.search.DocIdSetIterator;

abstract class DocValuesIterator extends DocIdSetIterator {

  /** Return an iterator for these doc values. */
  public abstract DocIdSetIterator iterator();

  /**
   * Advance the iterator to exactly {@code target} and return whether {@code target} has a value.
   * {@code target} must be greater than or equal to the current {@link #docID() doc ID} and must be
   * a valid doc ID, ie. &ge; 0 and &lt; {@code maxDoc}. After this method returns, calling {@link
   * #docID()} on the {@link #iterator()} returns {@code target}.
   */
  public abstract boolean advanceExact(int target) throws IOException;

  /**
   * Advance to the next document and return it.
   *
   * @see #iterator()
   * @deprecated Call iterator().nextDoc() instead.
   */
  @Deprecated
  @Override
  public int nextDoc() throws IOException {
    return iterator().nextDoc();
  }

  /**
   * Advance to the next document on or after {@code target}.
   *
   * @see #iterator()
   * @deprecated Call iterator().advance(target) instead.
   */
  @Deprecated
  @Override
  public int advance(int target) throws IOException {
    return iterator().advance(target);
  }

  /**
   * Return the current doc ID.
   *
   * @see #iterator()
   * @deprecated Call iterator().docID() instead.
   */
  @Deprecated
  @Override
  public int docID() {
    return iterator().docID();
  }

  /**
   * Return the cost of this iterator.
   *
   * @see #iterator()
   * @deprecated Call iterator().cost() instead.
   */
  @Deprecated
  @Override
  public long cost() {
    return iterator().cost();
  }
}
