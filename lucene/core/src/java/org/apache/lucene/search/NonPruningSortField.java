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

import java.io.IOException;
import java.util.Objects;
import org.apache.lucene.index.IndexSorter;

/**
 * A {@link SortField} that wraps another one and disables document skipping based on indexed data
 * by always requesting a comparator with {@link Pruning#NONE}.
 *
 * <p>Create instances with {@link SortField#withoutPruning(SortField)}.
 *
 * <p>This is a search-time wrapper only: it cannot be used as an index sort, and {@link
 * #getIndexSorter()} throws {@link UnsupportedOperationException}. Index-sort early termination
 * still applies when searching with it against an index sorted by the wrapped field.
 *
 * @lucene.experimental
 */
public final class NonPruningSortField extends SortField {

  private final SortField delegate;

  NonPruningSortField(SortField delegate) {
    super(
        delegate.getField(),
        delegate.getType(),
        delegate.getReverse(),
        delegate.getMissingValue(),
        delegate.getComparatorSource());
    this.delegate = Objects.requireNonNull(delegate);
  }

  /** Returns the wrapped SortField */
  public SortField getDelegate() {
    return delegate;
  }

  @Override
  public FieldComparator<?> getComparator(int numHits, Pruning pruning) {
    return delegate.getComparator(numHits, Pruning.NONE);
  }

  @Override
  public SortField rewrite(IndexSearcher searcher) throws IOException {
    SortField rewritten = delegate.rewrite(searcher);
    return rewritten == delegate ? this : SortField.withoutPruning(rewritten);
  }

  @Override
  public boolean needsScores() {
    return delegate.needsScores();
  }

  /**
   * Not supported: this SortField is for searching only. Index with the wrapped {@link
   * #getDelegate() delegate} instead.
   *
   * @throws UnsupportedOperationException always
   */
  @Override
  public IndexSorter getIndexSorter() {
    throw new UnsupportedOperationException(
        "withoutPruning sort fields can only be used at search time, not as an index sort");
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    return o instanceof NonPruningSortField other && delegate.equals(other.delegate);
  }

  @Override
  public int hashCode() {
    return 31 * delegate.hashCode() + 1;
  }

  @Override
  public String toString() {
    return delegate + "(no pruning)";
  }
}
