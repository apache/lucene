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
import org.apache.lucene.index.LeafReaderContext;

/**
 * Contract for filter {@link Query} types that may expose a contiguous doc id interval on a leaf
 * when the index's primary sort order aligns with the filter's field. Used to narrow bulk scoring
 * for boolean queries with a single such {@link BooleanClause.Occur#FILTER} clause.
 *
 * <p>The caller ({@link FilteredOnPrimaryIndexSortFieldQuery}) only guarantees that {@link
 * #denseDocIdRangeOrNull} is invoked on leaves with no deletions. Whether this particular leaf's
 * primary sort matches {@link #getField()} (and is the expected sort-field type) is the
 * implementation's responsibility — segments produced by older writers or partial merges may not
 * carry the primary sort even when other segments in the same reader do.
 *
 * <p>Implementations must therefore verify query-type-specific structural requirements (e.g.
 * primary sort matches and is the expected type, field is single-valued, the matched range is
 * actually dense) and return {@code null} when those cannot be proved. Returning {@code null} is
 * always safe; it causes execution to fall back to the unoptimized boolean {@link Weight}.
 *
 * <p>Implementations include {@link IndexSortSortedNumericDocValuesRangeQuery}, {@link TermQuery},
 * {@link PointRangeQuery} (1D int/long ranges), and package-private sorted doc-value range queries
 * in {@code org.apache.lucene.document}.
 *
 * @lucene.experimental
 */
public interface PrimarySortAlignable {

  /** Field constrained by this query. */
  String getField();

  /**
   * Matching docs as {@code [minDoc, maxDoc)} on this leaf, or {@code null} if a safe dense range
   * cannot be proved. The caller has already filtered out leaves with deletions; implementations
   * still verify per-leaf primary sort and field-specific density themselves.
   */
  DocIdRange denseDocIdRangeOrNull(LeafReaderContext context) throws IOException;
}
