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
 * <p>Implementations include {@link IndexSortSortedNumericDocValuesRangeQuery}. {@link TermQuery}
 * filters are adapted internally (see {@link PrimarySortAlignables#asAlignableOrNull(Query)}).
 *
 * @lucene.experimental
 */
public interface PrimarySortAlignable {

  /** Field constrained by this query. */
  String getField();

  /** Whether this filter may participate in the optimization on the given index. */
  boolean canOptimize(IndexSearcher searcher) throws IOException;

  /**
   * Whether a dense doc id interval can be derived on this leaf from primary sort layout.
   *
   * <p>Default: {@code denseDocIdRangeOrNull(context) != null}.
   */
  default boolean supportsDenseLeafRange(LeafReaderContext context) throws IOException {
    return denseDocIdRangeOrNull(context) != null;
  }

  /**
   * Matching docs as {@code [minDoc, maxDoc)} on this leaf, or {@code null} if unknown / not dense.
   */
  DocIdRange denseDocIdRangeOrNull(LeafReaderContext context) throws IOException;
}
