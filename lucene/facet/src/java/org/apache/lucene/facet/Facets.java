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
package org.apache.lucene.facet;

import java.io.IOException;
import java.util.List;

/**
 * Common base class for all facets implementations.
 *
 * @lucene.experimental
 */
public abstract class Facets {

  /** Default constructor. */
  public Facets() {}

  /**
   * Returns all child labels with non-zero counts under the specified path. Users should make no
   * assumptions about ordering of the children. Returns null if the specified path doesn't exist or
   * if this dimension was never seen.
   */
  public abstract FacetResult getAllChildren(String dim, String... path) throws IOException;

  /**
   * Returns the topN child labels under the specified path. Returns null if the specified path
   * doesn't exist or if this dimension was never seen.
   */
  public abstract FacetResult getTopChildren(int topN, String dim, String... path)
      throws IOException;

  /**
   * Return the count or value for a specific path. Returns -1 if this path doesn't exist, else the
   * count.
   */
  public abstract Number getSpecificValue(String dim, String... path) throws IOException;

  /**
   * Returns topN labels for any dimension that had hits, sorted by the number of hits that
   * dimension matched; this is used for "sparse" faceting, where many different dimensions were
   * indexed, for example depending on the type of document.
   */
  public abstract List<FacetResult> getAllDims(int topN) throws IOException;

  /**
   * Returns labels for topN dimensions and their topNChildren sorted by the number of
   * hits/aggregated values that dimension matched. Results should be the same as calling getAllDims
   * and then only using the first topNDims. Note that dims should be configured as requiring dim
   * counts if using this functionality to ensure accurate counts are available (see: {@link
   * FacetsConfig#setRequireDimCount(String, boolean)}).
   *
   * <p>Sub-classes may want to override this implementation with a more efficient one if they are
   * able.
   */
  public List<FacetResult> getTopDims(int topNDims, int topNChildren) throws IOException {
    List<FacetResult> allResults = getAllDims(topNChildren);
    return allResults.subList(0, Math.min(topNDims, allResults.size()));
  }

  /**
   * This helper method checks if topN is valid for getTopChildren and getAllDims. Throws
   * IllegalArgumentException if topN is invalid.
   *
   * @lucene.experimental it may not exist in future versions of Lucene
   */
  protected static void validateTopN(int topN) {
    if (topN <= 0) {
      throw new IllegalArgumentException("topN must be > 0 (got: " + topN + ")");
    }
  }
}
