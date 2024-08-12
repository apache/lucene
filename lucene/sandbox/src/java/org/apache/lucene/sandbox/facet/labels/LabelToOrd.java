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
package org.apache.lucene.sandbox.facet.labels;

import java.io.IOException;
import org.apache.lucene.facet.taxonomy.FacetLabel;

/**
 * Label to ord mapping interface.
 *
 * <p>TODO: move FacetLabel out of taxonomy folder to use it for any facets, not just taxonomy?
 *
 * <p>TODO: there is some overlap with {@link
 * org.apache.lucene.facet.taxonomy.writercache.LabelToOrdinal}, can we reuse something?
 *
 * @lucene.experimental
 */
public interface LabelToOrd {

  /**
   * Ordinal to return if facet label doesn't exist in {@link #getOrd(FacetLabel)} and {@link
   * #getOrds(FacetLabel[])}
   */
  int INVALID_ORD = -1;

  /** get ord for one label */
  int getOrd(FacetLabel label) throws IOException;

  /** get ords for multiple labels */
  int[] getOrds(FacetLabel[] labels) throws IOException;
}
