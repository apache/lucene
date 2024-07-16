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
package org.apache.lucene.sandbox.facet.abstracts;

import java.io.IOException;
import org.apache.lucene.index.LeafReaderContext;

/**
 * Record data for each facet of each doc. TODO: do we need FacetRecorderManager similar to
 * CollectorManager, e.g. is getLeafRecorder always thread safe? If we have a Manager-level
 * recorder, then collection within a slice can be done to a single non-sync map, which must be
 * faster. In our case, we didn't seem to benefit from it as we have more CPUs than segments in the
 * index. Also, slice recorder makes it harder to implement lazy recorder init, as we need to init
 * both slice and leaf recorders lazily.
 */
public interface FacetRecorder {
  /** Get leaf recorder. */
  FacetLeafRecorder getLeafRecorder(LeafReaderContext context) throws IOException;

  /** Return next collected ordinal, or {@link LeafFacetCutter#NO_MORE_ORDS} */
  OrdinalIterator recordedOrds();

  /** True if there are no records */
  boolean isEmpty();

  /**
   * Reduce leaf recorder results into this recorder. If facetRollup is not null, it also rolls up
   * values.
   *
   * @throws UnsupportedOperationException if facetRollup is not null and {@link
   *     FacetRollup#getDimOrdsToRollup()} returns at least one dimension ord, but this type of
   *     record can't be rolled up.
   */
  void reduce(FacetRollup facetRollup) throws IOException;
}
