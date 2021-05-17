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

package org.apache.lucene.sandbox.queries.profile;

import java.util.Objects;
import org.apache.lucene.search.Query;

/**
 * This class acts as storage for profiling a query. It also builds a representation of the query
 * tree which is built constructed "online" as the weights are wrapped by {@link
 * ProfileIndexSearcher}. This allows us to know the relationship between nodes in tree without
 * explicitly walking the tree or pre-wrapping everything.
 */
public final class QueryProfiler extends AbstractProfiler<QueryProfileBreakdown, Query> {

  /** The root Collector used in the search */
  private InternalProfileCollector collector;

  public QueryProfiler() {
    super(new InternalQueryProfileTree());
  }

  /** Set the collector that is associated with this profiler. */
  public void setCollector(InternalProfileCollector collector) {
    if (this.collector != null) {
      throw new IllegalStateException("The collector can only be set once.");
    }
    this.collector = Objects.requireNonNull(collector);
  }

  /**
   * Begin timing the rewrite phase of a request. All rewrites are accumulated together into a
   * single metric
   */
  public void startRewriteTime() {
    ((InternalQueryProfileTree) profileTree).startRewriteTime();
  }

  /**
   * Stop recording the current rewrite and add it's time to the total tally, returning the
   * cumulative time so far.
   *
   * @return cumulative rewrite time
   */
  public long stopAndAddRewriteTime() {
    return ((InternalQueryProfileTree) profileTree).stopAndAddRewriteTime();
  }

  /** @return total time taken to rewrite all queries in this profile */
  public long getRewriteTime() {
    return ((InternalQueryProfileTree) profileTree).getRewriteTime();
  }

  /** Return the current root Collector for this search */
  public CollectorResult getCollector() {
    return collector.getCollectorTree();
  }
}
