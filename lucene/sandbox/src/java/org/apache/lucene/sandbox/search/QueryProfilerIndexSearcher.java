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

package org.apache.lucene.sandbox.search;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executor;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;

/**
 * An extension of {@link IndexSearcher} that records profile information for all queries it
 * executes.
 */
public class QueryProfilerIndexSearcher extends IndexSearcher {

  private final QueryProfilerTree profiler;

  public QueryProfilerIndexSearcher(IndexReader reader) {
    super(reader);
    profiler = new QueryProfilerTree();
  }

  /**
   * Creates a profiling searcher that offloads per-slice search to the given executor. Passing a
   * non-null executor enables concurrent (including intra-segment) search, so that {@link
   * #slices(List)} is consulted to build leaf partitions.
   */
  public QueryProfilerIndexSearcher(IndexReader reader, Executor executor) {
    super(reader, executor);
    profiler = new QueryProfilerTree();
  }

  @Override
  public Query rewrite(Query original) throws IOException {
    profiler.startRewriteTime();
    try {
      return super.rewrite(original);
    } finally {
      profiler.stopAndAddRewriteTime();
    }
  }

  @Override
  public Weight createWeight(Query query, ScoreMode scoreMode, float boost) throws IOException {
    // createWeight() is called for each query in the tree, so we tell the queryProfiler
    // each invocation so that it can build an internal representation of the query
    // tree
    QueryProfilerBreakdown profile = profiler.getProfileBreakdown(query);
    QueryProfilerTimer timer = profile.getTimer(QueryProfilerTimingType.CREATE_WEIGHT);
    timer.start();
    final Weight weight;
    try {
      weight = query.createWeight(this, scoreMode, boost);
    } finally {
      timer.stop();
      profiler.pollLast();
    }
    return new QueryProfilerWeight(weight, profile);
  }

  @Override
  protected void search(LeafReaderContextPartition[] partitions, Weight weight, Collector collector)
      throws IOException {
    // search(partitions[]) is invoked once per slice (one executor task). Tag every partition
    // searched within it — via the nested searchLeaf calls below — with a unique slice id, so the
    // profiler can group per-partition timings by the slice that produced them.
    profiler.enterSlice();
    try {
      super.search(partitions, weight, collector);
    } finally {
      profiler.exitSlice();
    }
  }

  @Override
  protected void searchLeaf(
      LeafReaderContext ctx, int minDocId, int maxDocId, Weight weight, Collector collector)
      throws IOException {
    // searchLeaf() is invoked once per leaf partition and is the only place the partition's doc-id
    // bounds are in scope. Record the partition identity for the duration of the call so that any
    // leaf-level timings (build scorer, next doc, score, ...) recorded by the wrapped weight/scorer
    // are attributed to this specific partition rather than only to the executing thread.
    profiler.setCurrentPartition(ctx.ord, minDocId, maxDocId);
    try {
      super.searchLeaf(ctx, minDocId, maxDocId, weight, collector);
    } finally {
      profiler.clearCurrentPartition();
    }
  }

  /**
   * @return total time taken to rewrite all queries in this profile
   */
  public long getRewriteTime() {
    return profiler.getRewriteTime();
  }

  /**
   * @return a hierarchical representation of the profiled tree
   */
  public List<QueryProfilerResult> getProfileResult() {
    return profiler.getTree();
  }
}
