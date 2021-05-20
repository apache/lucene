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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;

/**
 * A simple extension of {@link IndexSearcher} to add a {@link QueryProfiler} that can be set to
 * test query timings.
 */
public class QueryProfilerIndexSearcher extends IndexSearcher {

  protected QueryProfiler profiler;

  public QueryProfilerIndexSearcher(IndexReader reader) {
    super(reader);
  }

  public void setProfiler(QueryProfiler profiler) {
    this.profiler = profiler;
  }

  @Override
  public Query rewrite(Query original) throws IOException {
    if (profiler != null) {
      profiler.startRewriteTime();
    }

    try {
      return super.rewrite(original);
    } finally {
      if (profiler != null) {
        profiler.stopAndAddRewriteTime();
      }
    }
  }

  @Override
  public Weight createWeight(Query query, ScoreMode scoreMode, float boost) throws IOException {
    if (profiler != null) {
      // createWeight() is called for each query in the tree, so we tell the queryProfiler
      // each invocation so that it can build an internal representation of the query
      // tree
      QueryProfilerBreakdown profile = profiler.getQueryBreakdown(query);
      QueryProfilerTimer timer = profile.getTimer(QueryProfilerTimingType.CREATE_WEIGHT);
      timer.start();
      final Weight weight;
      try {
        weight = query.createWeight(this, scoreMode, boost);
      } finally {
        timer.stop();
        profiler.pollLastQuery();
      }
      return new QueryProfilerWeight(query, weight, profile);
    } else {
      return super.createWeight(query, scoreMode, boost);
    }
  }
}
