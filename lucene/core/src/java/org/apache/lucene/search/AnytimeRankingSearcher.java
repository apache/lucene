/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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
import org.apache.lucene.index.BinScoreUtil;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.HeuristicSLAEstimator;
import org.apache.lucene.util.SLAEstimator;

/** SLA-aware searcher that applies bin-aware boosting and early termination. */
public final class AnytimeRankingSearcher implements AutoCloseable {

  private final IndexSearcher searcher;
  private final int topK;
  private final long baseSlaMs;
  private final SLAEstimator estimator;
  private final IndexReader reader;

  /** Constructs a searcher with SLA cutoff and bin boosting. */
  public AnytimeRankingSearcher(IndexReader reader, int topK, long baseSlaMs, String field)
      throws IOException {
    this.reader = BinScoreUtil.wrap(reader);
    this.searcher = new IndexSearcher(this.reader);
    this.topK = topK;
    this.baseSlaMs = baseSlaMs;
    this.estimator = new HeuristicSLAEstimator(field);
  }

  /** Executes a search with bin-aware scoring and SLA cutoff. */
  public TopDocs search(Query query) throws IOException {
    long estimated = (long) estimator.estimate(query, searcher.getIndexReader(), baseSlaMs);
    AnytimeRankingCollectorManager manager =
        new AnytimeRankingCollectorManager(topK, estimated, searcher.getIndexReader());
    return searcher.search(query, manager);
  }

  /** Returns the wrapped IndexSearcher. */
  public IndexSearcher getSearcher() {
    return searcher;
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }
}
