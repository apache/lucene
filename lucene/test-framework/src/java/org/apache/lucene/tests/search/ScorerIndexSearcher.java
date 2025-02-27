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
package org.apache.lucene.tests.search;

import java.io.IOException;
import java.util.concurrent.Executor;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FilterWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;

/** An {@link IndexSearcher} that always uses the {@link Scorer} API, never {@link BulkScorer}. */
public class ScorerIndexSearcher extends IndexSearcher {

  /**
   * Creates a searcher searching the provided index. Search on individual segments will be run in
   * the provided {@link Executor}.
   *
   * @see IndexSearcher#IndexSearcher(IndexReader, Executor)
   */
  public ScorerIndexSearcher(IndexReader r, Executor executor) {
    super(r, executor);
  }

  /**
   * Creates a searcher searching the provided index.
   *
   * @see IndexSearcher#IndexSearcher(IndexReader)
   */
  public ScorerIndexSearcher(IndexReader r) {
    super(r);
  }

  @Override
  protected void searchLeaf(
      LeafReaderContext ctx, int minDocId, int maxDocId, Weight weight, Collector collector)
      throws IOException {
    Weight filterWeight =
        new FilterWeight(weight) {
          @Override
          public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
            ScorerSupplier in = super.scorerSupplier(context);
            if (in == null) {
              return null;
            }
            return new ScorerSupplier() {

              @Override
              public Scorer get(long leadCost) throws IOException {
                return in.get(leadCost);
              }

              @Override
              public BulkScorer bulkScorer() throws IOException {
                // Don't delegate to `in` to make sure we get a DefaultBulkScorer
                return super.bulkScorer();
              }

              @Override
              public long cost() {
                return in.cost();
              }
            };
          }
        };

    super.searchLeaf(ctx, minDocId, maxDocId, filterWeight, collector);
  }
}
