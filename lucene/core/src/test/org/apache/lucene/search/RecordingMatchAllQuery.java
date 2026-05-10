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
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.Bits;

/**
 * A MUST clause that matches every document but is not rewritten away like {@link
 * MatchAllDocsQuery}; used so {@link BooleanQuery} can rewrite to {@link
 * FilteredOnPrimaryIndexSortFieldQuery}. Records each [{@code min}, {@code max}) window passed to
 * {@link BulkScorer#score}.
 */
final class RecordingMatchAllQuery extends Query {

  /** Bulk-scorer intervals in doc-id space, in invocation order. */
  final List<DocIdRange> scoredRanges = new ArrayList<>();

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
    return new ConstantScoreWeight(this, boost) {
      @Override
      public ScorerSupplier scorerSupplier(LeafReaderContext context) {
        final int maxDoc = context.reader().maxDoc();
        final float queryScore = score();
        return new ScorerSupplier() {
          @Override
          public Scorer get(long leadCost) throws IOException {
            return new ConstantScoreScorer(queryScore, scoreMode, DocIdSetIterator.all(maxDoc));
          }

          @Override
          public BulkScorer bulkScorer() {
            return new BulkScorer() {
              @Override
              public int score(LeafCollector collector, Bits acceptDocs, int min, int max)
                  throws IOException {
                scoredRanges.add(new DocIdRange(min, max));
                SimpleScorable scorable = new SimpleScorable();
                scorable.setScore(queryScore);
                collector.setScorer(scorable);
                for (int doc = min; doc < max && doc < maxDoc; ++doc) {
                  if (acceptDocs == null || acceptDocs.get(doc)) {
                    collector.collect(doc);
                  }
                }
                return max == maxDoc ? DocIdSetIterator.NO_MORE_DOCS : max;
              }

              @Override
              public long cost() {
                return maxDoc;
              }
            };
          }

          @Override
          public long cost() {
            return maxDoc;
          }
        };
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return true;
      }
    };
  }

  /** Clears {@link #scoredRanges} for reuse across searches. */
  void clearRecordedRanges() {
    scoredRanges.clear();
  }

  @Override
  public void visit(QueryVisitor visitor) {
    visitor.visitLeaf(this);
  }

  @Override
  public String toString(String field) {
    return "RecordingMatchAllQuery";
  }

  @Override
  public boolean equals(Object obj) {
    return this == obj;
  }

  @Override
  public int hashCode() {
    return System.identityHashCode(this);
  }
}
