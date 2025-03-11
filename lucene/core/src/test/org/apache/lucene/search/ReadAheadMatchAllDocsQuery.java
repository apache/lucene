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
import java.util.Collections;
import java.util.List;
import org.apache.lucene.index.LeafReaderContext;

/**
 * Query that matches all docs by returning a DenseConjunctionBulkScorer over a single clause. This
 * helps make sure that the competitive iterators produced by TopFieldCollector are compatible with
 * the read-ahead of this bulk scorer.
 */
public final class ReadAheadMatchAllDocsQuery extends Query {

  /** Sole constructor */
  public ReadAheadMatchAllDocsQuery() {}

  @Override
  public String toString(String field) {
    return "ReadAheadMatchAllDocsQuery";
  }

  @Override
  public void visit(QueryVisitor visitor) {
    // no-op
  }

  @Override
  public boolean equals(Object obj) {
    return sameClassAs(obj);
  }

  @Override
  public int hashCode() {
    return classHash();
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    return new ConstantScoreWeight(this, boost) {

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return true;
      }

      @Override
      public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        // Don't use ConstantScoreScoreSupplier, which only uses DenseConjunctionBulkScorer on
        // larg-ish segments.
        return new ScorerSupplier() {

          @Override
          public Scorer get(long leadCost) throws IOException {
            return new ConstantScoreScorer(
                score(), scoreMode, DocIdSetIterator.all(context.reader().maxDoc()));
          }

          @Override
          public BulkScorer bulkScorer() throws IOException {
            List<DocIdSetIterator> clauses =
                Collections.singletonList(DocIdSetIterator.all(context.reader().maxDoc()));
            return new DenseConjunctionBulkScorer(clauses, context.reader().maxDoc(), score());
          }

          @Override
          public long cost() {
            return context.reader().maxDoc();
          }
        };
      }
    };
  }
}
