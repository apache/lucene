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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.FilterWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

/** A {@link Query} wrapper that disables bulk-scoring optimizations. */
public class DisablingBulkScorerQuery extends Query {

  private final Query query;

  /** Sole constructor. */
  public DisablingBulkScorerQuery(Query query) {
    this.query = query;
  }

  @Override
  public Query rewrite(IndexSearcher indexSearcher) throws IOException {
    Query rewritten = query.rewrite(indexSearcher);
    if (query != rewritten) {
      return new DisablingBulkScorerQuery(rewritten);
    }
    return super.rewrite(indexSearcher);
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    Weight in = query.createWeight(searcher, scoreMode, boost);
    return new FilterWeight(in) {
      @Override
      public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
        Scorer scorer = scorer(context);
        if (scorer == null) {
          return null;
        }
        return new DefaultBulkScorer(scorer);
      }
    };
  }

  @Override
  public String toString(String field) {
    return query.toString(field);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    query.visit(visitor);
  }

  @Override
  public boolean equals(Object obj) {
    return sameClassAs(obj) && query.equals(((DisablingBulkScorerQuery) obj).query);
  }

  @Override
  public int hashCode() {
    return 31 * classHash() + query.hashCode();
  }
}
