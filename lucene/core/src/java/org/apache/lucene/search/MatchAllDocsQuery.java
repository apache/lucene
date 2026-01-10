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
import org.apache.lucene.index.LeafReaderContext;

/** A query that matches all documents. */
public final class MatchAllDocsQuery extends Query {

  /** A singleton instance */
  public static final MatchAllDocsQuery INSTANCE = new MatchAllDocsQuery();

  /**
   * Default constructor
   *
   * @deprecated Use {@link MatchAllDocsQuery#INSTANCE}
   */
  @Deprecated
  public MatchAllDocsQuery() {
    super();
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
    return new ConstantScoreWeight(this, boost) {
      @Override
      public String toString() {
        return "weight(" + MatchAllDocsQuery.this + ")";
      }

      @Override
      public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        return ConstantScoreScorerSupplier.matchAll(score(), scoreMode, context.reader().maxDoc());
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return true;
      }

      @Override
      public int count(LeafReaderContext context) {
        return context.reader().numDocs();
      }
    };
  }

  @Override
  public String toString(String field) {
    return "*:*";
  }

  @Override
  public boolean equals(Object o) {
    return sameClassAs(o);
  }

  @Override
  public int hashCode() {
    return classHash();
  }

  @Override
  public void visit(QueryVisitor visitor) {
    visitor.visitLeaf(this);
  }
}
