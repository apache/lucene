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

/** A query that matches no documents. */
public class MatchNoDocsQuery extends Query {

  private final String reason;

  /**
   * A singleton instance with a blank reason
   *
   * @see #MatchNoDocsQuery(String)
   */
  public static final MatchNoDocsQuery INSTANCE = new MatchNoDocsQuery();

  /**
   * Default constructor
   *
   * @deprecated Use either {@link MatchAllDocsQuery#INSTANCE} or {@link #MatchNoDocsQuery(String)}
   */
  @Deprecated
  public MatchNoDocsQuery() {
    this("");
  }

  /**
   * Provides a reason explaining why this query was used.
   *
   * <p>NOTE: All instances of this class are equal, even if they were constructed with distinct
   * reasons.
   */
  public MatchNoDocsQuery(String reason) {
    this.reason = reason;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    return new Weight(this) {
      @Override
      public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        return Explanation.noMatch(reason);
      }

      @Override
      public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        return null;
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return true;
      }

      @Override
      public int count(LeafReaderContext context) {
        return 0;
      }
    };
  }

  @Override
  public void visit(QueryVisitor visitor) {
    visitor.visitLeaf(this);
  }

  @Override
  public String toString(String field) {
    return "MatchNoDocsQuery(\"" + reason + "\")";
  }

  @Override
  public boolean equals(Object o) {
    return sameClassAs(o);
  }

  @Override
  public int hashCode() {
    return classHash();
  }
}
