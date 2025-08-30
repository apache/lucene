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
package org.apache.lucene.misc.search;

import java.io.IOException;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;

/** Query wrapper that prioritizes document ranges using impact information. */
public class ImpactRangeQuery extends Query {

  private final Query query;
  private final int rangeSize;
  private final int minDoc;
  private final int maxDoc;

  /** Create a new ImpactRangeQuery. */
  public ImpactRangeQuery(Query query, int rangeSize) {
    this.query = query;
    this.rangeSize = rangeSize;
    this.minDoc = 0;
    this.maxDoc = Integer.MAX_VALUE;
  }

  /** Create a new ImpactRangeQuery with document range restriction. */
  public ImpactRangeQuery(Query query, int minDoc, int maxDoc) {
    this.query = query;
    this.rangeSize = Math.max(1, maxDoc - minDoc);
    this.minDoc = minDoc;
    this.maxDoc = maxDoc;
  }

  @Override
  public Query rewrite(IndexSearcher indexSearcher) throws IOException {
    Query rewritten = query.rewrite(indexSearcher);
    if (rewritten != query) {
      if (minDoc != 0 || maxDoc != Integer.MAX_VALUE) {
        return new ImpactRangeQuery(rewritten, minDoc, maxDoc);
      } else {
        return new ImpactRangeQuery(rewritten, rangeSize);
      }
    }
    return this;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    Weight innerWeight = query.createWeight(searcher, scoreMode, boost);
    return new ImpactRangeWeight(
        this, innerWeight, rangeSize, minDoc, maxDoc, searcher, scoreMode, boost);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    query.visit(visitor.getSubVisitor(BooleanClause.Occur.MUST, query));
  }

  @Override
  public String toString(String field) {
    return "ImpactRange(" + query.toString(field) + ", rangeSize=" + rangeSize + ")";
  }

  @Override
  public boolean equals(Object obj) {
    if (sameClassAs(obj) == false) {
      return false;
    }
    ImpactRangeQuery other = (ImpactRangeQuery) obj;
    return query.equals(other.query)
        && rangeSize == other.rangeSize
        && minDoc == other.minDoc
        && maxDoc == other.maxDoc;
  }

  @Override
  public int hashCode() {
    int result = classHash();
    result = 31 * result + query.hashCode();
    result = 31 * result + rangeSize;
    result = 31 * result + minDoc;
    result = 31 * result + maxDoc;
    return result;
  }

  public Query getQuery() {
    return query;
  }

  public int getRangeSize() {
    return rangeSize;
  }
}
