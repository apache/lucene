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

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;

/**
 * A simple query wrapper for debug purposes. Behaves like the given query, but when printing to a
 * string, it will prepend the description parameter to the query output.
 */
public final class HumanReadableQuery extends Query {

  private final Query in;
  private final String description;

  /**
   * Create a new HumanReadableQuery
   *
   * @param in the query to wrap
   * @param description a human-readable description, used in toString()
   */
  public HumanReadableQuery(Query in, String description) {
    this.in = in;
    this.description = description;
  }

  /**
   * @return the wrapped Query
   */
  public Query getWrappedQuery() {
    return in;
  }

  /**
   * @return the query description
   */
  public String getDescription() {
    return description;
  }

  @Override
  public Query rewrite(IndexSearcher indexSearcher) {
    return in;
  }

  @Override
  public String toString(String field) {
    return this.getDescription() + ":" + in.toString(field);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    in.visit(visitor);
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) && in.equals(((HumanReadableQuery) other).in);
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
    throw new UnsupportedOperationException("HumanReadableQuery does not support #createWeight()");
  }

  @Override
  public int hashCode() {
    return 31 * classHash() + in.hashCode();
  }
}
