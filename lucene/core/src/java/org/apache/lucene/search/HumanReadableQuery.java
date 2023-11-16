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

/**
 * A query for debug purposes only. Takes a query and a description. Behaves like the given query,
 * but when printing to a string, it will prepend the description parameter to the given query.
 */
public final class HumanReadableQuery extends Query {

  private final Query in;
  private final String description;

  HumanReadableQuery(Query in, String description) {
    this.in = in;
    this.description = description;
  }

  public Query getIn() {
    return in;
  }

  public String getDescription() {
    return description;
  }

  @Override
  public Query rewrite(IndexSearcher indexSearcher) throws IOException {
    Query rewritten = in.rewrite(indexSearcher);
    if (rewritten == in) {
      return this;
    }
    return new HumanReadableQuery(rewritten, description);
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
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    return in.createWeight(searcher, scoreMode, boost);
  }

  @Override
  public int hashCode() {
    return in.hashCode();
  }
}
