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
import java.util.Objects;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestQueryRewriteBackwardsCompatibility extends LuceneTestCase {

  public void testQueryRewriteNoOverrides() throws IOException {
    Directory directory = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), directory, newIndexWriterConfig());
    IndexReader reader = w.getReader();
    w.close();
    IndexSearcher searcher = newSearcher(reader);
    Query queryNoOverrides = new TestQueryNoOverrides();
    assertSame(queryNoOverrides, searcher.rewrite(queryNoOverrides));
    assertSame(queryNoOverrides, queryNoOverrides.rewrite(searcher));
    assertSame(queryNoOverrides, queryNoOverrides.rewrite(reader));
    reader.close();
    directory.close();
  }

  public void testSingleQueryRewrite() throws IOException {
    Directory directory = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), directory, newIndexWriterConfig());
    IndexReader reader = w.getReader();
    w.close();
    IndexSearcher searcher = newSearcher(reader);

    RewriteCountingQuery oldQuery = new OldQuery(null);
    RewriteCountingQuery newQuery = new NewQuery(null);

    oldQuery.rewrite(searcher);
    oldQuery.rewrite(reader);

    newQuery.rewrite(searcher);
    newQuery.rewrite(reader);

    assertEquals(2, oldQuery.rewriteCount);
    assertEquals(2, newQuery.rewriteCount);
    reader.close();
    directory.close();
  }

  public void testNestedQueryRewrite() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, newIndexWriterConfig());
    IndexReader reader = w.getReader();
    w.close();
    IndexSearcher searcher = newSearcher(reader);

    RewriteCountingQuery query = random().nextBoolean() ? new NewQuery(null) : new OldQuery(null);

    for (int i = 0; i < 5 + random().nextInt(5); i++) {
      query = random().nextBoolean() ? new NewQuery(query) : new OldQuery(query);
    }

    query.rewrite(searcher);
    query.rewrite(reader);

    RewriteCountingQuery innerQuery = query;
    while (innerQuery != null) {
      assertEquals(2, innerQuery.rewriteCount);
      innerQuery = innerQuery.getInnerQuery();
    }
    reader.close();
    dir.close();
  }

  public void testRewriteQueryInheritance() throws IOException {
    Directory directory = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), directory, newIndexWriterConfig());
    IndexReader reader = w.getReader();
    w.close();
    IndexSearcher searcher = newSearcher(reader);
    NewRewritableCallingSuper oneRewrite = new NewRewritableCallingSuper();
    NewRewritableCallingSuper twoRewrites = new OldNewRewritableCallingSuper();
    NewRewritableCallingSuper threeRewrites = new OldOldNewRewritableCallingSuper();

    searcher.rewrite(oneRewrite);
    searcher.rewrite(twoRewrites);
    searcher.rewrite(threeRewrites);
    assertEquals(1, oneRewrite.rewriteCount);
    assertEquals(2, twoRewrites.rewriteCount);
    assertEquals(3, threeRewrites.rewriteCount);

    reader.close();
    directory.close();
  }

  private static class NewRewritableCallingSuper extends RewriteCountingQuery {

    @Override
    public Query rewrite(IndexSearcher searcher) throws IOException {
      rewriteCount++;
      return super.rewrite(searcher);
    }

    @Override
    public String toString(String field) {
      return "NewRewritableCallingSuper";
    }

    @Override
    public void visit(QueryVisitor visitor) {}

    @Override
    public boolean equals(Object obj) {
      return obj instanceof NewRewritableCallingSuper;
    }

    @Override
    public int hashCode() {
      return 1;
    }

    @Override
    RewriteCountingQuery getInnerQuery() {
      return null;
    }
  }

  private static class OldNewRewritableCallingSuper extends NewRewritableCallingSuper {
    @Override
    public Query rewrite(IndexReader reader) throws IOException {
      rewriteCount++;
      return super.rewrite(reader);
    }
  }

  private static class OldOldNewRewritableCallingSuper extends OldNewRewritableCallingSuper {
    @Override
    public Query rewrite(IndexReader reader) throws IOException {
      rewriteCount++;
      return super.rewrite(reader);
    }
  }

  private abstract static class RewriteCountingQuery extends Query {
    int rewriteCount = 0;

    abstract RewriteCountingQuery getInnerQuery();
  }

  private static class OldQuery extends RewriteCountingQuery {
    private final RewriteCountingQuery optionalSubQuery;

    private OldQuery(RewriteCountingQuery optionalSubQuery) {
      this.optionalSubQuery = optionalSubQuery;
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
      if (this.optionalSubQuery != null) {
        this.optionalSubQuery.rewrite(reader);
      }
      rewriteCount++;
      return this;
    }

    @Override
    public String toString(String field) {
      return "OldQuery";
    }

    @Override
    public void visit(QueryVisitor visitor) {}

    @Override
    public boolean equals(Object obj) {
      return obj instanceof OldQuery
          && Objects.equals(((OldQuery) obj).optionalSubQuery, optionalSubQuery);
    }

    @Override
    public int hashCode() {
      return 42 ^ Objects.hash(optionalSubQuery);
    }

    @Override
    RewriteCountingQuery getInnerQuery() {
      return optionalSubQuery;
    }
  }

  private static class NewQuery extends RewriteCountingQuery {
    private final RewriteCountingQuery optionalSubQuery;

    private NewQuery(RewriteCountingQuery optionalSubQuery) {
      this.optionalSubQuery = optionalSubQuery;
    }

    @Override
    public Query rewrite(IndexSearcher searcher) throws IOException {
      if (this.optionalSubQuery != null) {
        this.optionalSubQuery.rewrite(searcher);
      }
      rewriteCount++;
      return this;
    }

    @Override
    public String toString(String field) {
      return "NewQuery";
    }

    @Override
    public void visit(QueryVisitor visitor) {}

    @Override
    public boolean equals(Object obj) {
      return obj instanceof NewQuery
          && Objects.equals(((NewQuery) obj).optionalSubQuery, optionalSubQuery);
    }

    @Override
    public int hashCode() {
      return 73 ^ Objects.hash(optionalSubQuery);
    }

    @Override
    RewriteCountingQuery getInnerQuery() {
      return optionalSubQuery;
    }
  }

  private static class TestQueryNoOverrides extends Query {

    private final int randomHash = random().nextInt();

    @Override
    public String toString(String field) {
      return "TestQueryNoOverrides";
    }

    @Override
    public void visit(QueryVisitor visitor) {}

    @Override
    public boolean equals(Object obj) {
      return obj instanceof TestQueryNoOverrides
          && randomHash == ((TestQueryNoOverrides) obj).randomHash;
    }

    @Override
    public int hashCode() {
      return randomHash;
    }
  }
}
