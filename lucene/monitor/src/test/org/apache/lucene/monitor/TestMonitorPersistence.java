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

package org.apache.lucene.monitor;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Objects;
import java.util.function.Function;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.StringHelper;

public class TestMonitorPersistence extends MonitorTestBase {

  private final Path indexDirectory = createTempDir();

  protected Monitor newMonitorWithPersistence() throws IOException {
    return newMonitorWithPersistence(MonitorTestBase::parse);
  }

  protected Monitor newMonitorWithPersistence(Function<String, Query> parser) throws IOException {
    MonitorConfiguration config =
        new MonitorConfiguration()
            .setIndexPath(indexDirectory, MonitorQuerySerializer.fromParser(parser));

    return new Monitor(ANALYZER, config);
  }

  public void testCacheIsRepopulated() throws IOException {

    Document doc = new Document();
    doc.add(newTextField(FIELD, "test", Field.Store.NO));

    try (Monitor monitor = newMonitorWithPersistence()) {
      monitor.register(
          mq("1", "test"),
          mq("2", "test"),
          mq("3", "test", "language", "en"),
          mq("4", "test", "wibble", "quack"));

      assertEquals(4, monitor.match(doc, QueryMatch.SIMPLE_MATCHER).getMatchCount());

      IllegalArgumentException e =
          expectThrows(
              IllegalArgumentException.class,
              () ->
                  monitor.register(
                      new MonitorQuery(
                          "5", new MatchAllDocsQuery(), null, Collections.emptyMap())));
      assertEquals(
          "Cannot add a MonitorQuery with a null string representation to a non-ephemeral Monitor",
          e.getMessage());
    }

    try (Monitor monitor2 = newMonitorWithPersistence()) {
      assertEquals(4, monitor2.getQueryCount());
      assertEquals(4, monitor2.match(doc, QueryMatch.SIMPLE_MATCHER).getMatchCount());

      MonitorQuery mq = monitor2.getQuery("4");
      assertEquals("quack", mq.getMetadata().get("wibble"));
    }
  }

  public void testGetQueryPresent() throws IOException {
    try (Monitor monitor = newMonitorWithPersistence()) {
      MonitorQuery monitorQuery = mq("1", "test");
      monitor.register(monitorQuery);

      assertEquals(monitorQuery, monitor.getQuery("1"));
    }
  }

  public void testGetQueryNotPresent() throws IOException {
    try (Monitor monitor = newMonitorWithPersistence()) {
      assertNull(monitor.getQuery("1"));
    }
  }

  public void testEphemeralMonitorDoesNotStoreQueries() throws IOException {

    try (Monitor monitor2 = newMonitor(ANALYZER)) {
      IllegalStateException e =
          expectThrows(IllegalStateException.class, () -> monitor2.getQuery("query"));
      assertEquals(
          "Cannot get queries from an index with no MonitorQuerySerializer", e.getMessage());
    }
  }

  public void testReadingAfterHashOrderChange() throws IOException {
    Document doc = new Document();
    doc.add(newTextField(FIELD, "test", Field.Store.NO));
    Function<String, Query> parser =
        queryStr -> {
          var query = (BooleanQuery) MonitorTestBase.parse(queryStr);
          return incompatibleBooleanQuery(query, 1);
        };
    try (Monitor monitor = newMonitorWithPersistence(parser)) {
      StringBuilder queryStr = new StringBuilder();
      for (int i = 0; i < 100; ++i) {
        queryStr.append("test").append(i).append(" OR ");
      }
      queryStr.append(" test");
      var mq =
          new MonitorQuery(
              "1",
              incompatibleBooleanQuery((BooleanQuery) parse(queryStr.toString()), 1),
              queryStr.toString(),
              Collections.emptyMap());
      monitor.register(mq);
      assertEquals(1, monitor.getQueryCount());
      assertEquals(1, monitor.match(doc, QueryMatch.SIMPLE_MATCHER).getMatchCount());
    }

    parser =
        queryStr -> {
          var query = (BooleanQuery) MonitorTestBase.parse(queryStr);
          return incompatibleBooleanQuery(query, ~StringHelper.GOOD_FAST_HASH_SEED);
        };

    try (Monitor monitor2 = newMonitorWithPersistence(parser)) {
      assertEquals(1, monitor2.getQueryCount());
      assertEquals(1, monitor2.match(doc, QueryMatch.SIMPLE_MATCHER).getMatchCount());
    }
  }

  public void testReadingDismaxAfterHashOrderChange() throws IOException {
    Document doc = new Document();
    doc.add(newTextField(FIELD, "test", Field.Store.NO));
    float tieBreakerMultiplier = 0.8f;
    Function<String, Query> parser =
        queryStr -> {
          var query =
              new DisjunctionMaxQuery(
                  ((BooleanQuery) MonitorTestBase.parse(queryStr))
                      .getClauses(BooleanClause.Occur.SHOULD),
                  tieBreakerMultiplier);
          return incompatibleDisMaxQuery(query, 1);
        };
    try (Monitor monitor = newMonitorWithPersistence(parser)) {
      StringBuilder queryStr = new StringBuilder("(");
      for (int i = 0; i < 100; ++i) {
        queryStr.append("test").append(i).append(" OR ");
      }
      queryStr.append(" test)");
      var query =
          new DisjunctionMaxQuery(
              ((BooleanQuery) parse(queryStr.toString())).getClauses(BooleanClause.Occur.SHOULD),
              tieBreakerMultiplier);
      var mq =
          new MonitorQuery(
              "1", incompatibleDisMaxQuery(query, 1), queryStr.toString(), Collections.emptyMap());
      monitor.register(mq);
      assertEquals(1, monitor.getQueryCount());
      assertEquals(1, monitor.match(doc, QueryMatch.SIMPLE_MATCHER).getMatchCount());
    }

    parser =
        queryStr -> {
          var query =
              new DisjunctionMaxQuery(
                  ((BooleanQuery) MonitorTestBase.parse(queryStr))
                      .getClauses(BooleanClause.Occur.SHOULD),
                  tieBreakerMultiplier);
          return incompatibleDisMaxQuery(query, ~StringHelper.GOOD_FAST_HASH_SEED);
        };

    try (Monitor monitor2 = newMonitorWithPersistence(parser)) {
      assertEquals(1, monitor2.getQueryCount());
      assertEquals(1, monitor2.match(doc, QueryMatch.SIMPLE_MATCHER).getMatchCount());
    }
  }

  private static BooleanQuery incompatibleBooleanQuery(BooleanQuery query, int hashcodeFactor) {
    var booleanBuilder = new BooleanQuery.Builder();
    for (var clause : query) {
      booleanBuilder.add(
          new SimulateUpgradeQuery(clause.query(), hashcodeFactor), BooleanClause.Occur.SHOULD);
    }
    return booleanBuilder.build();
  }

  private static DisjunctionMaxQuery incompatibleDisMaxQuery(
      DisjunctionMaxQuery query, int hashcodeFactor) {
    return new DisjunctionMaxQuery(
        query.getDisjuncts().stream()
            .map(q -> new SimulateUpgradeQuery(q, hashcodeFactor))
            .toList(),
        query.getTieBreakerMultiplier());
  }

  private static final class SimulateUpgradeQuery extends Query {

    private final Query innerQuery;
    private final int hashcodeFactor;

    private SimulateUpgradeQuery(Query innerQuery, int hashcodeFactor) {
      this.innerQuery = innerQuery;
      this.hashcodeFactor = hashcodeFactor;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
        throws IOException {
      return innerQuery.createWeight(searcher, scoreMode, boost);
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
      return innerQuery.rewrite(indexSearcher);
    }

    @Override
    public String toString(String field) {
      return innerQuery.toString(field);
    }

    @Override
    public void visit(QueryVisitor visitor) {
      innerQuery.visit(visitor);
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof SimulateUpgradeQuery that)) return false;
      return Objects.equals(innerQuery, that.innerQuery);
    }

    @Override
    public int hashCode() {
      return innerQuery.hashCode() * hashcodeFactor;
    }
  }
}
