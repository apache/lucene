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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.FSDirectory;
import org.junit.Test;

public class TestMonitorReadonly extends MonitorTestBase {
  private static final Analyzer ANALYZER = new WhitespaceAnalyzer();

  @Test
  public void testReadonlyMonitorThrowsOnInexistentIndex() {
    Path indexDirectory = createTempDir();
    MonitorConfiguration config =
        new MonitorConfiguration()
            .setDirectoryProvider(
                () -> FSDirectory.open(indexDirectory),
                MonitorQuerySerializer.fromParser(MonitorTestBase::parse),
                true);
    assertThrows(
        IndexNotFoundException.class,
        () -> {
          new Monitor(ANALYZER, config);
        });
  }

  @Test
  public void testReadonlyMonitorThrowsWhenCallingWriteRequests() throws IOException {
    Path indexDirectory = createTempDir();
    MonitorConfiguration writeConfig =
        new MonitorConfiguration()
            .setIndexPath(
                indexDirectory, MonitorQuerySerializer.fromParser(MonitorTestBase::parse));

    // this will create the index
    Monitor writeMonitor = new Monitor(ANALYZER, writeConfig);
    writeMonitor.close();

    MonitorConfiguration config =
        new MonitorConfiguration()
            .setDirectoryProvider(
                () -> FSDirectory.open(indexDirectory),
                MonitorQuerySerializer.fromParser(MonitorTestBase::parse),
                true);
    try (Monitor monitor = new Monitor(ANALYZER, config)) {
      TermQuery query = new TermQuery(new Term(FIELD, "test"));
      assertThrows(
          UnsupportedOperationException.class,
          () -> {
            monitor.register(
                new MonitorQuery("query1", query, query.toString(), Collections.emptyMap()));
          });

      assertThrows(
          UnsupportedOperationException.class,
          () -> {
            monitor.deleteById("query1");
          });

      assertThrows(
          UnsupportedOperationException.class,
          () -> {
            monitor.clear();
          });
    }
  }

  @Test
  public void testSettingCustomDirectory() throws IOException {
    Path indexDirectory = createTempDir();
    Document doc = new Document();
    doc.add(newTextField(FIELD, "This is a Foobar test document", Field.Store.NO));

    MonitorConfiguration writeConfig =
        new MonitorConfiguration()
            .setDirectoryProvider(
                () -> FSDirectory.open(indexDirectory),
                MonitorQuerySerializer.fromParser(MonitorTestBase::parse));

    try (Monitor writeMonitor = new Monitor(ANALYZER, writeConfig)) {
      TermQuery query = new TermQuery(new Term(FIELD, "test"));
      writeMonitor.register(
          new MonitorQuery("query1", query, query.toString(), Collections.emptyMap()));
      TermQuery query2 = new TermQuery(new Term(FIELD, "Foobar"));
      writeMonitor.register(
          new MonitorQuery("query2", query2, query.toString(), Collections.emptyMap()));
      MatchingQueries<QueryMatch> matches = writeMonitor.match(doc, QueryMatch.SIMPLE_MATCHER);
      assertNotNull(matches.getMatches());
      assertEquals(2, matches.getMatchCount());
      assertNotNull(matches.matches("query2"));
    }
  }

  @Test
  public void testMonitorReadOnlyCouldReadOnTheSameIndex() throws IOException {
    Path indexDirectory = createTempDir();
    Document doc = new Document();
    doc.add(newTextField(FIELD, "This is a test document", Field.Store.NO));

    MonitorConfiguration writeConfig =
        new MonitorConfiguration()
            .setDirectoryProvider(
                () -> FSDirectory.open(indexDirectory),
                MonitorQuerySerializer.fromParser(MonitorTestBase::parse));

    try (Monitor writeMonitor = new Monitor(ANALYZER, writeConfig)) {
      TermQuery query = new TermQuery(new Term(FIELD, "test"));
      writeMonitor.register(
          new MonitorQuery("query1", query, query.toString(), Collections.emptyMap()));
    }

    MonitorConfiguration readConfig =
        new MonitorConfiguration()
            .setDirectoryProvider(
                () -> FSDirectory.open(indexDirectory),
                MonitorQuerySerializer.fromParser(MonitorTestBase::parse),
                true);

    try (Monitor readMonitor1 = new Monitor(ANALYZER, readConfig)) {
      MatchingQueries<QueryMatch> matches = readMonitor1.match(doc, QueryMatch.SIMPLE_MATCHER);
      assertNotNull(matches.getMatches());
      assertEquals(1, matches.getMatchCount());
      assertNotNull(matches.matches("query1"));
    }

    try (Monitor readMonitor2 = new Monitor(ANALYZER, readConfig)) {
      MatchingQueries<QueryMatch> matches = readMonitor2.match(doc, QueryMatch.SIMPLE_MATCHER);
      assertNotNull(matches.getMatches());
      assertEquals(1, matches.getMatchCount());
      assertNotNull(matches.matches("query1"));

      TermQuery query = new TermQuery(new Term(FIELD, "test"));
      assertThrows(
          UnsupportedOperationException.class,
          () -> {
            readMonitor2.register(
                new MonitorQuery("query1", query, query.toString(), Collections.emptyMap()));
          });
    }
  }

  @Test
  public void testReadonlyMonitorGetsRefreshed() throws IOException, InterruptedException {
    Path indexDirectory = createTempDir();
    Document doc = new Document();
    doc.add(newTextField(FIELD, "This is a test document", Field.Store.NO));

    MonitorConfiguration writeConfig =
        new MonitorConfiguration()
            .setDirectoryProvider(
                () -> FSDirectory.open(indexDirectory),
                MonitorQuerySerializer.fromParser(MonitorTestBase::parse));

    try (Monitor writeMonitor = new Monitor(ANALYZER, writeConfig)) {
      TermQuery query = new TermQuery(new Term(FIELD, "test"));
      writeMonitor.register(
          new MonitorQuery("query1", query, query.toString(), Collections.emptyMap()));

      MonitorConfiguration readConfig =
          new MonitorConfiguration()
              .setPurgeFrequency(2, TimeUnit.SECONDS)
              .setDirectoryProvider(
                  () -> FSDirectory.open(indexDirectory),
                  MonitorQuerySerializer.fromParser(MonitorTestBase::parse),
                  true);

      try (Monitor readMonitor = new Monitor(ANALYZER, readConfig)) {
        MatchingQueries<QueryMatch> matches = readMonitor.match(doc, QueryMatch.SIMPLE_MATCHER);
        assertNotNull(matches.getMatches());
        assertEquals(1, matches.getMatchCount());
        assertNotNull(matches.matches("query1"));

        TermQuery query2 = new TermQuery(new Term(FIELD, "test"));
        writeMonitor.register(
            new MonitorQuery("query2", query2, query2.toString(), Collections.emptyMap()));

        // Index returns stale result until background refresh thread calls maybeRefresh
        MatchingQueries<QueryMatch> matches2 = readMonitor.match(doc, QueryMatch.SIMPLE_MATCHER);
        assertNotNull(matches2.getMatches());
        assertEquals(1, matches2.getMatchCount());
        CountDownLatch latch = new CountDownLatch(1);
        readMonitor.addQueryIndexUpdateListener(
            new MonitorUpdateListener() {
              @Override
              public void onPurge() {
                latch.countDown();
              }
            });
        assertTrue(
            latch.await(readConfig.getPurgeFrequency() + 1, readConfig.getPurgeFrequencyUnits()));

        // after frequency results are refreshed
        MatchingQueries<QueryMatch> matches3 = readMonitor.match(doc, QueryMatch.SIMPLE_MATCHER);
        assertNotNull(matches3.getMatches());
        assertEquals(2, matches3.getMatchCount());
      }
    }
  }
}
