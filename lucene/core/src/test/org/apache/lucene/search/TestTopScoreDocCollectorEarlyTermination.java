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

import java.util.List;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestTopScoreDocCollectorEarlyTermination extends LuceneTestCase {
  private DirectoryReader createIndex(Directory directory, int segments, int docs)
      throws Exception {
    try (IndexWriter writer =
        new IndexWriter(
            directory, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
      for (int segment = 0; segment < segments; segment++) {
        for (int doc = 0; doc < docs; doc++) {
          Document document = new Document();
          long latency = doc % 50;
          long high = doc % 50;
          document.add(new LongPoint("latency", latency));
          document.add(new SortedNumericDocValuesField("latency", latency));
          document.add(new LongPoint("high", high));
          document.add(new SortedNumericDocValuesField("high", high));
          writer.addDocument(document);
        }
        writer.flush();
      }
      return DirectoryReader.open(writer);
    }
  }

  private static Query filters() {
    return new BooleanQuery.Builder()
        .add(range("latency", 5), BooleanClause.Occur.FILTER)
        .add(range("high", 10), BooleanClause.Occur.FILTER)
        .build();
  }

  private static Query range(String field, long lower) {
    return new IndexOrDocValuesQuery(
        LongPoint.newRangeQuery(field, lower, Long.MAX_VALUE),
        SortedNumericDocValuesField.newSlowRangeQuery(field, lower, Long.MAX_VALUE));
  }

  private static void assertHits(TopDocs expected, TopDocs actual) {
    assertEquals(expected.scoreDocs.length, actual.scoreDocs.length);
    for (int i = 0; i < expected.scoreDocs.length; i++) {
      assertEquals(expected.scoreDocs[i].doc, actual.scoreDocs[i].doc);
      assertEquals(expected.scoreDocs[i].score, actual.scoreDocs[i].score, 0f);
    }
  }

  public void testSkipRemainingSegments() throws Exception {
    try (Directory directory = newDirectory()) {
      try (DirectoryReader reader = createIndex(directory, 3, 100)) {
        assertEquals(3, reader.leaves().size());

        IndexSearcher searcher = new IndexSearcher(reader);
        for (Query query : List.of(new ConstantScoreQuery(filters()), filters())) {
          TopDocs expected =
              searcher.search(query, new TopScoreDocCollectorManager(5, Integer.MAX_VALUE));
          assertEquals(240, expected.totalHits.value());
          Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.TOP_SCORES, 1);
          TopScoreDocCollector collector = new TopScoreDocCollector(5, null, 20, null);
          collector.setWeight(weight);
          searcher.searchLeaf(reader.leaves().getFirst(), 0, 100, weight, collector);
          for (int segment = 1; segment < reader.leaves().size(); segment++) {
            var context = reader.leaves().get(segment);
            expectThrows(
                CollectionTerminatedException.class, () -> collector.getLeafCollector(context));
          }
          TopDocs actual = collector.topDocs();
          assertHits(expected, actual);
          assertEquals(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO, actual.totalHits.relation());
          assertTrue(actual.totalHits.value() > 20 && actual.totalHits.value() <= 80);
        }
      }
    }
  }

  public void testSharedBoundaryPreservesEarlierDocIds() throws Exception {
    try (Directory directory = newDirectory()) {
      try (DirectoryReader reader = createIndex(directory, 3, 100)) {
        assertEquals(3, reader.leaves().size());
        IndexSearcher searcher = new IndexSearcher(reader);
        Query query = filters();
        TopDocs expected =
            searcher.search(query, new TopScoreDocCollectorManager(5, Integer.MAX_VALUE));
        Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.TOP_SCORES, 1);
        TopScoreDocCollectorManager manager = new TopScoreDocCollectorManager(5, 10);
        TopScoreDocCollector earlierSlice = manager.newCollector();
        TopScoreDocCollector laterSlice = manager.newCollector();
        earlierSlice.setWeight(weight);
        laterSlice.setWeight(weight);

        // The later slice publishes a shared boundary first.
        searcher.searchLeaf(reader.leaves().get(1), 0, 100, weight, laterSlice);
        assertEquals(5, laterSlice.topDocsSize());
        assertEquals(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO, laterSlice.totalHitsRelation);
        // Equal-score documents in the earlier slice have smaller doc IDs and remain competitive.
        searcher.searchLeaf(reader.leaves().getFirst(), 0, 100, weight, earlierSlice);
        // Equal-score documents after the shared boundary cannot be competitive.
        expectThrows(
            CollectionTerminatedException.class,
            () -> laterSlice.getLeafCollector(reader.leaves().get(2)));
        assertHits(expected, manager.reduce(List.of(earlierSlice, laterSlice)));
      }
    }
  }
}
