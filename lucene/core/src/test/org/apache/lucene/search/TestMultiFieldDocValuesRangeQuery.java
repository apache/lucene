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
import java.util.List;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.MultiFieldDocValuesRangeQuery.FieldRange;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;

/**
 * Tests for {@link MultiFieldDocValuesRangeQuery}. Verifies that the coordinated multi-field skip
 * evaluation produces the same results as independent BooleanQuery FILTER clauses.
 */
public class TestMultiFieldDocValuesRangeQuery extends LuceneTestCase {

  /** Helper to build a MultiFieldDocValuesRangeQuery with a proper fallback. */
  private static MultiFieldDocValuesRangeQuery buildQuery(List<FieldRange> ranges) {
    BooleanQuery.Builder fallback = new BooleanQuery.Builder();
    for (FieldRange fr : ranges) {
      fallback.add(
          SortedNumericDocValuesField.newSlowRangeQuery(
              fr.field(), fr.lowerValue(), fr.upperValue()),
          BooleanClause.Occur.FILTER);
    }
    return new MultiFieldDocValuesRangeQuery(ranges, fallback.build());
  }

  /**
   * Compare coordinated query results against a BooleanQuery with independent range filters. They
   * must return exactly the same documents.
   */
  public void testMatchesBooleanQuery() throws IOException {
    try (Directory dir = newDirectory()) {
      int numDocs = 20000;
      try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
        for (int i = 0; i < numDocs; i++) {
          Document doc = new Document();
          doc.add(new NumericDocValuesField("price", i % 100));
          doc.add(new NumericDocValuesField("rating", i % 5));
          doc.add(new NumericDocValuesField("stock", i % 1000));
          writer.addDocument(doc);
        }
        writer.forceMerge(1);
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = newSearcher(reader);

        MultiFieldDocValuesRangeQuery coordinated =
            buildQuery(
                List.of(
                    new FieldRange("price", 20, 40),
                    new FieldRange("rating", 2, 4),
                    new FieldRange("stock", 100, 500)));

        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        bq.add(
            SortedNumericDocValuesField.newSlowRangeQuery("price", 20, 40),
            BooleanClause.Occur.FILTER);
        bq.add(
            SortedNumericDocValuesField.newSlowRangeQuery("rating", 2, 4),
            BooleanClause.Occur.FILTER);
        bq.add(
            SortedNumericDocValuesField.newSlowRangeQuery("stock", 100, 500),
            BooleanClause.Occur.FILTER);

        int coordCount = searcher.count(coordinated);
        int boolCount = searcher.count(bq.build());
        assertEquals(
            "Coordinated query must match same doc count as BooleanQuery", boolCount, coordCount);
      }
    }
  }

  /** Test with no matching documents — all fields out of range. */
  public void testNoMatches() throws IOException {
    try (Directory dir = newDirectory()) {
      try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
        for (int i = 0; i < 10000; i++) {
          Document doc = new Document();
          doc.add(new NumericDocValuesField("a", i));
          doc.add(new NumericDocValuesField("b", i));
          writer.addDocument(doc);
        }
        writer.forceMerge(1);
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = newSearcher(reader);
        MultiFieldDocValuesRangeQuery q =
            buildQuery(List.of(new FieldRange("a", 0, 100), new FieldRange("b", 5000, 9999)));
        assertEquals(0, searcher.count(q));
      }
    }
  }

  /** Test where one field matches everything and the other is selective. */
  public void testOneFieldMatchesAll() throws IOException {
    try (Directory dir = newDirectory()) {
      try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
        for (int i = 0; i < 10000; i++) {
          Document doc = new Document();
          doc.add(new NumericDocValuesField("wide", i));
          doc.add(new NumericDocValuesField("narrow", i % 10));
          writer.addDocument(doc);
        }
        writer.forceMerge(1);
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = newSearcher(reader);
        MultiFieldDocValuesRangeQuery q =
            buildQuery(List.of(new FieldRange("wide", 0, 99999), new FieldRange("narrow", 3, 5)));

        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        bq.add(
            SortedNumericDocValuesField.newSlowRangeQuery("wide", 0, 99999),
            BooleanClause.Occur.FILTER);
        bq.add(
            SortedNumericDocValuesField.newSlowRangeQuery("narrow", 3, 5),
            BooleanClause.Occur.FILTER);

        assertEquals(searcher.count(bq.build()), searcher.count(q));
      }
    }
  }

  /** Test with negative values. */
  public void testNegativeValues() throws IOException {
    try (Directory dir = newDirectory()) {
      try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
        for (int i = 0; i < 10000; i++) {
          Document doc = new Document();
          doc.add(new NumericDocValuesField("a", i - 5000));
          doc.add(new NumericDocValuesField("b", (i - 5000) * 2));
          writer.addDocument(doc);
        }
        writer.forceMerge(1);
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = newSearcher(reader);
        MultiFieldDocValuesRangeQuery q =
            buildQuery(List.of(new FieldRange("a", -1000, 1000), new FieldRange("b", -500, 500)));

        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        bq.add(
            SortedNumericDocValuesField.newSlowRangeQuery("a", -1000, 1000),
            BooleanClause.Occur.FILTER);
        bq.add(
            SortedNumericDocValuesField.newSlowRangeQuery("b", -500, 500),
            BooleanClause.Occur.FILTER);

        assertEquals(searcher.count(bq.build()), searcher.count(q));
      }
    }
  }

  /** Test with multi-valued fields (SortedNumericDocValues). */
  public void testMultiValuedFields() throws IOException {
    try (Directory dir = newDirectory()) {
      try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
        for (int i = 0; i < 10000; i++) {
          Document doc = new Document();
          doc.add(new SortedNumericDocValuesField("prices", i * 10));
          doc.add(new SortedNumericDocValuesField("prices", i * 10 + 5));
          doc.add(new SortedNumericDocValuesField("ratings", i % 5));
          doc.add(new SortedNumericDocValuesField("ratings", (i % 5) + 1));
          writer.addDocument(doc);
        }
        writer.forceMerge(1);
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = newSearcher(reader);
        MultiFieldDocValuesRangeQuery q =
            buildQuery(
                List.of(new FieldRange("prices", 500, 2000), new FieldRange("ratings", 3, 5)));

        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        bq.add(
            SortedNumericDocValuesField.newSlowRangeQuery("prices", 500, 2000),
            BooleanClause.Occur.FILTER);
        bq.add(
            SortedNumericDocValuesField.newSlowRangeQuery("ratings", 3, 5),
            BooleanClause.Occur.FILTER);

        assertEquals(searcher.count(bq.build()), searcher.count(q));
      }
    }
  }

  /** Test with 5 fields. */
  public void testFiveFields() throws IOException {
    try (Directory dir = newDirectory()) {
      try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
        for (int i = 0; i < 50000; i++) {
          Document doc = new Document();
          doc.add(new NumericDocValuesField("f1", i % 100));
          doc.add(new NumericDocValuesField("f2", i % 50));
          doc.add(new NumericDocValuesField("f3", i % 200));
          doc.add(new NumericDocValuesField("f4", i % 10));
          doc.add(new NumericDocValuesField("f5", i % 500));
          writer.addDocument(doc);
        }
        writer.forceMerge(1);
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = newSearcher(reader);
        MultiFieldDocValuesRangeQuery q =
            buildQuery(
                List.of(
                    new FieldRange("f1", 10, 30),
                    new FieldRange("f2", 5, 15),
                    new FieldRange("f3", 50, 100),
                    new FieldRange("f4", 2, 4),
                    new FieldRange("f5", 100, 200)));

        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        bq.add(
            SortedNumericDocValuesField.newSlowRangeQuery("f1", 10, 30),
            BooleanClause.Occur.FILTER);
        bq.add(
            SortedNumericDocValuesField.newSlowRangeQuery("f2", 5, 15), BooleanClause.Occur.FILTER);
        bq.add(
            SortedNumericDocValuesField.newSlowRangeQuery("f3", 50, 100),
            BooleanClause.Occur.FILTER);
        bq.add(
            SortedNumericDocValuesField.newSlowRangeQuery("f4", 2, 4), BooleanClause.Occur.FILTER);
        bq.add(
            SortedNumericDocValuesField.newSlowRangeQuery("f5", 100, 200),
            BooleanClause.Occur.FILTER);

        assertEquals(searcher.count(bq.build()), searcher.count(q));
      }
    }
  }

  /** Test that the query rejects fewer than 2 field ranges. */
  public void testMinimumTwoFields() {
    BooleanQuery fallback =
        new BooleanQuery.Builder()
            .add(
                SortedNumericDocValuesField.newSlowRangeQuery("single", 0, 100),
                BooleanClause.Occur.FILTER)
            .build();
    expectThrows(
        IllegalArgumentException.class,
        () ->
            new MultiFieldDocValuesRangeQuery(List.of(new FieldRange("single", 0, 100)), fallback));
  }

  /** Test that BooleanQuery.rewrite detects and rewrites to MultiFieldDocValuesRangeQuery. */
  public void testBooleanQueryRewrite() throws IOException {
    try (Directory dir = newDirectory()) {
      try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
        for (int i = 0; i < 10000; i++) {
          Document doc = new Document();
          doc.add(new NumericDocValuesField("a", i % 100));
          doc.add(new NumericDocValuesField("b", i % 50));
          writer.addDocument(doc);
        }
        writer.forceMerge(1);
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = newSearcher(reader);

        // Build a BooleanQuery with 2 required numeric range queries
        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        bq.add(
            SortedNumericDocValuesField.newSlowRangeQuery("a", 10, 30), BooleanClause.Occur.FILTER);
        bq.add(
            SortedNumericDocValuesField.newSlowRangeQuery("b", 5, 15), BooleanClause.Occur.FILTER);

        Query rewritten = bq.build().rewrite(searcher);

        // The rewritten query should contain a MultiFieldDocValuesRangeQuery
        boolean foundMultiField = false;
        if (rewritten instanceof BooleanQuery rewrittenBq) {
          for (BooleanClause clause : rewrittenBq.clauses()) {
            if (clause.query() instanceof MultiFieldDocValuesRangeQuery) {
              foundMultiField = true;
              break;
            }
          }
        }
        assertTrue(
            "BooleanQuery with 2+ numeric range filters should rewrite to include MultiFieldDocValuesRangeQuery",
            foundMultiField);
      }
    }
  }
}
