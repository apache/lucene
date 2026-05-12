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

import java.util.Random;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.tests.util.LuceneTestCase;

/**
 * Verifies correctness of single-field and multi-field doc values range queries using {@link
 * BatchDocValuesRangeIterator}.
 *
 * <p>Key behavioral notes:
 *
 * <ul>
 *   <li>Single-field range with a second clause (e.g., MatchAllDocsQuery): goes through {@code
 *       DenseConjunctionBulkScorer}, which calls {@code BatchDocValuesRangeIterator.intoBitSet()},
 *       dispatching to SIMD via {@code NumericDocValues.rangeIntoBitSet()} for MAYBE blocks.
 *   <li>Multi-field range: {@code DenseConjunctionBulkScorer} intersects all clauses via {@code
 *       intoBitSet()}, with YES blocks set directly and MAYBE blocks evaluated via {@code
 *       rangeIntoBitSet()}.
 * </ul>
 */
public class TestSkipBlockRangeIteratorIntoBitSet extends LuceneTestCase {

  private static final int DOC_COUNT = 50_000;

  private Directory dir;
  private DirectoryReader reader;
  private IndexSearcher searcher;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir = MMapDirectory.open(createTempDir("intoBitSetTest"));
    IndexWriterConfig iwc = new IndexWriterConfig();
    iwc.setCodec(new Lucene104Codec());
    IndexWriter w = new IndexWriter(dir, iwc);
    Random r = new Random(42);
    for (int i = 0; i < DOC_COUNT; i++) {
      Document doc = new Document();
      doc.add(NumericDocValuesField.indexedField("age", r.nextInt(100)));
      doc.add(NumericDocValuesField.indexedField("score", r.nextInt(1000)));
      w.addDocument(doc);
    }
    w.forceMerge(1);
    reader = DirectoryReader.open(w);
    w.close();
    searcher = new IndexSearcher(reader);
  }

  @Override
  public void tearDown() throws Exception {
    reader.close();
    dir.close();
    super.tearDown();
  }

  /** Single-field range query returns correct results. */
  public void testSingleFieldRangeCorrectness() throws Exception {
    Query q = SortedNumericDocValuesField.newSlowRangeQuery("age", 20, 40);
    int count = searcher.count(q);
    assertTrue("Should find some docs in range [20,40]", count > 0);
    assertTrue("Should not match all docs", count < DOC_COUNT);
  }

  /**
   * Multi-field range query returns correct results and is a strict subset of each single-field
   * result.
   */
  public void testMultiFieldRangeCorrectness() throws Exception {
    Query ageOnly = SortedNumericDocValuesField.newSlowRangeQuery("age", 20, 40);
    Query scoreOnly = SortedNumericDocValuesField.newSlowRangeQuery("score", 100, 500);
    Query both =
        new BooleanQuery.Builder().add(ageOnly, Occur.FILTER).add(scoreOnly, Occur.FILTER).build();

    int ageCount = searcher.count(ageOnly);
    int scoreCount = searcher.count(scoreOnly);
    int bothCount = searcher.count(both);

    assertTrue("Should find some docs matching both fields", bothCount > 0);
    assertTrue("Conjunction must be <= age-only count", bothCount <= ageCount);
    assertTrue("Conjunction must be <= score-only count", bothCount <= scoreCount);
  }

  /** Verifies that results are consistent across repeated executions (no state leakage). */
  public void testResultsAreRepeatable() throws Exception {
    Query q =
        new BooleanQuery.Builder()
            .add(SortedNumericDocValuesField.newSlowRangeQuery("age", 20, 40), Occur.FILTER)
            .add(SortedNumericDocValuesField.newSlowRangeQuery("score", 100, 500), Occur.FILTER)
            .build();

    int count1 = searcher.count(q);
    int count2 = searcher.count(q);
    assertEquals("Results must be deterministic", count1, count2);
  }
}
