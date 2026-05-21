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
package org.apache.lucene.document;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

import java.io.IOException;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

public class TestSortedSkipperScorerSupplier extends LuceneTestCase {

  /**
   * Build a single-segment sorted index with contiguous blocks of numeric doc values and return the
   * cost for an exact-match query on each value.
   *
   * @param blockSizes number of docs per value (value i gets blockSizes[i] docs)
   * @param skipIntervalSize skip index interval size for the codec
   * @param reverse whether the index sort is reversed
   * @return array of costs, one per value
   */
  private long[] buildIndexAndGetCosts(int[] blockSizes, int skipIntervalSize, boolean reverse)
      throws IOException {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig config =
          new IndexWriterConfig()
              .setCodec(
                  TestUtil.alwaysDocValuesFormat(new Lucene90DocValuesFormat(skipIntervalSize)))
              .setIndexSort(new Sort(new SortField("dv", SortField.Type.LONG, reverse)));
      try (IndexWriter iw =
          new IndexWriter(dir, config)) { // not RandomIndexWriter so we stay deterministic
        for (int i = 0; i < blockSizes.length; i++) {
          for (int j = 0; j < blockSizes[i]; j++) {
            Document doc = new Document();
            doc.add(NumericDocValuesField.indexedField("dv", i));
            iw.addDocument(doc);
          }
        }
        iw.forceMerge(1);
      }
      long[] costs = new long[blockSizes.length];
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals(1, reader.leaves().size());
        LeafReaderContext ctx = reader.leaves().getFirst();
        IndexSearcher searcher = new IndexSearcher(reader);
        for (int i = 0; i < blockSizes.length; i++) {
          Query q = NumericDocValuesField.newSlowRangeQuery("dv", i, i);
          assertEquals(blockSizes[i], searcher.count(q));
          Query rewritten = searcher.rewrite(q);
          Weight weight = rewritten.createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1.0f);
          ScorerSupplier supplier = weight.scorerSupplier(ctx);
          costs[i] = supplier.cost();
        }
      }
      return costs;
    }
  }

  private void assertCostWithinBounds(
      long cost, int blockSize, int skipIntervalSize, String label) {
    assertThat(label, cost, greaterThanOrEqualTo((long) blockSize));
    assertThat(label, cost, lessThanOrEqualTo(blockSize + 2L * skipIntervalSize));
  }

  public void testCostBothBoundariesExact() throws IOException {
    // Each block is >= skipIntervalSize and has a single value, so the skip index
    // creates one interval per block with exact boundaries on both sides.
    int skipIntervalSize = 16;
    int[] blockSizes = {32, 48, 32};
    for (boolean reverse : new boolean[] {false, true}) {
      long[] costs = buildIndexAndGetCosts(blockSizes, skipIntervalSize, reverse);
      for (int i = 0; i < blockSizes.length; i++) {
        assertEquals("block " + i + " reverse=" + reverse, blockSizes[i], costs[i]);
      }
    }
  }

  public void testCostMinInexactMaxExact() throws IOException {
    // Block B (val=1, 8 docs) shares its interval with block A (val=0, 8 docs).
    // The next interval starts with val=2, so the max boundary for val=1 is exact,
    // but the min boundary is inexact (interval also contains val=0).
    int skipIntervalSize = 16;
    int[] blockSizes = {8, 8, 32};
    for (boolean reverse : new boolean[] {false, true}) {
      long[] costs = buildIndexAndGetCosts(blockSizes, skipIntervalSize, reverse);
      assertCostWithinBounds(costs[1], blockSizes[1], skipIntervalSize, "reverse=" + reverse);
    }
  }

  public void testCostMinExactMaxInexact() throws IOException {
    // Block B (val=1, 4 docs) starts at its own interval boundary but shares the
    // interval tail with block C (val=2, 12 docs). The min boundary is exact
    // (block A fills prior intervals cleanly), but the max boundary is inexact.
    int skipIntervalSize = 16;
    int[] blockSizes = {32, 4, 12};
    for (boolean reverse : new boolean[] {false, true}) {
      long[] costs = buildIndexAndGetCosts(blockSizes, skipIntervalSize, reverse);
      assertCostWithinBounds(costs[1], blockSizes[1], skipIntervalSize, "reverse=" + reverse);
    }
  }

  public void testCostBothBoundariesInexact() throws IOException {
    // Block C (val=2, 4 docs) sits in the middle of an interval that also contains
    // vals 1 and 3, so both boundaries are inexact.
    int skipIntervalSize = 16;
    int[] blockSizes = {32, 4, 4, 8, 32};
    for (boolean reverse : new boolean[] {false, true}) {
      long[] costs = buildIndexAndGetCosts(blockSizes, skipIntervalSize, reverse);
      assertCostWithinBounds(costs[2], blockSizes[2], skipIntervalSize, "reverse=" + reverse);
    }
  }

  public void testCostWithOversizedNeighborInterval() throws IOException {
    // Block B (val=1, 8 docs) shares its first interval with block A (val=0, 8 docs).
    // Block C (val=2, 64 docs) forms a single oversized interval (docCount >> skipIntervalSize)
    // because all its docs share the same value.
    // The max boundary for val=1 is exact (next interval starts with val=2), so the
    // cost must NOT include the oversized interval's docCount as padding.
    // Without the fix, cost would be 16 + 64 = 80, exceeding the bound of 8 + 32 = 40.
    int skipIntervalSize = 16;
    int[] blockSizes = {8, 8, 64};
    for (boolean reverse : new boolean[] {false, true}) {
      long[] costs = buildIndexAndGetCosts(blockSizes, skipIntervalSize, reverse);
      assertCostWithinBounds(costs[1], blockSizes[1], skipIntervalSize, "reverse=" + reverse);
    }
  }
}
