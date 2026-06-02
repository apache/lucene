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

import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.FixedBitSet;

public class TestRangeFilteredBulkScorer extends LuceneTestCase {

  public void testCollectRangeClipsToQueryInterval() throws Exception {
    BulkScorer bs = newBulkScorer(20, 80);
    var collector = new RangeRecordingCollector();
    int next = bs.score(collector, null, 0, 100);
    assertCollectedRanges(List.of(new int[] {20, 80}), collector.ranges);
    assertTrue(next >= 100);
  }

  public void testScoreWhenWindowEndsBeforeRange() throws Exception {
    BulkScorer bs = newBulkScorer(50, 100);
    var collector = new RangeRecordingCollector();
    int next = bs.score(collector, null, 0, 40);
    assertTrue(collector.ranges.isEmpty());
    assertEquals(50, next);
  }

  public void testScoreWhenWindowStartsAtOrAfterRangeEnd() throws Exception {
    BulkScorer bs = newBulkScorer(10, 50);
    var collector = new RangeRecordingCollector();
    int next = bs.score(collector, null, 55, 200);
    assertTrue(collector.ranges.isEmpty());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, next);
  }

  public void testCost() {
    assertEquals(30L, newBulkScorer(10, 40).cost());
  }

  public void testCompetitiveIteratorRaisesMin() throws Exception {
    BulkScorer bs = newBulkScorer(10, 60);
    var collector = new RangeRecordingCollector();
    DocIdSetIterator competitive = DocIdSetIterator.range(25, 100);
    assertEquals(25, competitive.nextDoc());
    collector.setCompetitiveIterator(competitive);

    int next = bs.score(collector, null, 0, 100);
    assertCollectedRanges(List.of(new int[] {25, 60}), collector.ranges);
    assertTrue(next >= 100);
  }

  public void testAcceptDocsAllLiveMatchesNoAcceptDocs() throws Exception {
    BulkScorer bs = newBulkScorer(10, 40);
    var bits = new FixedBitSet(200);
    bits.set(0, 200);
    var collector = new RangeRecordingCollector();
    int next = bs.score(collector, bits, 0, 100);
    assertCollectedRanges(List.of(new int[] {10, 40}), collector.ranges);
    assertTrue(next >= 100);
  }

  public void testAcceptDocsSplitsAroundDeletedDoc() throws Exception {
    BulkScorer bs = newBulkScorer(10, 20);
    var bits = new FixedBitSet(30);
    bits.set(10, 20);
    bits.clear(15);

    var collector = new RangeRecordingCollector();
    bs.score(collector, bits, 0, 100);
    assertCollectedRanges(List.of(new int[] {10, 15}, new int[] {16, 20}), collector.ranges);
  }

  public void testAcceptDocsNoDocsLiveInWindow() throws Exception {
    BulkScorer bs = newBulkScorer(10, 30);
    var bits = new FixedBitSet(50);
    // nothing set => all deleted

    var collector = new RangeRecordingCollector();
    int next = bs.score(collector, bits, 0, 100);
    assertTrue(collector.ranges.isEmpty());
    assertTrue(next >= 100);
  }

  private static RangeBulkScorer newBulkScorer(int rangeMin, int rangeMaxExclusive) {
    var iterator = DocIdSetIterator.range(rangeMin, rangeMaxExclusive);
    return new RangeBulkScorer(iterator, 1f, rangeMin, rangeMaxExclusive);
  }

  /** Records {@link LeafCollector#collectRange} calls as {@code [min inclusive, max exclusive)}. */
  private static class RangeRecordingCollector implements LeafCollector {
    final List<int[]> ranges = new ArrayList<>();
    private DocIdSetIterator competitiveIterator;

    void setCompetitiveIterator(DocIdSetIterator competitiveIterator) {
      this.competitiveIterator = competitiveIterator;
    }

    @Override
    public void setScorer(Scorable scorer) {}

    @Override
    public void collect(int doc) {
      fail("collect should not be used when testing collectRange path");
    }

    @Override
    public void collectRange(int min, int max) {
      ranges.add(new int[] {min, max});
    }

    @Override
    public DocIdSetIterator competitiveIterator() {
      return competitiveIterator;
    }
  }

  private static void assertCollectedRanges(List<int[]> expected, List<int[]> actual) {
    assertEquals(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      assertArrayEquals(expected.get(i), actual.get(i));
    }
  }
}
