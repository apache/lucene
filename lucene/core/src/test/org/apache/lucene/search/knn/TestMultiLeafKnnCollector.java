package org.apache.lucene.search.knn;

import org.apache.lucene.search.TopKnnCollector;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.hnsw.BlockingFloatHeap;

public class TestMultiLeafKnnCollector extends LuceneTestCase {

  /** Validates a fix for GH#13462 */
  public void testGlobalScoreCoordination() {
    int k = 7;
    BlockingFloatHeap globalHeap = new BlockingFloatHeap(k);
    MultiLeafKnnCollector collector1 =
        new MultiLeafKnnCollector(k, globalHeap, new TopKnnCollector(k, Integer.MAX_VALUE));
    MultiLeafKnnCollector collector2 =
        new MultiLeafKnnCollector(k, globalHeap, new TopKnnCollector(k, Integer.MAX_VALUE));

    // Collect k (7) hits in collector1 with scores [100, 106]:
    for (int i = 0; i < k; i++) {
      collector1.collect(0, 100f + i);
    }

    // The global heap should be updated since k hits were collected, and have a min score of
    // 100:
    assertEquals(100f, globalHeap.peek(), 0f);
    assertEquals(100f, collector1.minCompetitiveSimilarity(), 0f);

    // Collect k (7) hits in collector2 with only two that are competitive (200 and 300),
    // which also forces an update of the global heap with collector2's hits. This is a tricky
    // case where the heap will not be fully ordered, so it ensures global queue updates don't
    // incorrectly short-circuit (see GH#13462):
    collector2.collect(0, 10f);
    collector2.collect(0, 11f);
    collector2.collect(0, 12f);
    collector2.collect(0, 13f);
    collector2.collect(0, 200f);
    collector2.collect(0, 14f);
    collector2.collect(0, 300f);

    // At this point, our global heap should contain [102, 103, 104, 105, 106, 200, 300] since
    // values 200 and 300 from collector2 should have pushed out 100 and 101 from collector1.
    // The min value on the global heap should be 102:
    assertEquals(102f, globalHeap.peek(), 0f);
    assertEquals(102f, collector2.minCompetitiveSimilarity(), 0f);
  }
}
