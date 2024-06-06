package org.apache.lucene.search.knn;

import org.apache.lucene.search.TopKnnCollector;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.hnsw.BlockingFloatHeap;
import org.junit.Ignore;

public class TestMultiLeafKnnCollector extends LuceneTestCase {
  @Ignore
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
    // which also forces an update of the global heap with collector2's hits:
    collector2.collect(0, 10f);
    collector2.collect(0, 11f);
    collector2.collect(0, 12f);
    collector2.collect(0, 13f);
    collector2.collect(0, 200f);
    collector2.collect(0, 14f);
    collector2.collect(0, 300f);

    // The value 200 is failing to get added to the global heap. collector2 keeps its local scores
    // in a heap for the global update where the layout is [10, 11, 12, 13, 200, 14, 300]. This
    // is a correct heap partial ordering but the logic for updating the global heap assumes the
    // data is sorted and short-circuits the updates to the global heap after visiting the value
    // 14 (it visits the values in descending order assuming they are fully sorted).

    // At this point, our global heap should contain [102, 103, 104, 105, 106, 200, 300] since
    // values 200 and 300 from collector2 should have pushed out 100 and 101 from collector1.
    // The min value on the global heap should be 102:
    assertEquals(102f, globalHeap.peek(), 0f);
    assertEquals(102f, collector2.minCompetitiveSimilarity(), 0f);
  }
}
