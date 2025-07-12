package org.apache.lucene.benchmark.jmh;

import org.apache.lucene.tests.util.LuceneTestCase;
import java.io.IOException;
import java.util.Arrays;

public class TestVectorScorerFloat32Benchmark extends LuceneTestCase {

  @com.carrotsearch.randomizedtesting.annotations.Repeat(iterations = 10)
  public void testDotProduct() throws IOException {
    float delta = 1e-3f * 1024;
    var bench = new VectorScorerFloat32Benchmark();
    bench.size = 1024;
    bench.init();

    try {
      Arrays.fill(bench.scores, 0.0f);
      bench.dotProductDefault();
      var expectedScores = Arrays.copyOfRange(bench.scores, 0, bench.scores.length);

      Arrays.fill(bench.scores, 0.0f);
      bench.dotProductNewScorer();
      var actualScores = Arrays.copyOfRange(bench.scores, 0, bench.scores.length);
      assertArrayEquals(expectedScores, actualScores, delta);

      Arrays.fill(bench.scores, 0.0f);
      bench.dotProductNewBulkScore();
      actualScores = Arrays.copyOfRange(bench.scores, 0, bench.scores.length);
      assertArrayEquals(expectedScores, actualScores, delta);

    } finally {
      bench.teardown();
    }
  }
}
