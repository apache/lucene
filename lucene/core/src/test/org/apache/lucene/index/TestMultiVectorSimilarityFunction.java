package org.apache.lucene.index;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.Test;

public class TestMultiVectorSimilarityFunction extends LuceneTestCase {

  @Test
  public void testSumMaxWithDotProduct() {
    final int dimension = 3;
    float[] a = new float[] {1.f, 4.f, -3.f, 8.f, 3.f, -7.f, -2.f, 1.f, 9.f};
    float[] b = new float[] {-5.f, 2.f, 4.f, 7.f, 1.f, -3.f, -5.f, 8.f, 3.f};

    MultiVectorSimilarityFunction mvsf = new MultiVectorSimilarityFunction(VectorSimilarityFunction.DOT_PRODUCT, MultiVectorSimilarityFunction.Aggregation.SUM_MAX);
    float score = mvsf.compare(a, b, dimension);
    assertEquals(95f, score, 0.00001f);
  }

}
