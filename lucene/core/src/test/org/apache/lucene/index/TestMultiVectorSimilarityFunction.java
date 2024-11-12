package org.apache.lucene.index;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.VectorUtil;
import org.junit.Test;

public class TestMultiVectorSimilarityFunction extends LuceneTestCase {

  @Test
  public void testSumMaxWithDotProduct() {
    final int dimension = 3;
    final VectorSimilarityFunction vectorSim = VectorSimilarityFunction.DOT_PRODUCT;

    float[][] a =
        new float[][] {
          VectorUtil.l2normalize(new float[] {1.f, 4.f, -3.f}),
          VectorUtil.l2normalize(new float[] {8.f, 3.f, -7.f})
        };
    float[][] b =
        new float[][] {
          VectorUtil.l2normalize(new float[] {-5.f, 2.f, 4.f}),
          VectorUtil.l2normalize(new float[] {7.f, 1.f, -3.f}),
          VectorUtil.l2normalize(new float[] {-5.f, 8.f, 3.f})
        };

    float result = 0f;
    float[] a0_bDot =
        new float[] {
          vectorSim.compare(a[0], b[0]),
          vectorSim.compare(a[0], b[1]),
          vectorSim.compare(a[0], b[2])
        };
    float max = Float.MIN_VALUE;
    for (float k : a0_bDot) {
      max = Float.max(max, k);
    }
    result += max;

    float[] a1_bDot =
        new float[] {
          vectorSim.compare(a[1], b[0]),
          vectorSim.compare(a[1], b[1]),
          vectorSim.compare(a[1], b[2])
        };
    max = Float.MIN_VALUE;
    for (float k : a1_bDot) {
      max = Float.max(max, k);
    }
    result += max;

    float[] a_Packed = new float[a.length * dimension];
    int i = 0;
    for (float[] v : a) {
      System.arraycopy(v, 0, a_Packed, i, dimension);
      i += dimension;
    }
    float[] b_Packed = new float[b.length * dimension];
    i = 0;
    for (float[] v : b) {
      System.arraycopy(v, 0, b_Packed, i, dimension);
      i += dimension;
    }

    MultiVectorSimilarityFunction mvSim =
        new MultiVectorSimilarityFunction(
            VectorSimilarityFunction.DOT_PRODUCT,
            MultiVectorSimilarityFunction.Aggregation.SUM_MAX);
    float score = mvSim.compare(a_Packed, b_Packed, dimension);
    assertEquals(result, score, 0.0001f);
  }

  @Test
  public void testDimensionCheck() {
    float[] a = {1f, 2f, 3f, 4f, 5f, 6f};
    float[] b = {1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f, 9f};
    MultiVectorSimilarityFunction mvSim =
        new MultiVectorSimilarityFunction(
            VectorSimilarityFunction.DOT_PRODUCT,
            MultiVectorSimilarityFunction.Aggregation.SUM_MAX);
    assertThrows(IllegalArgumentException.class, () -> mvSim.compare(a, b, 2));
  }
}
