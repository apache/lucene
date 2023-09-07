package org.apache.lucene.util;

import java.io.IOException;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestScalarQuantizedVectorSimilarity extends LuceneTestCase {

  public void testToEuclidean() throws IOException {
    int dims = 128;
    int numVecs = 100;

    float[][] floats = randomFloats(numVecs, dims);
    for (int quantile : new int[] {90, 95, 99, 100}) {
      float error = Math.max((100 - quantile) * 0.01f, 0.01f);
      FloatVectorValues floatVectorValues = fromFloats(floats);
      ScalarQuantizer scalarQuantizer = ScalarQuantizer.fromVectors(floatVectorValues, quantile);
      byte[][] quantized = quantizeVectors(scalarQuantizer, floats);
      float globalOffset = scalarQuantizer.globalVectorOffset(dims);
      float[] query = ArrayUtil.copyOfSubArray(floats[0], 0, dims);
      ScalarQuantizedVectorSimilarity quantizedSimilarity =
          ScalarQuantizedVectorSimilarity.fromVectorSimilarity(
              VectorSimilarityFunction.EUCLIDEAN, globalOffset);
      assertQuantizedScores(
          floats,
          quantized,
          query,
          error,
          VectorSimilarityFunction.EUCLIDEAN,
          quantizedSimilarity,
          scalarQuantizer,
          globalOffset);
    }
  }

  public void testToCosine() throws IOException {
    int dims = 128;
    int numVecs = 100;

    float[][] floats = randomFloats(numVecs, dims);

    for (int quantile : new int[] {90, 95, 99, 100}) {
      float error = Math.max((100 - quantile) * 0.01f, 0.01f);
      FloatVectorValues floatVectorValues = fromFloatsNormalized(floats);
      ScalarQuantizer scalarQuantizer = ScalarQuantizer.fromVectors(floatVectorValues, quantile);
      byte[][] quantized = quantizeVectorsNormalized(scalarQuantizer, floats);
      float globalOffset = scalarQuantizer.globalVectorOffset(dims);
      float[] query = ArrayUtil.copyOfSubArray(floats[0], 0, dims);
      ScalarQuantizedVectorSimilarity quantizedSimilarity =
          ScalarQuantizedVectorSimilarity.fromVectorSimilarity(
              VectorSimilarityFunction.COSINE, globalOffset);
      assertQuantizedScores(
          floats,
          quantized,
          query,
          error,
          VectorSimilarityFunction.COSINE,
          quantizedSimilarity,
          scalarQuantizer,
          globalOffset);
    }
  }

  public void testToDotProduct() throws IOException {
    int dims = 128;
    int numVecs = 100;

    float[][] floats = randomFloats(numVecs, dims);
    for (float[] fs : floats) {
      VectorUtil.l2normalize(fs);
    }
    for (int quantile : new int[] {90, 95, 99, 100}) {
      float error = Math.max((100 - quantile) * 0.01f, 0.01f);
      FloatVectorValues floatVectorValues = fromFloats(floats);
      ScalarQuantizer scalarQuantizer = ScalarQuantizer.fromVectors(floatVectorValues, quantile);
      byte[][] quantized = quantizeVectors(scalarQuantizer, floats);
      float globalOffset = scalarQuantizer.globalVectorOffset(dims);
      float[] query = randomFloatArray(dims);
      VectorUtil.l2normalize(query);
      ScalarQuantizedVectorSimilarity quantizedSimilarity =
          ScalarQuantizedVectorSimilarity.fromVectorSimilarity(
              VectorSimilarityFunction.DOT_PRODUCT, globalOffset);
      assertQuantizedScores(
          floats,
          quantized,
          query,
          error,
          VectorSimilarityFunction.DOT_PRODUCT,
          quantizedSimilarity,
          scalarQuantizer,
          globalOffset);
    }
  }

  public void testToMaxInnerProduct() throws IOException {
    int dims = 128;
    int numVecs = 100;

    float[][] floats = randomFloats(numVecs, dims);
    for (int quantile : new int[] {90, 95, 99, 100}) {
      float error = Math.max((100 - quantile) * 0.5f, 0.5f);
      FloatVectorValues floatVectorValues = fromFloats(floats);
      ScalarQuantizer scalarQuantizer = ScalarQuantizer.fromVectors(floatVectorValues, quantile);
      byte[][] quantized = quantizeVectors(scalarQuantizer, floats);
      float globalOffset = scalarQuantizer.globalVectorOffset(dims);
      float[] query = randomFloatArray(dims);
      ScalarQuantizedVectorSimilarity quantizedSimilarity =
          ScalarQuantizedVectorSimilarity.fromVectorSimilarity(
              VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT, globalOffset);
      assertQuantizedScores(
          floats,
          quantized,
          query,
          error,
          VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT,
          quantizedSimilarity,
          scalarQuantizer,
          globalOffset);
    }
  }

  public void testPercentilesSorted() {
    float[] percs = new float[100];
    for (int i = 0; i < 100; i++) {
      percs[i] = (float) i;
    }
    int quantile = 90;
    int index = (int) Math.ceil(quantile / 100.0 * percs.length);
    float upper = percs[index];
    float lower = percs[(int) Math.ceil((100 - quantile) / 100.0 * percs.length)];
    assertEquals(upper, 90, 1e-7);
    assertEquals(lower, 10, 1e-7);
  }

  public void testQuantiles() {
    float[] percs = new float[1000];
    for (int i = 0; i < 1000; i++) {
      percs[i] = (float) i;
    }
    shuffleArray(percs);
    float[] upperAndLower = ScalarQuantizer.getUpperAndLowerQuantile(percs, 90);
    assertEquals(50f, upperAndLower[0], 1e-7);
    assertEquals(950f, upperAndLower[1], 1e-7);
    shuffleArray(percs);
    upperAndLower = ScalarQuantizer.getUpperAndLowerQuantile(percs, 95);
    assertEquals(25f, upperAndLower[0], 1e-7);
    assertEquals(975f, upperAndLower[1], 1e-7);
    shuffleArray(percs);
    upperAndLower = ScalarQuantizer.getUpperAndLowerQuantile(percs, 99);
    assertEquals(5f, upperAndLower[0], 1e-7);
    assertEquals(995f, upperAndLower[1], 1e-7);
  }

  private void assertQuantizedScores(
      float[][] floats,
      byte[][] quantized,
      float[] query,
      float error,
      VectorSimilarityFunction similarityFunction,
      ScalarQuantizedVectorSimilarity quantizedSimilarity,
      ScalarQuantizer scalarQuantizer,
      float globalOffset) {
    for (int i = 0; i < floats.length; i++) {
      float storedOffset =
          ScalarQuantizedVectorSimilarity.scoreCorrectiveOffset(
              similarityFunction,
              quantized[i],
              scalarQuantizer.getAlpha(),
              scalarQuantizer.getOffset());
      byte[] quantizedQuery = scalarQuantizer.quantize(query);
      float queryOffset =
          ScalarQuantizedVectorSimilarity.scoreCorrectiveOffset(
              similarityFunction,
              quantizedQuery,
              scalarQuantizer.getAlpha(),
              scalarQuantizer.getOffset());
      float original = similarityFunction.compare(query, floats[i]);
      float quantizedScore =
          quantizedSimilarity.score(
              quantizedQuery, queryOffset, quantized[i], globalOffset + storedOffset);
      assertEquals("Not within acceptable error [" + error + "]", original, quantizedScore, error);
    }
  }

  static void shuffleArray(float[] ar) {
    for (int i = ar.length - 1; i > 0; i--) {
      int index = random().nextInt(i + 1);
      float a = ar[index];
      ar[index] = ar[i];
      ar[i] = a;
    }
  }

  private static float[] randomFloatArray(int dims) {
    float[] arr = new float[dims];
    for (int j = 0; j < dims; j++) {
      arr[j] = random().nextFloat(-1, 1);
    }
    return arr;
  }

  private static float[][] randomFloats(int num, int dims) {
    float[][] floats = new float[num][];
    for (int i = 0; i < num; i++) {
      floats[i] = randomFloatArray(dims);
    }
    return floats;
  }

  private static byte[][] quantizeVectors(ScalarQuantizer scalarQuantizer, float[][] floats) {
    byte[][] quantized = new byte[floats.length][];
    int i = 0;
    for (float[] v : floats) {
      quantized[i++] = scalarQuantizer.quantize(v);
    }
    return quantized;
  }

  private static byte[][] quantizeVectorsNormalized(
      ScalarQuantizer scalarQuantizer, float[][] floats) {
    byte[][] quantized = new byte[floats.length][];
    int i = 0;
    for (float[] f : floats) {
      float[] v = ArrayUtil.copyOfSubArray(f, 0, f.length);
      VectorUtil.l2normalize(v);
      quantized[i++] = scalarQuantizer.quantize(v);
    }
    return quantized;
  }

  private static FloatVectorValues fromFloatsNormalized(float[][] floats) {
    return new TestSimpleFloatVectorValues(floats) {
      @Override
      public float[] vectorValue() throws IOException {
        if (curDoc == -1 || curDoc >= floats.length) {
          throw new IOException("Current doc not set or too many iterations");
        }
        float[] v = ArrayUtil.copyOfSubArray(floats[curDoc], 0, floats[curDoc].length);
        VectorUtil.l2normalize(v);
        return v;
      }
    };
  }

  private static FloatVectorValues fromFloats(float[][] floats) {
    return new TestSimpleFloatVectorValues(floats);
  }

  private static class TestSimpleFloatVectorValues extends FloatVectorValues {
    protected final float[][] floats;
    protected int curDoc = -1;

    TestSimpleFloatVectorValues(float[][] values) {
      this.floats = values;
    }

    @Override
    public int dimension() {
      return floats[0].length;
    }

    @Override
    public int size() {
      return floats.length;
    }

    @Override
    public float[] vectorValue() throws IOException {
      if (curDoc == -1 || curDoc >= floats.length) {
        throw new IOException("Current doc not set or too many iterations");
      }
      return floats[curDoc];
    }

    @Override
    public int docID() {
      if (curDoc >= floats.length) {
        return NO_MORE_DOCS;
      }
      return curDoc;
    }

    @Override
    public int nextDoc() throws IOException {
      curDoc++;
      return docID();
    }

    @Override
    public int advance(int target) throws IOException {
      curDoc = target;
      return docID();
    }
  }
}
