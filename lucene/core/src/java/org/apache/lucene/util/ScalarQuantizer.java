package org.apache.lucene.util;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;

public class ScalarQuantizer {

  public static final int SCALAR_QUANTIZATION_SAMPLE_SIZE = 100_000;

  private final float alpha;
  private final float offset;
  private final float[] quantiles;

  public ScalarQuantizer(float[] quantiles) {
    assert quantiles.length == 2;
    assert quantiles[1] >= quantiles[0];
    this.quantiles = quantiles;
    this.alpha = (quantiles[1] - quantiles[0]) / 127f;
    this.offset = quantiles[0];
  }

  public byte[] quantize(float[] vector) {
    byte[] q = new byte[vector.length];
    for (int i = 0; i < vector.length; i++) {
      q[i] = (byte) Math.max(-128f, Math.min((vector[i] - offset) / alpha, 127f));
    }
    return q;
  }

  public float calculateVectorOffset(byte[] vector, VectorSimilarityFunction similarityFunction) {
    if (similarityFunction != VectorSimilarityFunction.EUCLIDEAN) {
      int sum = 0;
      for (byte b : vector) {
        sum += b;
      }
      return sum * getAlpha() * getOffset();
    }
    return 0f;
  }

  public void quantizeTo(float[] vector, byte[] output) {
    assert vector.length == output.length;
    for (int i = 0; i < vector.length; i++) {
      output[i] = (byte) Math.max(-128f, Math.min((vector[i] - offset) / alpha, 127f));
    }
  }

  public float getLowerQuantile() {
    return quantiles[0];
  }

  public float getUpperQuantile() {
    return quantiles[1];
  }

  public float getAlpha() {
    return alpha;
  }

  public float getOffset() {
    return offset;
  }

  public float globalVectorOffset(int dim) {
    return offset * offset * dim;
  }

  private static final Random random = new Random(42);
  public static ScalarQuantizer fromVectors(FloatVectorValues floatVectorValues, int quantile) throws IOException {
    if (floatVectorValues.size() == 0) {
      return new ScalarQuantizer(new float[]{0f, 0f});
    }
    if (quantile == 100) {
      float min = Float.POSITIVE_INFINITY;
      float max = Float.NEGATIVE_INFINITY;
      while (floatVectorValues.nextDoc() != NO_MORE_DOCS) {
        for (float v : floatVectorValues.vectorValue()) {
          min = Math.min(min, v);
          max = Math.max(max, v);
        }
      }
      return new ScalarQuantizer(new float[]{min, max});
    }
    int dim = floatVectorValues.dimension();
    if (floatVectorValues.size() < SCALAR_QUANTIZATION_SAMPLE_SIZE) {
      int copyOffset = 0;
      float[] values = new float[floatVectorValues.size()];
      while (floatVectorValues.nextDoc() != NO_MORE_DOCS) {
        float[] floatVector = floatVectorValues.vectorValue();
        System.arraycopy(floatVector, 0, values, copyOffset, floatVector.length);
        copyOffset += dim;
      }
      return new ScalarQuantizer(getUpperAndLowerQuantile(values, quantile));
    }
    int numFloatVecs = floatVectorValues.size();
    // Reservoir sample the vector ordinals we want to read
    float[] values = new float[SCALAR_QUANTIZATION_SAMPLE_SIZE * dim];
    int[] vectorsToTake = IntStream.range(0, SCALAR_QUANTIZATION_SAMPLE_SIZE).toArray();
    int curIndex = SCALAR_QUANTIZATION_SAMPLE_SIZE + 1;
    while (curIndex < numFloatVecs) {
      int j = random.nextInt(curIndex);
      if (j < vectorsToTake.length) {
        vectorsToTake[j] = curIndex;
      }
      curIndex++;
    }
    Arrays.sort(vectorsToTake);
    int copyOffset = 0;
    for (int i : vectorsToTake) {
      int docId = floatVectorValues.advance(i);
      assert docId != NO_MORE_DOCS;
      float[] floatVector = floatVectorValues.vectorValue();
      System.arraycopy(floatVector, 0, values, copyOffset, floatVector.length);
      copyOffset += dim;
    }
    return new ScalarQuantizer(getUpperAndLowerQuantile(values, quantile));
  }

  /**
   * Takes an array of floats, sorted or not, and returns a minimum and maximum value. These values
   * are such that they reside on the `(1 - quantile)/2` and `quantile/2` percentiles. Example:
   * providing floats `[0..100]` and asking for `90` quantiles will return `5` and `95`.
   *
   * @param arr array of floats
   * @param quantile the configured quantile
   * @return lower and upper quantile values
   */
  static float[] getUpperAndLowerQuantile(float[] arr, int quantile) {
    float quantileFloat = quantile / 100f;
    int selectorIndex = (int) (arr.length * (1f - quantileFloat) / 2f + 0.5f);
    Selector selector = new FloatSelector(arr);
    selector.select(0, arr.length, arr.length - selectorIndex);
    selector.select(0, arr.length - selectorIndex, selectorIndex);
    float min = Float.POSITIVE_INFINITY;
    float max = Float.NEGATIVE_INFINITY;
    for (int i = selectorIndex; i <= arr.length - selectorIndex; i++) {
      min = Math.min(arr[i], min);
      max = Math.max(arr[i], max);
    }
    return new float[] {min, max};
  }

  private static class FloatSelector extends IntroSelector {
    float pivot = Float.NaN;

    private final float[] arr;

    private FloatSelector(float[] arr) {
      this.arr = arr;
    }

    @Override
    protected void setPivot(int i) {
      pivot = arr[i];
    }

    @Override
    protected int comparePivot(int j) {
      return Float.compare(pivot, arr[j]);
    }

    @Override
    protected void swap(int i, int j) {
      final float tmp = arr[i];
      arr[i] = arr[j];
      arr[j] = tmp;
    }
  }
}
