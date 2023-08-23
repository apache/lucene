package org.apache.lucene.util;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.stream.IntStream;
import org.apache.lucene.index.FloatVectorValues;

public class ScalarQuantizer {

  public static final int SCALAR_QUANTIZATION_SAMPLE_SIZE = 100_000;

  private final float alpha;
  private final float offset;

  public ScalarQuantizer(float alpha, float offset) {
    this.alpha = alpha;
    this.offset = offset;
  }

  byte[] quantize(float[] vector) {
    byte[] q = new byte[vector.length];
    for (int i = 0; i < vector.length; i++) {
      q[i] = (byte) Math.max(-128f, Math.min((vector[i] - offset) / alpha, 127f));
    }
    return q;
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

  public static class Builder {
    private final FloatVectorValues floatVectorValues;
    private final int quantile;
    private final Random random = new Random(42);

    public Builder(FloatVectorValues floatVectorValues, int quantile) {
      this.floatVectorValues = floatVectorValues;
      this.quantile = quantile;
    }

    ScalarQuantizer build(int dim) throws IOException {
      if (quantile == 100) {
        float min = Float.POSITIVE_INFINITY;
        float max = Float.NEGATIVE_INFINITY;
        while (floatVectorValues.nextDoc() != NO_MORE_DOCS) {
          for (float v : floatVectorValues.vectorValue()) {
            if (v < min) {
              min = v;
            }
            if (v > max) {
              max = v;
            }
          }
        }
        return new ScalarQuantizer((max - min) / 127f, min);
      }
      int numFloatVecs = floatVectorValues.size();
      int numVecs = Math.min(floatVectorValues.size(), SCALAR_QUANTIZATION_SAMPLE_SIZE);
      float[] values = new float[numVecs * dim];
      int[] vectorsToTake = IntStream.range(0, numVecs).toArray();
      int curIndex = numVecs + 1;
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
      float[] upperAndLower = getUpperAndLowerQuantile(values, quantile);
      return new ScalarQuantizer((upperAndLower[1] - upperAndLower[0]) / 127f, upperAndLower[0]);
    }
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
