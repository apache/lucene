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
package org.apache.lucene.util.quantization;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.HitQueue;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.util.IntroSelector;
import org.apache.lucene.util.Selector;

/**
 * Will scalar quantize float vectors into `int8` byte values. This is a lossy transformation.
 * Scalar quantization works by first calculating the quantiles of the float vector values. The
 * quantiles are calculated using the configured confidence interval. The [minQuantile, maxQuantile]
 * are then used to scale the values into the range [0, 127] and bucketed into the nearest byte
 * values.
 *
 * <h2>How Scalar Quantization Works</h2>
 *
 * <p>The basic mathematical equations behind this are fairly straight forward and based on min/max
 * normalization. Given a float vector `v` and a confidenceInterval `q` we can calculate the
 * quantiles of the vector values [minQuantile, maxQuantile].
 *
 * <pre class="prettyprint">
 *   byte = (float - minQuantile) * 127/(maxQuantile - minQuantile)
 *   float = (maxQuantile - minQuantile)/127 * byte + minQuantile
 * </pre>
 *
 * <p>This then means to multiply two float values together (e.g. dot_product) we can do the
 * following:
 *
 * <pre class="prettyprint">
 *   float1 * float2 ~= (byte1 * (maxQuantile - minQuantile)/127 + minQuantile) * (byte2 * (maxQuantile - minQuantile)/127 + minQuantile)
 *   float1 * float2 ~= (byte1 * byte2 * (maxQuantile - minQuantile)^2)/(127^2) + (byte1 * minQuantile * (maxQuantile - minQuantile)/127) + (byte2 * minQuantile * (maxQuantile - minQuantile)/127) + minQuantile^2
 *   let alpha = (maxQuantile - minQuantile)/127
 *   float1 * float2 ~= (byte1 * byte2 * alpha^2) + (byte1 * minQuantile * alpha) + (byte2 * minQuantile * alpha) + minQuantile^2
 * </pre>
 *
 * <p>The expansion for square distance is much simpler:
 *
 * <pre class="prettyprint">
 *  square_distance = (float1 - float2)^2
 *  (float1 - float2)^2 ~= (byte1 * alpha + minQuantile - byte2 * alpha - minQuantile)^2
 *  = (alpha*byte1 + minQuantile)^2 + (alpha*byte2 + minQuantile)^2 - 2*(alpha*byte1 + minQuantile)(alpha*byte2 + minQuantile)
 *  this can be simplified to:
 *  = alpha^2 (byte1 - byte2)^2
 * </pre>
 */
public class ScalarQuantizer {

  public static final int SCALAR_QUANTIZATION_SAMPLE_SIZE = 25_000;
  // 20*dimension provides protection from extreme confidence intervals
  // and also prevents humongous allocations
  static final int SCRATCH_SIZE = 20;

  private final float alpha;
  private final float scale;
  private final byte bits;
  private final float minQuantile, maxQuantile;

  /**
   * @param minQuantile the lower quantile of the distribution
   * @param maxQuantile the upper quantile of the distribution
   * @param bits the number of bits to use for quantization
   */
  public ScalarQuantizer(float minQuantile, float maxQuantile, byte bits) {
    if (Float.isNaN(minQuantile)
        || Float.isInfinite(minQuantile)
        || Float.isNaN(maxQuantile)
        || Float.isInfinite(maxQuantile)) {
      throw new IllegalStateException("Scalar quantizer does not support infinite or NaN values");
    }
    assert maxQuantile >= minQuantile;
    assert bits > 0 && bits <= 8;
    this.minQuantile = minQuantile;
    this.maxQuantile = maxQuantile;
    this.bits = bits;
    final float divisor = (float) ((1 << bits) - 1);
    this.scale = divisor / (maxQuantile - minQuantile);
    this.alpha = (maxQuantile - minQuantile) / divisor;
  }

  /**
   * Quantize a float vector into a byte vector
   *
   * @param src the source vector
   * @param dest the destination vector
   * @param similarityFunction the similarity function used to calculate the quantile
   * @return the corrective offset that needs to be applied to the score
   */
  public float quantize(float[] src, byte[] dest, VectorSimilarityFunction similarityFunction) {
    assert src.length == dest.length;
    float correction = 0;
    for (int i = 0; i < src.length; i++) {
      correction += quantizeFloat(src[i], dest, i);
    }
    if (similarityFunction.equals(VectorSimilarityFunction.EUCLIDEAN)) {
      return 0;
    }
    return correction;
  }

  private float quantizeFloat(float v, byte[] dest, int destIndex) {
    assert dest == null || destIndex < dest.length;
    // Make sure the value is within the quantile range, cutting off the tails
    // see first parenthesis in equation: byte = (float - minQuantile) * 127/(maxQuantile -
    // minQuantile)
    float dx = v - minQuantile;
    float dxc = Math.max(minQuantile, Math.min(maxQuantile, v)) - minQuantile;
    // Scale the value to the range [0, 127], this is our quantized value
    // scale = 127/(maxQuantile - minQuantile)
    float dxs = scale * dxc;
    // We multiply by `alpha` here to get the quantized value back into the original range
    // to aid in calculating the corrective offset
    float dxq = Math.round(dxs) * alpha;
    if (dest != null) {
      dest[destIndex] = (byte) Math.round(dxs);
    }
    // Calculate the corrective offset that needs to be applied to the score
    // in addition to the `byte * minQuantile * alpha` term in the equation
    // we add the `(dx - dxq) * dxq` term to account for the fact that the quantized value
    // will be rounded to the nearest whole number and lose some accuracy
    // Additionally, we account for the global correction of `minQuantile^2` in the equation
    return minQuantile * (v - minQuantile / 2.0F) + (dx - dxq) * dxq;
  }

  /**
   * Recalculate the old score corrective value given new current quantiles
   *
   * @param quantizedVector the old vector
   * @param oldQuantizer the old quantizer
   * @param similarityFunction the similarity function used to calculate the quantile
   * @return the new offset
   */
  public float recalculateCorrectiveOffset(
      byte[] quantizedVector,
      ScalarQuantizer oldQuantizer,
      VectorSimilarityFunction similarityFunction) {
    if (similarityFunction.equals(VectorSimilarityFunction.EUCLIDEAN)) {
      return 0f;
    }
    float correctiveOffset = 0f;
    for (int i = 0; i < quantizedVector.length; i++) {
      // dequantize the old value in order to recalculate the corrective offset
      float v = (oldQuantizer.alpha * quantizedVector[i]) + oldQuantizer.minQuantile;
      correctiveOffset += quantizeFloat(v, null, 0);
    }
    return correctiveOffset;
  }

  /**
   * Dequantize a byte vector into a float vector
   *
   * @param src the source vector
   * @param dest the destination vector
   */
  void deQuantize(byte[] src, float[] dest) {
    assert src.length == dest.length;
    for (int i = 0; i < src.length; i++) {
      dest[i] = (alpha * src[i]) + minQuantile;
    }
  }

  public float getLowerQuantile() {
    return minQuantile;
  }

  public float getUpperQuantile() {
    return maxQuantile;
  }

  public float getConstantMultiplier() {
    return alpha * alpha;
  }

  public byte getBits() {
    return bits;
  }

  @Override
  public String toString() {
    return "ScalarQuantizer{"
        + "minQuantile="
        + minQuantile
        + ", maxQuantile="
        + maxQuantile
        + ", bits="
        + bits
        + '}';
  }

  private static final Random random = new Random(42);

  private static int[] reservoirSampleIndices(int numFloatVecs, int sampleSize) {
    int[] vectorsToTake = IntStream.range(0, sampleSize).toArray();
    for (int i = sampleSize; i < numFloatVecs; i++) {
      int j = random.nextInt(i + 1);
      if (j < sampleSize) {
        vectorsToTake[j] = i;
      }
    }
    Arrays.sort(vectorsToTake);
    return vectorsToTake;
  }

  /**
   * This will read the float vector values and calculate the quantiles. If the number of float
   * vectors is less than {@link #SCALAR_QUANTIZATION_SAMPLE_SIZE} then all the values will be read
   * and the quantiles calculated. If the number of float vectors is greater than {@link
   * #SCALAR_QUANTIZATION_SAMPLE_SIZE} then a random sample of {@link
   * #SCALAR_QUANTIZATION_SAMPLE_SIZE} will be read and the quantiles calculated.
   *
   * @param floatVectorValues the float vector values from which to calculate the quantiles
   * @param confidenceInterval the confidence interval used to calculate the quantiles
   * @param totalVectorCount the total number of live float vectors in the index. This is vital for
   *     accounting for deleted documents when calculating the quantiles.
   * @param bits the number of bits to use for quantization
   * @return A new {@link ScalarQuantizer} instance
   * @throws IOException if there is an error reading the float vector values
   */
  public static ScalarQuantizer fromVectors(
      FloatVectorValues floatVectorValues,
      float confidenceInterval,
      int totalVectorCount,
      byte bits)
      throws IOException {
    return fromVectors(
        floatVectorValues,
        confidenceInterval,
        totalVectorCount,
        bits,
        SCALAR_QUANTIZATION_SAMPLE_SIZE);
  }

  static ScalarQuantizer fromVectors(
      FloatVectorValues floatVectorValues,
      float confidenceInterval,
      int totalVectorCount,
      byte bits,
      int quantizationSampleSize)
      throws IOException {
    assert 0.9f <= confidenceInterval && confidenceInterval <= 1f;
    assert quantizationSampleSize > SCRATCH_SIZE;
    if (totalVectorCount == 0) {
      return new ScalarQuantizer(0f, 0f, bits);
    }
    if (confidenceInterval == 1f) {
      float min = Float.POSITIVE_INFINITY;
      float max = Float.NEGATIVE_INFINITY;
      while (floatVectorValues.nextDoc() != NO_MORE_DOCS) {
        for (float v : floatVectorValues.vectorValue()) {
          min = Math.min(min, v);
          max = Math.max(max, v);
        }
      }
      return new ScalarQuantizer(min, max, bits);
    }
    final float[] quantileGatheringScratch =
        new float[floatVectorValues.dimension() * Math.min(SCRATCH_SIZE, totalVectorCount)];
    int count = 0;
    double[] upperSum = new double[1];
    double[] lowerSum = new double[1];
    float[] confidenceIntervals = new float[] {confidenceInterval};
    if (totalVectorCount <= quantizationSampleSize) {
      int scratchSize = Math.min(SCRATCH_SIZE, totalVectorCount);
      int i = 0;
      while (floatVectorValues.nextDoc() != NO_MORE_DOCS) {
        float[] vectorValue = floatVectorValues.vectorValue();
        System.arraycopy(
            vectorValue, 0, quantileGatheringScratch, i * vectorValue.length, vectorValue.length);
        i++;
        if (i == scratchSize) {
          extractQuantiles(confidenceIntervals, quantileGatheringScratch, upperSum, lowerSum);
          i = 0;
          count++;
        }
      }
      // Note, we purposefully don't use the rest of the scratch state if we have fewer than
      // `SCRATCH_SIZE` vectors, mainly because if we are sampling so few vectors then we don't
      // want to be adversely affected by the extreme confidence intervals over small sample sizes
      return new ScalarQuantizer((float) lowerSum[0] / count, (float) upperSum[0] / count, bits);
    }
    int[] vectorsToTake = reservoirSampleIndices(totalVectorCount, quantizationSampleSize);
    int index = 0;
    int idx = 0;
    for (int i : vectorsToTake) {
      while (index <= i) {
        // We cannot use `advance(docId)` as MergedVectorValues does not support it
        floatVectorValues.nextDoc();
        index++;
      }
      assert floatVectorValues.docID() != NO_MORE_DOCS;
      float[] vectorValue = floatVectorValues.vectorValue();
      System.arraycopy(
          vectorValue, 0, quantileGatheringScratch, idx * vectorValue.length, vectorValue.length);
      idx++;
      if (idx == SCRATCH_SIZE) {
        extractQuantiles(confidenceIntervals, quantileGatheringScratch, upperSum, lowerSum);
        count++;
        idx = 0;
      }
    }
    return new ScalarQuantizer((float) lowerSum[0] / count, (float) upperSum[0] / count, bits);
  }

  public static ScalarQuantizer fromVectorsAutoInterval(
      FloatVectorValues floatVectorValues,
      VectorSimilarityFunction function,
      int totalVectorCount,
      byte bits)
      throws IOException {
    if (totalVectorCount == 0) {
      return new ScalarQuantizer(0f, 0f, bits);
    }

    int sampleSize = Math.min(totalVectorCount, 1000);
    final float[] quantileGatheringScratch =
        new float[floatVectorValues.dimension() * Math.min(SCRATCH_SIZE, totalVectorCount)];
    int count = 0;
    double[] upperSum = new double[2];
    double[] lowerSum = new double[2];
    final List<float[]> sampledDocs = new ArrayList<>(sampleSize);
    float[] confidenceIntervals =
        new float[] {
          1
              - Math.min(32, floatVectorValues.dimension() / 10f)
                  / (floatVectorValues.dimension() + 1),
          1 - 1f / (floatVectorValues.dimension() + 1)
        };
    if (totalVectorCount <= sampleSize) {
      int scratchSize = Math.min(SCRATCH_SIZE, totalVectorCount);
      int i = 0;
      while (floatVectorValues.nextDoc() != NO_MORE_DOCS) {
        gatherSample(floatVectorValues, quantileGatheringScratch, sampledDocs, i);
        i++;
        if (i == scratchSize) {
          extractQuantiles(confidenceIntervals, quantileGatheringScratch, upperSum, lowerSum);
          i = 0;
          count++;
        }
      }
    } else {
      // Reservoir sample the vector ordinals we want to read
      int[] vectorsToTake = reservoirSampleIndices(totalVectorCount, 1000);
      // TODO make this faster by .advance()ing & dual iterator
      int index = 0;
      int idx = 0;
      for (int i : vectorsToTake) {
        while (index <= i) {
          // We cannot use `advance(docId)` as MergedVectorValues does not support it
          floatVectorValues.nextDoc();
          index++;
        }
        assert floatVectorValues.docID() != NO_MORE_DOCS;
        gatherSample(floatVectorValues, quantileGatheringScratch, sampledDocs, idx);
        idx++;
        if (idx == SCRATCH_SIZE) {
          extractQuantiles(confidenceIntervals, quantileGatheringScratch, upperSum, lowerSum);
          count++;
          idx = 0;
        }
      }
    }

    // Here we gather the upper and lower bounds for the quantile grid search
    final float al = (float) lowerSum[1] / count;
    final float bu = (float) upperSum[1] / count;
    final float au = (float) lowerSum[0] / count;
    final float bl = (float) upperSum[0] / count;
    if (Float.isNaN(al)
        || Float.isInfinite(al)
        || Float.isNaN(au)
        || Float.isInfinite(au)
        || Float.isNaN(bl)
        || Float.isInfinite(bl)
        || Float.isNaN(bu)
        || Float.isInfinite(bu)) {
      throw new IllegalStateException("Quantile calculation resulted in NaN or infinite values");
    }
    final float[] lowerCandidates = new float[16];
    final float[] upperCandidates = new float[16];
    int idx = 0;
    for (float i = 0f; i < 32f; i += 2f) {
      lowerCandidates[idx] = al + i * (au - al) / 32f;
      upperCandidates[idx] = bl + i * (bu - bl) / 32f;
      idx++;
    }
    // Now we need to find the best candidate pair by correlating the true quantized nearest
    // neighbor scores
    // with the float vector scores
    List<ScoreDocsAndScoreVariance> nearestNeighbors = findNearestNeighbors(sampledDocs, function);
    float[] bestPair =
        candidateGridSearch(
            nearestNeighbors, sampledDocs, lowerCandidates, upperCandidates, function, bits);
    return new ScalarQuantizer(bestPair[0], bestPair[1], bits);
  }

  private static void extractQuantiles(
      float[] confidenceIntervals,
      float[] quantileGatheringScratch,
      double[] upperSum,
      double[] lowerSum) {
    assert confidenceIntervals.length == upperSum.length
        && confidenceIntervals.length == lowerSum.length;
    for (int i = 0; i < confidenceIntervals.length; i++) {
      float[] upperAndLower =
          getUpperAndLowerQuantile(quantileGatheringScratch, confidenceIntervals[i]);
      upperSum[i] += upperAndLower[1];
      lowerSum[i] += upperAndLower[0];
    }
  }

  private static void gatherSample(
      FloatVectorValues floatVectorValues,
      float[] quantileGatheringScratch,
      List<float[]> sampledDocs,
      int i)
      throws IOException {
    float[] vectorValue = floatVectorValues.vectorValue();
    float[] copy = new float[vectorValue.length];
    System.arraycopy(vectorValue, 0, copy, 0, vectorValue.length);
    sampledDocs.add(copy);
    System.arraycopy(
        vectorValue, 0, quantileGatheringScratch, i * vectorValue.length, vectorValue.length);
  }

  private static float[] candidateGridSearch(
      List<ScoreDocsAndScoreVariance> nearestNeighbors,
      List<float[]> vectors,
      float[] lowerCandidates,
      float[] upperCandidates,
      VectorSimilarityFunction function,
      byte bits) {
    double maxCorr = Double.NEGATIVE_INFINITY;
    float bestLower = 0f;
    float bestUpper = 0f;
    ScoreErrorCorrelator scoreErrorCorrelator =
        new ScoreErrorCorrelator(function, nearestNeighbors, vectors, bits);
    // first do a coarse grained search to find the initial best candidate pair
    int bestQuandrantLower = 0;
    int bestQuandrantUpper = 0;
    for (int i = 0; i < lowerCandidates.length; i += 4) {
      float lower = lowerCandidates[i];
      if (Float.isNaN(lower) || Float.isInfinite(lower)) {
        assert false : "Lower candidate is NaN or infinite";
        continue;
      }
      for (int j = 0; j < upperCandidates.length; j += 4) {
        float upper = upperCandidates[j];
        if (Float.isNaN(upper) || Float.isInfinite(upper)) {
          assert false : "Upper candidate is NaN or infinite";
          continue;
        }
        if (upper <= lower) {
          continue;
        }
        double mean = scoreErrorCorrelator.scoreErrorCorrelation(lower, upper);
        if (mean > maxCorr) {
          maxCorr = mean;
          bestLower = lower;
          bestUpper = upper;
          bestQuandrantLower = i;
          bestQuandrantUpper = j;
        }
      }
    }
    // Now search within the best quadrant
    for (int i = bestQuandrantLower + 1; i < bestQuandrantLower + 4; i++) {
      for (int j = bestQuandrantUpper + 1; j < bestQuandrantUpper + 4; j++) {
        float lower = lowerCandidates[i];
        float upper = upperCandidates[j];
        if (Float.isNaN(lower)
            || Float.isInfinite(lower)
            || Float.isNaN(upper)
            || Float.isInfinite(upper)) {
          assert false : "Lower or upper candidate is NaN or infinite";
          continue;
        }
        if (upper <= lower) {
          continue;
        }
        double mean = scoreErrorCorrelator.scoreErrorCorrelation(lower, upper);
        if (mean > maxCorr) {
          maxCorr = mean;
          bestLower = lower;
          bestUpper = upper;
        }
      }
    }
    return new float[] {bestLower, bestUpper};
  }

  /**
   * @param vectors The vectors to find the nearest neighbors for each other
   * @param similarityFunction The similarity function to use
   * @return The top 10 nearest neighbors for each vector from the vectors list
   */
  private static List<ScoreDocsAndScoreVariance> findNearestNeighbors(
      List<float[]> vectors, VectorSimilarityFunction similarityFunction) {
    List<HitQueue> queues = new ArrayList<>(vectors.size());
    queues.add(new HitQueue(10, false));
    for (int i = 0; i < vectors.size(); i++) {
      float[] vector = vectors.get(i);
      for (int j = i + 1; j < vectors.size(); j++) {
        float[] otherVector = vectors.get(j);
        float score = similarityFunction.compare(vector, otherVector);
        // initialize the rest of the queues
        if (queues.size() <= j) {
          queues.add(new HitQueue(10, false));
        }
        queues.get(i).insertWithOverflow(new ScoreDoc(j, score));
        queues.get(j).insertWithOverflow(new ScoreDoc(i, score));
      }
    }
    // Extract the top 10 from each queue
    List<ScoreDocsAndScoreVariance> result = new ArrayList<>(vectors.size());
    OnlineMeanAndVar meanAndVar = new OnlineMeanAndVar();
    for (int i = 0; i < vectors.size(); i++) {
      HitQueue queue = queues.get(i);
      ScoreDoc[] scoreDocs = new ScoreDoc[queue.size()];
      for (int j = queue.size() - 1; j >= 0; j--) {
        scoreDocs[j] = queue.pop();
        assert scoreDocs[j] != null;
        meanAndVar.add(scoreDocs[j].score);
      }
      result.add(new ScoreDocsAndScoreVariance(scoreDocs, meanAndVar.var()));
      meanAndVar.reset();
    }
    return result;
  }

  /**
   * Takes an array of floats, sorted or not, and returns a minimum and maximum value. These values
   * are such that they reside on the `(1 - confidenceInterval)/2` and `confidenceInterval/2`
   * percentiles. Example: providing floats `[0..100]` and asking for `90` quantiles will return `5`
   * and `95`.
   *
   * @param arr array of floats
   * @param confidenceInterval the configured confidence interval
   * @return lower and upper quantile values
   */
  static float[] getUpperAndLowerQuantile(float[] arr, float confidenceInterval) {
    assert arr.length > 0;
    // If we have 1 or 2 values, we can't calculate the quantiles, simply return the min and max
    if (arr.length <= 2) {
      Arrays.sort(arr);
      return new float[] {arr[0], arr[arr.length - 1]};
    }
    int selectorIndex = (int) (arr.length * (1f - confidenceInterval) / 2f + 0.5f);
    if (selectorIndex > 0) {
      Selector selector = new FloatSelector(arr);
      selector.select(0, arr.length, arr.length - selectorIndex);
      selector.select(0, arr.length - selectorIndex, selectorIndex);
    }
    float min = Float.POSITIVE_INFINITY;
    float max = Float.NEGATIVE_INFINITY;
    for (int i = selectorIndex; i < arr.length - selectorIndex; i++) {
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

  private static class ScoreDocsAndScoreVariance {
    private final ScoreDoc[] scoreDocs;
    private final float scoreVariance;

    public ScoreDocsAndScoreVariance(ScoreDoc[] scoreDocs, float scoreVariance) {
      this.scoreDocs = scoreDocs;
      this.scoreVariance = scoreVariance;
    }

    public ScoreDoc[] getScoreDocs() {
      return scoreDocs;
    }
  }

  private static class OnlineMeanAndVar {
    private double mean = 0.0;
    private double var = 0.0;
    private int n = 0;

    void reset() {
      mean = 0.0;
      var = 0.0;
      n = 0;
    }

    void add(double x) {
      n++;
      double delta = x - mean;
      mean += delta / n;
      var += delta * (x - mean);
    }

    float var() {
      return (float) (var / (n - 1));
    }
  }

  /**
   * This class is used to correlate the scores of the nearest neighbors with the errors in the
   * scores. This is used to find the best quantile pair for the scalar quantizer.
   */
  private static class ScoreErrorCorrelator {
    private final OnlineMeanAndVar corr = new OnlineMeanAndVar();
    private final OnlineMeanAndVar errors = new OnlineMeanAndVar();
    private final VectorSimilarityFunction function;
    private final List<ScoreDocsAndScoreVariance> nearestNeighbors;
    private final List<float[]> vectors;
    private final byte[] query;
    private final byte[] vector;
    private final byte bits;

    public ScoreErrorCorrelator(
        VectorSimilarityFunction function,
        List<ScoreDocsAndScoreVariance> nearestNeighbors,
        List<float[]> vectors,
        byte bits) {
      this.function = function;
      this.nearestNeighbors = nearestNeighbors;
      this.vectors = vectors;
      this.query = new byte[vectors.get(0).length];
      this.vector = new byte[vectors.get(0).length];
      this.bits = bits;
    }

    double scoreErrorCorrelation(float lowerQuantile, float upperQuantile) {
      corr.reset();
      ScalarQuantizer quantizer = new ScalarQuantizer(lowerQuantile, upperQuantile, bits);
      ScalarQuantizedVectorSimilarity scalarQuantizedVectorSimilarity =
          ScalarQuantizedVectorSimilarity.fromVectorSimilarity(
              function, quantizer.getConstantMultiplier(), quantizer.bits);
      for (int i = 0; i < nearestNeighbors.size(); i++) {
        float queryCorrection = quantizer.quantize(vectors.get(i), query, function);
        ScoreDocsAndScoreVariance scoreDocsAndScoreVariance = nearestNeighbors.get(i);
        ScoreDoc[] scoreDocs = scoreDocsAndScoreVariance.getScoreDocs();
        float scoreVariance = scoreDocsAndScoreVariance.scoreVariance;
        // calculate the score for the vector against its nearest neighbors but with quantized
        // scores now
        errors.reset();
        for (ScoreDoc scoreDoc : scoreDocs) {
          float vectorCorrection = quantizer.quantize(vectors.get(scoreDoc.doc), vector, function);
          float qScore =
              scalarQuantizedVectorSimilarity.score(
                  query, queryCorrection, vector, vectorCorrection);
          errors.add(qScore - scoreDoc.score);
        }
        corr.add(1 - errors.var() / scoreVariance);
      }
      return Double.isNaN(corr.mean) ? 0.0 : corr.mean;
    }
  }
}
