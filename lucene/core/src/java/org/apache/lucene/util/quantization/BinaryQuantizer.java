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

import static org.apache.lucene.index.VectorSimilarityFunction.COSINE;
import static org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.VectorUtil;

/**
 * Quantized that quantizes raw vector values to binary. The algorithm is based on the paper <a
 * href="https://arxiv.org/abs/2405.12497">RaBitQ</a>.
 */
public class BinaryQuantizer {
  private final int discretizedDimensions;

  private final VectorSimilarityFunction similarityFunction;
  private final float sqrtDimensions;

  public BinaryQuantizer(
      int dimensions, int discretizedDimensions, VectorSimilarityFunction similarityFunction) {
    if (dimensions <= 0) {
      throw new IllegalArgumentException("dimensions must be > 0 but was: " + dimensions);
    }
    assert discretizedDimensions % 64 == 0
        : "discretizedDimensions must be a multiple of 64 but was: " + discretizedDimensions;
    this.discretizedDimensions = discretizedDimensions;
    this.similarityFunction = similarityFunction;
    this.sqrtDimensions = (float) Math.sqrt(dimensions);
  }

  BinaryQuantizer(int dimensions, VectorSimilarityFunction similarityFunction) {
    this(dimensions, dimensions, similarityFunction);
  }

  private static void removeSignAndDivide(float[] a, float divisor) {
    for (int i = 0; i < a.length; i++) {
      a[i] = Math.abs(a[i]) / divisor;
    }
  }

  private static float sumAndNormalize(float[] a, float norm) {
    float aDivided = 0f;

    for (int i = 0; i < a.length; i++) {
      aDivided += a[i];
    }

    aDivided = aDivided / norm;
    if (!Float.isFinite(aDivided)) {
      aDivided = 0.8f; // can be anything
    }

    return aDivided;
  }

  private static void packAsBinary(float[] vector, byte[] packedVector) {
    for (int h = 0; h < vector.length; h += 8) {
      byte result = 0;
      int q = 0;
      for (int i = 7; i >= 0; i--) {
        if (vector[h + i] > 0) {
          result |= (byte) (1 << q);
        }
        q++;
      }
      packedVector[h / 8] = result;
    }
  }

  public VectorSimilarityFunction getSimilarity() {
    return this.similarityFunction;
  }

  private record SubspaceOutput(float projection) {}

  private SubspaceOutput generateSubSpace(
      float[] vector, float[] centroid, byte[] quantizedVector) {
    // typically no-op if dimensions/64
    float[] paddedCentroid = BQSpaceUtils.pad(centroid, discretizedDimensions);
    float[] paddedVector = BQSpaceUtils.pad(vector, discretizedDimensions);

    VectorUtil.subtract(paddedVector, paddedCentroid);

    // The inner product between the data vector and the quantized data vector
    float norm = VectorUtil.l2Norm(paddedVector);

    packAsBinary(paddedVector, quantizedVector);

    removeSignAndDivide(paddedVector, sqrtDimensions);
    float projection = sumAndNormalize(paddedVector, norm);

    return new SubspaceOutput(projection);
  }

  record SubspaceOutputMIP(float OOQ, float normOC, float oDotC) {}

  private SubspaceOutputMIP generateSubSpaceMIP(
      float[] vector, float[] centroid, byte[] quantizedVector) {

    // typically no-op if dimensions/64
    float[] paddedCentroid = BQSpaceUtils.pad(centroid, discretizedDimensions);
    float[] paddedVector = BQSpaceUtils.pad(vector, discretizedDimensions);

    float oDotC = VectorUtil.dotProduct(paddedVector, paddedCentroid);
    VectorUtil.subtract(paddedVector, paddedCentroid);

    float normOC = VectorUtil.l2Norm(paddedVector);
    packAsBinary(paddedVector, quantizedVector);
    VectorUtil.divide(paddedVector, normOC); // OmC / norm(OmC)

    float OOQ = computerOOQ(vector.length, paddedVector, quantizedVector);

    return new SubspaceOutputMIP(OOQ, normOC, oDotC);
  }

  private float computerOOQ(int originalLength, float[] normOMinusC, byte[] packedBinaryVector) {
    float OOQ = 0f;
    for (int j = 0; j < originalLength / 8; j++) {
      for (int r = 0; r < 8; r++) {
        int sign = ((packedBinaryVector[j] >> (7 - r)) & 0b00000001);
        OOQ += (normOMinusC[j * 8 + r] * (2 * sign - 1));
      }
    }
    OOQ = OOQ / sqrtDimensions;
    return OOQ;
  }

  private static float[] range(float[] q) {
    float vl = 1e20f;
    float vr = -1e20f;
    for (int i = 0; i < q.length; i++) {
      if (q[i] < vl) {
        vl = q[i];
      }
      if (q[i] > vr) {
        vr = q[i];
      }
    }

    return new float[] {vl, vr};
  }

  /** Results of quantizing a vector for both querying and indexing */
  public record QueryAndIndexResults(float[] indexFeatures, float[] queryFeatures) {}

  /**
   * Quantizes the given vector to both single bits and int4 precision. Also containes two distinct
   * float arrays of corrective factors. For details see the individual methods {@link
   * #quantizeForIndex(float[], byte[], float[])} and {@link #quantizeForQuery(float[], byte[],
   * float[])}.
   *
   * @param vector the vector to quantize
   * @param indexDestination the destination byte array to store the quantized bit vector
   * @param queryDestination the destination byte array to store the quantized int4 vector
   * @param centroid the centroid to use for quantization
   * @return the corrective factors used for scoring error correction.
   */
  public QueryAndIndexResults quantizeQueryAndIndex(
      float[] vector, byte[] indexDestination, byte[] queryDestination, float[] centroid) {
    assert similarityFunction != COSINE || VectorUtil.isUnitVector(vector);
    assert similarityFunction != COSINE || VectorUtil.isUnitVector(centroid);
    assert this.discretizedDimensions == BQSpaceUtils.discretize(vector.length, 64);

    if (this.discretizedDimensions != indexDestination.length * 8) {
      throw new IllegalArgumentException(
          "vector and quantized vector destination must be compatible dimensions: "
              + BQSpaceUtils.discretize(vector.length, 64)
              + " [ "
              + this.discretizedDimensions
              + " ]"
              + "!= "
              + indexDestination.length
              + " * 8");
    }

    if (this.discretizedDimensions != (queryDestination.length * 8) / BQSpaceUtils.B_QUERY) {
      throw new IllegalArgumentException(
          "vector and quantized vector destination must be compatible dimensions: "
              + vector.length
              + " [ "
              + this.discretizedDimensions
              + " ]"
              + "!= ("
              + queryDestination.length
              + " * 8) / "
              + BQSpaceUtils.B_QUERY);
    }

    if (vector.length != centroid.length) {
      throw new IllegalArgumentException(
          "vector and centroid dimensions must be the same: "
              + vector.length
              + "!= "
              + centroid.length);
    }
    vector = ArrayUtil.copyArray(vector);
    // only need distToC for euclidean
    float distToC =
        similarityFunction == EUCLIDEAN ? VectorUtil.squareDistance(vector, centroid) : 0f;
    // only need vdotc for dot-products similarity, but not for euclidean
    float vDotC = similarityFunction != EUCLIDEAN ? VectorUtil.dotProduct(vector, centroid) : 0f;
    VectorUtil.subtract(vector, centroid);
    // both euclidean and dot-product need the norm of the vector, just at different times
    float normVmC = VectorUtil.l2Norm(vector);
    // quantize for index
    packAsBinary(BQSpaceUtils.pad(vector, discretizedDimensions), indexDestination);
    if (similarityFunction != EUCLIDEAN) {
      VectorUtil.divide(vector, normVmC);
    }

    // Quantize for query
    float[] range = range(vector);
    float lower = range[0];
    float upper = range[1];
    // Δ := (𝑣𝑟 − 𝑣𝑙)/(2𝐵𝑞 − 1)
    float width = (upper - lower) / ((1 << BQSpaceUtils.B_QUERY) - 1);

    QuantResult quantResult = quantize(vector, lower, width);
    byte[] byteQuery = quantResult.result();

    // q¯ = Δ · q¯𝑢 + 𝑣𝑙 · 1𝐷
    // q¯ is an approximation of q′  (scalar quantized approximation)
    // FIXME: vectors need to be padded but that's expensive; update transponseBin to deal
    byteQuery = BQSpaceUtils.pad(byteQuery, discretizedDimensions);
    BQSpaceUtils.transposeBin(byteQuery, discretizedDimensions, queryDestination);
    final float[] indexCorrections;
    final float[] queryCorrections;
    if (similarityFunction == EUCLIDEAN) {
      indexCorrections = new float[2];
      indexCorrections[0] = (float) Math.sqrt(distToC);
      removeSignAndDivide(vector, sqrtDimensions);
      indexCorrections[1] = sumAndNormalize(vector, normVmC);
      queryCorrections = new float[] {distToC, lower, width, quantResult.quantizedSum};
    } else {
      indexCorrections = new float[3];
      indexCorrections[0] = computerOOQ(vector.length, vector, indexDestination);
      indexCorrections[1] = normVmC;
      indexCorrections[2] = vDotC;
      queryCorrections = new float[] {lower, width, normVmC, vDotC, quantResult.quantizedSum};
    }
    return new QueryAndIndexResults(indexCorrections, queryCorrections);
  }

  /**
   * Quantizes the given vector to single bits and returns an array of corrective factors. For the
   * dot-product family of distances, the corrective terms are, in order
   *
   * <ul>
   *   <li>the dot-product of the normalized, centered vector with its binarized self
   *   <li>the norm of the centered vector
   *   <li>the dot-product of the vector with the centroid
   * </ul>
   *
   * For euclidean:
   *
   * <ul>
   *   <li>The euclidean distance to the centroid
   *   <li>The sum of the dimensions divided by the vector norm
   * </ul>
   *
   * @param vector the vector to quantize
   * @param destination the destination byte array to store the quantized vector
   * @param centroid the centroid to use for quantization
   * @return the corrective factors used for scoring error correction.
   */
  public float[] quantizeForIndex(float[] vector, byte[] destination, float[] centroid) {
    assert similarityFunction != COSINE || VectorUtil.isUnitVector(vector);
    assert similarityFunction != COSINE || VectorUtil.isUnitVector(centroid);
    assert this.discretizedDimensions == BQSpaceUtils.discretize(vector.length, 64);

    if (this.discretizedDimensions != destination.length * 8) {
      throw new IllegalArgumentException(
          "vector and quantized vector destination must be compatible dimensions: "
              + BQSpaceUtils.discretize(vector.length, 64)
              + " [ "
              + this.discretizedDimensions
              + " ]"
              + "!= "
              + destination.length
              + " * 8");
    }

    if (vector.length != centroid.length) {
      throw new IllegalArgumentException(
          "vector and centroid dimensions must be the same: "
              + vector.length
              + "!= "
              + centroid.length);
    }

    float[] corrections;

    vector = ArrayUtil.copyArray(vector);

    switch (similarityFunction) {
      case EUCLIDEAN:
        float distToCentroid = (float) Math.sqrt(VectorUtil.squareDistance(vector, centroid));

        SubspaceOutput subspaceOutput = generateSubSpace(vector, centroid, destination);
        corrections = new float[2];
        corrections[0] = distToCentroid;
        corrections[1] = subspaceOutput.projection();
        break;
      case MAXIMUM_INNER_PRODUCT:
      case COSINE:
      case DOT_PRODUCT:
        SubspaceOutputMIP subspaceOutputMIP = generateSubSpaceMIP(vector, centroid, destination);
        corrections = new float[3];
        corrections[0] = subspaceOutputMIP.OOQ();
        corrections[1] = subspaceOutputMIP.normOC();
        corrections[2] = subspaceOutputMIP.oDotC();
        break;
      default:
        throw new UnsupportedOperationException(
            "Unsupported similarity function: " + similarityFunction);
    }

    return corrections;
  }

  private record QuantResult(byte[] result, int quantizedSum) {}

  private static QuantResult quantize(float[] vector, float lower, float width) {
    // FIXME: speed up with panama? and/or use existing scalar quantization utils in Lucene?
    byte[] result = new byte[vector.length];
    float oneOverWidth = 1.0f / width;
    int sumQ = 0;
    for (int i = 0; i < vector.length; i++) {
      byte res = (byte) ((vector[i] - lower) * oneOverWidth);
      result[i] = res;
      sumQ += res;
    }

    return new QuantResult(result, sumQ);
  }

  /**
   * Quantizes the given vector to int4 precision and returns an array of corrective factors.
   *
   * <p>Corrective factors are used for scoring error correction. For the dot-product family of
   *
   * <ul>
   *   <li>The lower bound for the int4 quantized vector
   *   <li>The width for int4 quantized vector
   *   <li>The norm of the centroid centered vector
   *   <li>The dot-product of the vector with the centroid
   *   <li>The sum of the quantized dimensions
   * </ul>
   *
   * For euclidean:
   *
   * <ul>
   *   <li>The euclidean distance to the centroid
   *   <li>The lower bound for the int4 quantized vector
   *   <li>The width for int4 quantized vector
   *   <li>The sum of the quantized dimensions
   * </ul>
   *
   * @param vector the vector to quantize
   * @param destination the destination byte array to store the quantized vector
   * @param centroid the centroid to use for quantization
   * @return the corrective factors used for scoring error correction.
   */
  public float[] quantizeForQuery(float[] vector, byte[] destination, float[] centroid) {
    assert similarityFunction != COSINE || VectorUtil.isUnitVector(vector);
    assert similarityFunction != COSINE || VectorUtil.isUnitVector(centroid);
    assert this.discretizedDimensions == BQSpaceUtils.discretize(vector.length, 64);

    if (this.discretizedDimensions != (destination.length * 8) / BQSpaceUtils.B_QUERY) {
      throw new IllegalArgumentException(
          "vector and quantized vector destination must be compatible dimensions: "
              + vector.length
              + " [ "
              + this.discretizedDimensions
              + " ]"
              + "!= ("
              + destination.length
              + " * 8) / "
              + BQSpaceUtils.B_QUERY);
    }

    if (vector.length != centroid.length) {
      throw new IllegalArgumentException(
          "vector and centroid dimensions must be the same: "
              + vector.length
              + "!= "
              + centroid.length);
    }

    // FIXME: make a copy of vector so we don't overwrite it here?
    //  ... (could subtractInPlace but the passed vector is modified) <<---
    float[] vmC = ArrayUtil.copyArray(vector);
    VectorUtil.subtract(vmC, centroid);

    // FIXME: should other similarity functions behave like MIP on query like COSINE
    float normVmC = 0f;
    if (similarityFunction != EUCLIDEAN) {
      normVmC = VectorUtil.l2Norm(vmC);
      VectorUtil.divide(vmC, normVmC);
    }
    float[] range = range(vmC);
    float lower = range[0];
    float upper = range[1];
    // Δ := (𝑣𝑟 − 𝑣𝑙)/(2𝐵𝑞 − 1)
    float width = (upper - lower) / ((1 << BQSpaceUtils.B_QUERY) - 1);

    QuantResult quantResult = quantize(vmC, lower, width);
    byte[] byteQuery = quantResult.result();

    // q¯ = Δ · q¯𝑢 + 𝑣𝑙 · 1𝐷
    // q¯ is an approximation of q′  (scalar quantized approximation)
    // FIXME: vectors need to be padded but that's expensive; update transponseBin to deal
    byteQuery = BQSpaceUtils.pad(byteQuery, discretizedDimensions);
    BQSpaceUtils.transposeBin(byteQuery, discretizedDimensions, destination);

    final float[] corrections;
    if (similarityFunction != EUCLIDEAN) {
      float vDotC = VectorUtil.dotProduct(vector, centroid);
      corrections = new float[] {lower, width, normVmC, vDotC, quantResult.quantizedSum};
    } else {
      float distToCentroid = (float) Math.sqrt(VectorUtil.squareDistance(vector, centroid));
      corrections = new float[] {distToCentroid, lower, width, quantResult.quantizedSum};
    }

    return corrections;
  }
}
