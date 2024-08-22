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

import java.util.Random;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.VectorUtil;

public class BinaryQuantizer {
  private final int discretizedDimensions;

  // dim floats random numbers sampled from the uniform distribution [0,1]
  private final float[] u;

  private final VectorSimilarityFunction similarityFunction;
  private final float sqrtDimensions;

  public BinaryQuantizer(int dimensions, VectorSimilarityFunction similarityFunction) {
    this.discretizedDimensions = (dimensions + 63) / 64 * 64;
    this.similarityFunction = similarityFunction;
    Random random = new Random(42);
    u = new float[discretizedDimensions];
    for (int i = 0; i < discretizedDimensions; i++) {
      u[i] = (float) random.nextDouble();
    }
    this.sqrtDimensions = (float) Math.sqrt(discretizedDimensions);
  }

  // FIXME: move this out to vector utils?
  private static float[] subset(float[] a, int lastColumn) {
    if (a.length == lastColumn) {
      return a;
    }
    return ArrayUtil.copyOfSubArray(a, 0, lastColumn);
  }

  // FIXME: move this out to vector utils
  //  public static void removeSignAndDivide(float[] a, float divisor) {
  //    // FIXME: revert to old behavior for small dimensions
  //    //            for(int j = 0; j < a[0].length; j++) {
  //    //                a[i][j] = Math.abs(a[i][j]) / divisor;
  //    //            }
  //    int size = a.length / FLOAT_SPECIES.length();
  //    for (int r = 0; r < size; r++) {
  //      int offset = FLOAT_SPECIES.length() * r;
  //      FloatVector va = FloatVector.fromArray(FLOAT_SPECIES, a, offset);
  //      va.abs().div(divisor).intoArray(a, offset);
  //    }
  //
  //    // tail
  //    int remainder = a.length % FLOAT_SPECIES.length();
  //    if (remainder != 0) {
  //      for (int i = a.length - remainder; i < a.length; i++) {
  //        a[i] = Math.abs(a[i]) / divisor;
  //      }
  //    }
  //  }

  // FIXME: move this out to vector utils?
  private static void removeSignAndDivide(float[] a, float divisor) {
    for (int i = 0; i < a.length; i++) {
      a[i] = Math.abs(a[i]) / divisor;
    }
  }

  // FIXME: move this out to vector utils?
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

  private static byte[] packAsBinary(float[] vector, int dimensions) {
    int totalValues = dimensions / 8;

    byte[] allBinary = new byte[totalValues];

    for (int h = 0; h < vector.length; h += 8) {
      byte result = 0;
      int q = 0;
      for (int i = 7; i >= 0; i--) {
        if (vector[h + i] > 0) {
          result |= (byte) (1 << q);
        }
        q++;
      }
      allBinary[h / 8] = result;
    }

    return allBinary;
  }

  private record SubspaceOutput(byte[] packedBinaryVector, float projection) {}

  private SubspaceOutput generateSubSpace(float[] vector, float[] centroid) {
    // typically no-op if dimensions/64
    float[] paddedCentroid = BQVectorUtils.pad(centroid, discretizedDimensions);
    float[] paddedVector = BQVectorUtils.pad(vector, discretizedDimensions);

    BQVectorUtils.subtractInPlace(paddedVector, paddedCentroid);

    // The inner product between the data vector and the quantized data vector
    float norm = BQVectorUtils.norm(paddedVector);

    byte[] packedBinaryVector = packAsBinary(paddedVector, discretizedDimensions);

    paddedVector = subset(paddedVector, discretizedDimensions); // typically no-op if dimensions/64
    removeSignAndDivide(paddedVector, (float) Math.pow(discretizedDimensions, 0.5));
    float projection = sumAndNormalize(paddedVector, norm);

    return new SubspaceOutput(packedBinaryVector, projection);
  }

  record SubspaceOutputMIP(
      byte[] packedBinaryVector, float xbSum, float oDotC, float normOC, float OOQ) {}

  private SubspaceOutputMIP generateSubSpaceMIP(float[] vector, float[] centroid) {

    // typically no-op if dimensions/64
    float[] paddedCentroid = BQVectorUtils.pad(centroid, discretizedDimensions);
    float[] paddedVector = BQVectorUtils.pad(vector, discretizedDimensions);

    float oDotC = VectorUtil.dotProduct(paddedVector, paddedCentroid);
    BQVectorUtils.subtractInPlace(paddedVector, paddedCentroid);

    float normOC = BQVectorUtils.norm(paddedVector);
    float[] normOMinusC = BQVectorUtils.divide(paddedVector, normOC); // OmC / norm(OmC)

    byte[] packedBinaryVector = packAsBinary(paddedVector, discretizedDimensions);

    float OOQ = computerOOQ(vector, normOMinusC, packedBinaryVector);

    short xbSum = (short) BQVectorUtils.popcount(packedBinaryVector, discretizedDimensions);

    return new SubspaceOutputMIP(packedBinaryVector, xbSum, oDotC, normOC, OOQ);
  }

  private float computerOOQ(float[] vector, float[] normOMinusC, byte[] packedBinaryVector) {
    float OOQ = 0f;
    for (int j = 0; j < vector.length / 8; j++) {
      for (int r = 0; r < 8; r++) {
        OOQ +=
            (normOMinusC[j * 8 + r]
                * (2f * ((packedBinaryVector[j] >> (7 - r)) & 0b00000001) - 1f));
      }
    }
    OOQ = OOQ / sqrtDimensions;
    return OOQ;
  }

  // FIXME: move this to a utils class
  private static float[] range(float[] q, float[] c) {
    float vl = 1e20f;
    float vr = -1e20f;
    for (int i = 0; i < q.length; i++) {
      float tmp = q[i] - c[i];
      if (tmp < vl) {
        vl = tmp;
      }
      if (tmp > vr) {
        vr = tmp;
      }
    }

    return new float[] {vl, vr};
  }

  public float[] quantizeForIndex(float[] vector, byte[] destination, float[] centroid) {
    float[] corrections = null;

    float distToCentroid = VectorUtil.squareDistance(vector, centroid);
    switch (similarityFunction) {
      case VectorSimilarityFunction.EUCLIDEAN:
      case VectorSimilarityFunction.COSINE:
      case VectorSimilarityFunction.DOT_PRODUCT:
        // FIXME: pass in a copy of vector as we will make changes to it in this function?
        SubspaceOutput subspaceOutput = generateSubSpace(vector, centroid);
        corrections = new float[2];
        // FIXME: quantize these values so we are passing back 1 byte values for all three of these
        // instead of floats
        corrections[0] = distToCentroid;
        corrections[1] = subspaceOutput.projection();
        System.arraycopy(
            subspaceOutput.packedBinaryVector(), 0, destination, 0, destination.length);
        break;
      case VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT:
        SubspaceOutputMIP subspaceOutputMIP = generateSubSpaceMIP(vector, centroid);
        corrections = new float[4];
        corrections[0] = subspaceOutputMIP.xbSum();
        // FIXME: quantize these values so we are passing back 1 byte values for all three of these
        // instead of floats
        corrections[1] = subspaceOutputMIP.oDotC();
        corrections[2] = subspaceOutputMIP.normOC();
        corrections[3] = subspaceOutputMIP.OOQ();
        System.arraycopy(
            subspaceOutputMIP.packedBinaryVector(), 0, destination, 0, destination.length);
        break;
    }

    return corrections;
  }

  private record QuantResult(byte[] result, int sumQ) {}

  private static QuantResult quantize(float[] q, float[] c, float[] u, float vl, float width) {
    // FIXME: speed up with panama? and/or use existing scalar quantization utils in Lucene?
    byte[] result = new byte[q.length];
    float oneOverWidth = 1.0f / width;
    int sumQ = 0;
    for (int i = 0; i < q.length; i++) {
      byte res = (byte) (((q[i] - c[i]) - vl) * oneOverWidth + u[i]);
      result[i] = res;
      sumQ += res;
    }

    return new QuantResult(result, sumQ);
  }

  public float[] quantizeForQuery(float[] vector, byte[] destination, float[] centroid) {
    float[] corrections = null;

    float lower, upper, width;
    byte[] byteQuery;
    int sumQ;
    float[] range;
    switch (similarityFunction) {
      case VectorSimilarityFunction.EUCLIDEAN:
      case VectorSimilarityFunction.COSINE:
      case VectorSimilarityFunction.DOT_PRODUCT:
        corrections = new float[3];

        range = range(vector, centroid);
        lower = range[0];
        upper = range[1];
        // Δ := (𝑣𝑟 − 𝑣𝑙)/(2𝐵𝑞 − 1)
        width = (upper - lower) / ((1 << BQSpaceUtils.B_QUERY) - 1);

        QuantResult quantResult = quantize(vector, centroid, u, lower, width);
        byteQuery = quantResult.result();
        sumQ = quantResult.sumQ();

        // Binary String Representation
        // FIXME: vectors need to be padded but that's expensive; update transponseBin to deal
        byteQuery = BQVectorUtils.pad(byteQuery, discretizedDimensions);
        BQSpaceUtils.transposeBin(byteQuery, discretizedDimensions, destination);
        corrections[0] = sumQ;
        corrections[1] = lower;
        corrections[2] = width;
        break;
      case VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT:
        corrections = new float[3];
        // FIXME: make a copy of vector so we don't overwrite it here?
        BQVectorUtils.subtract(vector, centroid);
        float[] qmCn = BQVectorUtils.divide(vector, BQVectorUtils.norm(vector));

        // FIXME: group this and pull it out as a separate function for quanization
        // Preprocess the residual query and the quantized query
        range = range(qmCn, centroid);
        lower = range[0];
        upper = range[1];
        // Δ := (𝑣𝑟 − 𝑣𝑙)/(2𝐵𝑞 − 1)
        width = (upper - lower) / ((1 << BQSpaceUtils.B_QUERY) - 1);

        byteQuery = new byte[qmCn.length];
        float oneOverWidth = 1.0f / width;
        sumQ = 0;
        for (int i = 0; i < qmCn.length; i++) {
          byte res = (byte) ((qmCn[i] - lower) * oneOverWidth + u[i]);
          byteQuery[i] = res;
          sumQ += res;
        }

        // q¯ = Δ · q¯𝑢 + 𝑣𝑙 · 1𝐷
        // q¯ is an approximation of q′  (scalar quantized approximation)
        // FIXME: vectors need to be padded but that's expensive; update transponseBin to deal
        byteQuery = BQVectorUtils.pad(byteQuery, discretizedDimensions);
        BQSpaceUtils.transposeBin(byteQuery, discretizedDimensions, destination);
        corrections[0] = sumQ;
        corrections[1] = lower;
        corrections[2] = width;
        break;
    }

    return corrections;
  }
}
