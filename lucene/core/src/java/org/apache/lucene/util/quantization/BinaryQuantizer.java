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

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.VectorUtil;

import java.util.Arrays;
import java.util.Random;


record SubspaceOutput(float projection, byte[] packedBinaryVector) {}
record SubspaceOutputMIP(float projection, byte[] packedBinaryVector, float oDotC, float normOC, float OOQ) {}

// FIXME: write a couple of high level tests for now
public class BinaryQuantizer {
  private static final int QUERY_PROJECTIONS = 4;
  private final float[] centroid;

  private static final VectorSpecies<Float> FLOAT_SPECIES = FloatVector.SPECIES_PREFERRED;

  private final int dimensions;
  private final int discretizedDimensions;

  private final float[] u; // discretizedDimensions of floats random numbers sampled from the uniform distribution [0,1]

  private final float sqrtDimensions;

  public BinaryQuantizer(float[] centroid) {
    this.centroid = centroid;
    this.dimensions = centroid.length;
    this.discretizedDimensions = (dimensions + 63) / 64 * 64;

    // FIXME: allow the seed to be settable
    Random random = new Random(1);
    u = new float[discretizedDimensions];
    for (int i = 0; i < discretizedDimensions; i++) {
      u[i] = (float) random.nextDouble();
    }

    this.sqrtDimensions = (float) Math.sqrt(discretizedDimensions); // FIXME: cache this
  }

  // FIXME: move this out to vector utils
  public static float[] pad(float[] vector, int dimensions) {
    if (vector.length >= dimensions) {
      return vector;
    }
    float[] paddedVector = new float[dimensions];
    for (int i = 0; i < dimensions; i++) {
      if (i < vector.length) {
        paddedVector[i] = vector[i];
      } else {
        paddedVector[i] = 0;
      }
    }
    return paddedVector;
  }

  // FIXME: move this out to vector utils
  public static void subtract(float[] a, float[] b) {
    for (int j = 0; j < a.length; j++) {
      a[j] -= b[j];
    }
  }

  // FIXME: move this out to vector utils
  public static float norm(float[] vector) {
    float normalized = 0f;
    // Calculate magnitude/length of the vector
    double magnitude = 0;

    int size = vector.length / FLOAT_SPECIES.length();
    for (int r = 0; r < size; r++) {
      int offset = FLOAT_SPECIES.length() * r;
      FloatVector va = FloatVector.fromArray(FLOAT_SPECIES, vector, offset);
      magnitude += va.mul(va).reduceLanes(VectorOperators.ADD);
    }

    // tail
    int remainder = vector.length % FLOAT_SPECIES.length();
    if (remainder != 0) {
      for (int i = vector.length - remainder; i < vector.length; i++) {
        magnitude = Math.fma(vector[i], vector[i], magnitude);
      }
    }

    // FIXME: evaluate for small dimensions whether this is faster
    //            for (int i = 0; i < vector.length; i++) {
    //                magnitude = Math.fma(vector[i], vector[i], magnitude);
    //            }

    magnitude = Math.sqrt(magnitude);

    // FIXME: FUTURE - not good; sometimes this needs to be 0
    //            if (magnitude == 0) {
    //                throw new IllegalArgumentException("Cannot normalize a vector of length
    // zero.");
    //            }

    normalized = (float) magnitude;

    return normalized;
  }

  // FIXME: move this out to vector utils
  public static float[] subset(float[] a, int lastColumn) {
    return Arrays.copyOf(a, lastColumn);
  }

  // FIXME: move this out to vector utils
  public static void removeSignAndDivide(float[] a, float divisor) {
    // FIXME: revert to old behavior for small dimensions
    //            for(int j = 0; j < a[0].length; j++) {
    //                a[i][j] = Math.abs(a[i][j]) / divisor;
    //            }
    int size = a.length / FLOAT_SPECIES.length();
    for (int r = 0; r < size; r++) {
      int offset = FLOAT_SPECIES.length() * r;
      FloatVector va = FloatVector.fromArray(FLOAT_SPECIES, a, offset);
      va.abs().div(divisor).intoArray(a, offset);
    }

    // tail
    int remainder = a.length % FLOAT_SPECIES.length();
    if (remainder != 0) {
      for (int i = a.length - remainder; i < a.length; i++) {
        a[i] = Math.abs(a[i]) / divisor;
      }
    }
  }

  // FIXME: move this out to vector utils
  public static float sumAndNormalize(float[] a, float norm) {
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

  // FIXME: move this out to vector utils
  public static float[] divide(float[] a, float b) {
    float[] c = new float[a.length];
    for (int j = 0; j < a.length; j++) {
      c[j] = a[j] / b;
    }
    return c;
  }

  public static byte[] packAsBinary(float[] vector, int dimensions) {
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

  public static float computeOOQ(float[] vector, int dimensions) {
    // FIXME: write me
    return 0.0f;
  }

  public SubspaceOutput generateSubSpace(
          float[] vector,
          float[] centroid) {

    // typically no-op if dimensions/64
    float[] paddedCentroid = pad(centroid, discretizedDimensions);
    float[] paddedVector = pad(vector, discretizedDimensions);
    subtract(paddedVector, paddedCentroid);

    // The inner product between the data vector and the quantized data vector
    float norm = norm(paddedVector);
    float[] vectorSubset = subset(paddedVector, discretizedDimensions); // typically no-op if D/64
    removeSignAndDivide(vectorSubset, (float) Math.pow(discretizedDimensions, 0.5));
    float projection = sumAndNormalize(vectorSubset, norm);
    byte[] packedBinaryVector = packAsBinary(paddedVector, discretizedDimensions);
    return new SubspaceOutput(projection, packedBinaryVector);
  }

  // FIXME: write me & come up with a better name for this function
  public SubspaceOutputMIP generateSubSpaceMIP(
          float[] vector,
          float[] centroid) {

    // typically no-op if dimensions/64
    float[] paddedCentroid = pad(centroid, discretizedDimensions);
    float[] paddedVector = pad(vector, discretizedDimensions);
    float oDotC = VectorUtil.dotProduct(paddedVector, paddedCentroid);
    subtract(paddedVector, paddedCentroid);

    float normOC = norm(paddedVector);
    float[] normOMinusC = divide(paddedVector, normOC); // == OmC / norm(OmC)

    float[] vectorSubset = subset(paddedVector, discretizedDimensions); // typically no-op if D/64
    removeSignAndDivide(vectorSubset, (float) Math.pow(discretizedDimensions, 0.5));
    float projection = sumAndNormalize(vectorSubset, normOC);
    byte[] packedBinaryVector = packAsBinary(paddedVector, discretizedDimensions);

    // FIXME: pull this out to a function
    float OOQ = 0f;
    for(int j = 0; j < vector.length / 8; j++) {
      for(int r = 0; r < 8; r++) {
        OOQ += (normOMinusC[j*8+r] * (2f * ((packedBinaryVector[j] >> (7-r)) & 0b00000001) - 1f));
      }
    }
    OOQ = OOQ / sqrtDimensions;

    return new SubspaceOutputMIP(projection, packedBinaryVector, oDotC, normOC, OOQ);
  }

  // FIXME: reintroduce a space utils
  public static final int B_QUERY = 4;
  private static final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_128;
  private static final byte BYTE_MASK = (1 << B_QUERY) - 1;

  // FIXME: clean up this function and move to utils like "space utils"
  private static void moveMaskEpi8Byte(byte[] v, byte[] v1b) {
    int m = 0;
    for (int k = 0; k < v.length; k++) {
      if ((v[k] & 0b10000000) == 0b10000000) {
        v1b[m] |= 0b00000001;
      }
      if (k % 8 == 7) {
        m++;
      } else {
        v1b[m] <<= 1;
      }
    }
  }

  // FIXME: clean up this function and move to utils like "space utils"
  public static byte[] transposeBinPan(byte[] q, int D) {
    assert B_QUERY > 0;
    int B = (D + 63) / 64 * 64;
    byte[] quantQueryByte = new byte[B_QUERY * B / 8];
    int qOffset = 0;

    final byte[] v = new byte[32];
    final byte[] v1b = new byte[4];
    for (int i = 0; i < B; i += 32) {
      ByteVector q0 = ByteVector.fromArray(SPECIES, q, qOffset);
      ByteVector q1 = ByteVector.fromArray(SPECIES, q, qOffset + 16);

      ByteVector v0 = q0.lanewise(VectorOperators.LSHL, 8 - B_QUERY);
      ByteVector v1 = q1.lanewise(VectorOperators.LSHL, 8 - B_QUERY);
      v0 =
              v0.lanewise(
                      VectorOperators.OR, q0.lanewise(VectorOperators.LSHR, B_QUERY).and(BYTE_MASK));
      v1 =
              v1.lanewise(
                      VectorOperators.OR, q1.lanewise(VectorOperators.LSHR, B_QUERY).and(BYTE_MASK));

      for (int j = 0; j < B_QUERY; j++) {
        v0.intoArray(v, 0);
        v1.intoArray(v, 16);
        moveMaskEpi8Byte(v, v1b);
        for (int k = 0; k < 4; k++) {
          quantQueryByte[(B_QUERY - j - 1) * (B / 8) + i / 8 + k] = v1b[k];
          v1b[k] = 0;
        }

        v0 = v0.lanewise(VectorOperators.ADD, v0);
        v1 = v1.lanewise(VectorOperators.ADD, v1);
      }
      qOffset += 32;
    }
    return quantQueryByte;
  }

  public float[] quantizeForIndex(
          float[] vector, byte[] destination, VectorSimilarityFunction similarityFunction) {
    float[] corrections = null;

    float distToCentroid = VectorUtil.squareDistance(vector, centroid);
    switch (similarityFunction) {
      case VectorSimilarityFunction.EUCLIDEAN:
      case VectorSimilarityFunction.COSINE:
      case VectorSimilarityFunction.DOT_PRODUCT:
        // FIXME: pass in a copy of vector as we will make changes to it in this function?
        SubspaceOutput subspaceOutput = generateSubSpace(vector, centroid);
        corrections = new float[2];
        corrections[0] = distToCentroid;
        corrections[1] = subspaceOutput.projection();
        destination = subspaceOutput.packedBinaryVector();
        break;
      case VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT:
        SubspaceOutputMIP subspaceOutputMIP = generateSubSpaceMIP(vector, centroid);
        corrections = new float[5];
        corrections[0] = distToCentroid;
        corrections[1] = subspaceOutputMIP.projection();
        corrections[2] = subspaceOutputMIP.oDotC();
        corrections[3] = subspaceOutputMIP.normOC();
        corrections[4] = subspaceOutputMIP.OOQ();
        destination = subspaceOutputMIP.packedBinaryVector();
        break;
    }

    return corrections;
  }

  // FIXME: move this to a utils class
  public static float[] range(float[] q, float[] c) {
    float vl = Float.POSITIVE_INFINITY;
    float vr = Float.NEGATIVE_INFINITY;
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

  // FIXME: move this to a utils class
  public static QuantResult quantize(float[] q, float[] c, float[] u, float vl, float width) {
    // FIXME: speed up with panama?
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

  // FIXME: pull this out
  public record QuantResult(byte[] result, int sumQ) {}

  public float[] quantizeForQuery(
          float[] vector, byte[] destination, VectorSimilarityFunction similarityFunction) {
    float[] corrections = null;

    float distToCentroid = VectorUtil.squareDistance(vector, centroid);
    float vl, vr, width;
    byte[] byteQuery;
    int sumQ;
    switch (similarityFunction) {
      case VectorSimilarityFunction.EUCLIDEAN:
      case VectorSimilarityFunction.COSINE:
      case VectorSimilarityFunction.DOT_PRODUCT:
        // FIXME: clean up and pull out this stuff into a function
        corrections = new float[3];
        float[] v = range(vector, centroid);
        vl = v[0];
        vr = v[1];
        width = (vr - vl) / ((1 << B_QUERY) - 1);

        QuantResult quantResult = quantize(vector, centroid, u, vl, width);
        byteQuery = quantResult.result();
        sumQ = quantResult.sumQ();

        // Binary String Representation
        destination = transposeBinPan(byteQuery, discretizedDimensions);
        corrections[0] = sumQ;
        corrections[1] = vl;
        corrections[2] = width;
        break;
      case VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT:
        // FIXME: clean up and pull out this stuff into a function
        corrections = new float[3];
        // FIXME: make a copy of vector so we don't overwrite it here?
        subtract(vector, centroid);
        float[] QmCn = divide(vector, norm(vector));

        // Preprocess the residual query and the quantized query
        vl = Float.POSITIVE_INFINITY;
        vr = Float.NEGATIVE_INFINITY;
        for (int i = 0; i < QmCn.length; i++) {
          if (QmCn[i] < vl) {
            vl = QmCn[i];
          }
          if (QmCn[i] > vr) {
            vr = QmCn[i];
          }
        }

        // Δ := (𝑣𝑟 − 𝑣𝑙)/(2𝐵𝑞 − 1)
        width = (vr - vl) / ((1 << B_QUERY) - 1);

        byteQuery = new byte[QmCn.length];
        float oneOverWidth = 1.0f / width;
        sumQ = 0;
        for (int i = 0; i < QmCn.length; i++) {
          byte res = (byte) ((QmCn[i] - vl) * oneOverWidth + u[i]);
          byteQuery[i] = res;
          sumQ += res;
        }

        // q¯ = Δ · q¯𝑢 + 𝑣𝑙 · 1𝐷
        // q¯ is an approximation of q′  (scalar quantized approximation)
        destination = transposeBinPan(byteQuery, discretizedDimensions);
        corrections[0] = sumQ;
        corrections[1] = vl;
        corrections[2] = width;
        break;
    }

    return corrections;
  }
}