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
package org.apache.lucene.sandbox.codecs.turboquant;

/**
 * Precomputed Lloyd-Max optimal centroids for Gaussian-distributed coordinates. After random
 * rotation, each coordinate of a unit vector in ℝᵈ follows approximately N(0, 1/d) for d ≥ 64.
 * Canonical centroids are computed for N(0,1) and scaled by 1/√d at runtime.
 */
public final class BetaCodebook {

  private BetaCodebook() {}

  // Canonical Lloyd-Max optimal centroids for N(0,1), computed via Lloyd's algorithm.
  // Symmetric around 0. Scaled by 1/√d at runtime.

  // @formatter:off
  private static final float[] CENTROIDS_2 = {
    -1.510418f, -0.452780f, 0.452780f, 1.510418f
  };

  private static final float[] CENTROIDS_3 = {
    -2.151946f, -1.343909f, -0.756005f, -0.245094f,
     0.245094f,  0.756005f,  1.343909f,  2.151946f
  };

  private static final float[] CENTROIDS_4 = {
    -2.732590f, -2.069017f, -1.618046f, -1.256231f,
    -0.942340f, -0.656759f, -0.388048f, -0.128395f,
     0.128395f,  0.388048f,  0.656759f,  0.942340f,
     1.256231f,  1.618046f,  2.069017f,  2.732590f
  };

  private static final float[] CENTROIDS_8 = {
    -4.035480f, -3.565625f, -3.268187f, -3.045475f, -2.865491f, -2.713551f, -2.581644f, -2.464895f,
    -2.360107f, -2.265066f, -2.178166f, -2.098206f, -2.024257f, -1.955584f, -1.891595f, -1.831799f,
    -1.775785f, -1.723203f, -1.673751f, -1.627164f, -1.583207f, -1.541672f, -1.502368f, -1.465126f,
    -1.429789f, -1.396212f, -1.364264f, -1.333822f, -1.304772f, -1.277010f, -1.250438f, -1.224965f,
    -1.200508f, -1.176989f, -1.154335f, -1.132480f, -1.111361f, -1.090923f, -1.071113f, -1.051883f,
    -1.033188f, -1.014988f, -0.997247f, -0.979930f, -0.963006f, -0.946448f, -0.930229f, -0.914327f,
    -0.898719f, -0.883388f, -0.868315f, -0.853484f, -0.838881f, -0.824492f, -0.810305f, -0.796310f,
    -0.782495f, -0.768852f, -0.755371f, -0.742046f, -0.728869f, -0.715832f, -0.702931f, -0.690157f,
    -0.677508f, -0.664976f, -0.652557f, -0.640248f, -0.628042f, -0.615938f, -0.603930f, -0.592014f,
    -0.580189f, -0.568449f, -0.556793f, -0.545217f, -0.533718f, -0.522294f, -0.510941f, -0.499658f,
    -0.488442f, -0.477290f, -0.466201f, -0.455172f, -0.444200f, -0.433285f, -0.422424f, -0.411614f,
    -0.400855f, -0.390145f, -0.379481f, -0.368862f, -0.358286f, -0.347752f, -0.337259f, -0.326803f,
    -0.316386f, -0.306003f, -0.295655f, -0.285340f, -0.275057f, -0.264803f, -0.254579f, -0.244382f,
    -0.234211f, -0.224066f, -0.213944f, -0.203846f, -0.193768f, -0.183712f, -0.173674f, -0.163654f,
    -0.153652f, -0.143665f, -0.133694f, -0.123736f, -0.113791f, -0.103857f, -0.093934f, -0.084021f,
    -0.074116f, -0.064219f, -0.054328f, -0.044443f, -0.034562f, -0.024685f, -0.014810f, -0.004936f,
     0.004936f,  0.014810f,  0.024685f,  0.034562f,  0.044443f,  0.054328f,  0.064219f,  0.074116f,
     0.084021f,  0.093934f,  0.103857f,  0.113791f,  0.123736f,  0.133694f,  0.143665f,  0.153652f,
     0.163654f,  0.173674f,  0.183712f,  0.193768f,  0.203846f,  0.213944f,  0.224066f,  0.234211f,
     0.244382f,  0.254579f,  0.264803f,  0.275057f,  0.285340f,  0.295655f,  0.306003f,  0.316386f,
     0.326803f,  0.337259f,  0.347752f,  0.358286f,  0.368862f,  0.379481f,  0.390145f,  0.400855f,
     0.411614f,  0.422424f,  0.433285f,  0.444200f,  0.455172f,  0.466201f,  0.477290f,  0.488442f,
     0.499658f,  0.510941f,  0.522294f,  0.533718f,  0.545217f,  0.556793f,  0.568449f,  0.580189f,
     0.592014f,  0.603930f,  0.615938f,  0.628042f,  0.640248f,  0.652557f,  0.664976f,  0.677508f,
     0.690157f,  0.702931f,  0.715832f,  0.728869f,  0.742046f,  0.755371f,  0.768852f,  0.782495f,
     0.796310f,  0.810305f,  0.824492f,  0.838881f,  0.853484f,  0.868315f,  0.883388f,  0.898719f,
     0.914327f,  0.930229f,  0.946448f,  0.963006f,  0.979930f,  0.997247f,  1.014988f,  1.033188f,
     1.051883f,  1.071113f,  1.090923f,  1.111361f,  1.132480f,  1.154335f,  1.176989f,  1.200508f,
     1.224965f,  1.250438f,  1.277010f,  1.304772f,  1.333822f,  1.364264f,  1.396212f,  1.429789f,
     1.465126f,  1.502368f,  1.541672f,  1.583207f,  1.627164f,  1.673751f,  1.723203f,  1.775785f,
     1.831799f,  1.891595f,  1.955584f,  2.024257f,  2.098206f,  2.178166f,  2.265066f,  2.360107f,
     2.464895f,  2.581644f,  2.713551f,  2.865491f,  3.045475f,  3.268187f,  3.565625f,  4.035480f
  };
  // @formatter:on

  private static float[] canonicalCentroids(int b) {
    return switch (b) {
      case 2 -> CENTROIDS_2;
      case 3 -> CENTROIDS_3;
      case 4 -> CENTROIDS_4;
      case 8 -> CENTROIDS_8;
      default -> throw new IllegalArgumentException("Unsupported bit-width: " + b);
    };
  }

  /**
   * Returns 2^b centroid values scaled by 1/√d for the given dimension and bit-width. These are the
   * reconstruction values for quantized coordinates after Hadamard rotation.
   */
  public static float[] centroids(int d, int b) {
    float[] canonical = canonicalCentroids(b);
    float scale = (float) (1.0 / Math.sqrt(d));
    float[] result = new float[canonical.length];
    for (int i = 0; i < canonical.length; i++) {
      result[i] = canonical[i] * scale;
    }
    return result;
  }

  /**
   * Returns 2^b + 1 decision boundary values scaled by 1/√d. Boundaries are midpoints between
   * adjacent centroids, with first = -∞ (represented as {@code -Float.MAX_VALUE}) and last = +∞
   * (represented as {@code Float.MAX_VALUE}).
   */
  public static float[] boundaries(int d, int b) {
    float[] c = centroids(d, b);
    float[] bd = new float[c.length + 1];
    bd[0] = -Float.MAX_VALUE;
    bd[c.length] = Float.MAX_VALUE;
    for (int i = 0; i < c.length - 1; i++) {
      bd[i + 1] = (c[i] + c[i + 1]) / 2;
    }
    return bd;
  }

  /**
   * Quantizes a single coordinate value to the nearest centroid index using binary search on
   * boundaries.
   */
  public static int quantize(float value, float[] boundaries) {
    // Binary search for the bin
    int lo = 1, hi = boundaries.length - 2;
    while (lo <= hi) {
      int mid = (lo + hi) >>> 1;
      if (value < boundaries[mid]) {
        hi = mid - 1;
      } else {
        lo = mid + 1;
      }
    }
    return hi;
  }
}
