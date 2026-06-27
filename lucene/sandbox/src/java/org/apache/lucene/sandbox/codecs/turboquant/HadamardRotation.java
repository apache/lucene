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

import java.util.Random;

/**
 * Randomized Hadamard rotation for TurboQuant. Applies Π = BlockHadamard · Permutation · SignFlip
 * to decorrelate vector coordinates before scalar quantization.
 *
 * <p>For power-of-2 dimensions (e.g., d=4096), this is a single Hadamard transform with random
 * sign flips. For non-power-of-2 dimensions (e.g., d=768), a block-diagonal Hadamard is used with
 * blocks determined by the binary decomposition of d, preceded by a random permutation.
 *
 * <p>The rotation is orthogonal, so it preserves all distances and inner products.
 */
public final class HadamardRotation {

  private final int d;
  private final int[] blockSizes;
  private final int[] permutation;
  private final int[] inversePermutation;
  private final boolean[] signs; // true = negative

  private HadamardRotation(int d, int[] blockSizes, int[] permutation, boolean[] signs) {
    this.d = d;
    this.blockSizes = blockSizes;
    this.permutation = permutation;
    this.signs = signs;
    this.inversePermutation = new int[d];
    for (int i = 0; i < d; i++) {
      inversePermutation[permutation[i]] = i;
    }
  }

  /**
   * Creates a HadamardRotation for the given dimension and seed. The rotation is deterministic for
   * a given (d, seed) pair.
   */
  public static HadamardRotation create(int d, long seed) {
    if (d < 1) {
      throw new IllegalArgumentException("Dimension must be >= 1, got " + d);
    }
    int[] blockSizes = decomposeBlocks(d);
    Random rng = new Random(seed);

    // Fisher-Yates shuffle for random permutation
    int[] permutation = new int[d];
    for (int i = 0; i < d; i++) {
      permutation[i] = i;
    }
    for (int i = d - 1; i > 0; i--) {
      int j = rng.nextInt(i + 1);
      int tmp = permutation[i];
      permutation[i] = permutation[j];
      permutation[j] = tmp;
    }

    // Random sign flips
    boolean[] signs = new boolean[d];
    for (int i = 0; i < d; i++) {
      signs[i] = rng.nextBoolean();
    }

    return new HadamardRotation(d, blockSizes, permutation, signs);
  }

  /**
   * Decomposes d into power-of-2 block sizes (binary representation). The blocks are returned in
   * descending order and sum to d.
   */
  static int[] decomposeBlocks(int d) {
    if (d < 1) {
      throw new IllegalArgumentException("d must be >= 1, got " + d);
    }
    int bitCount = Integer.bitCount(d);
    int[] blocks = new int[bitCount];
    int idx = 0;
    for (int bit = 30; bit >= 0; bit--) {
      if ((d & (1 << bit)) != 0) {
        blocks[idx++] = 1 << bit;
      }
    }
    return blocks;
  }

  /**
   * Applies the rotation: out = BlockHadamard(Permute(SignFlip(x))). The output is normalized so
   * that ||out|| = ||x||.
   */
  public void rotate(float[] x, float[] out) {
    // Step 1: Sign flip
    for (int i = 0; i < d; i++) {
      out[i] = signs[i] ? -x[i] : x[i];
    }

    // Step 2: Permute (out[permutation[i]] = signFlipped[i], but we need to reorder)
    // We need a temp buffer for the permutation step
    float[] temp = new float[d];
    for (int i = 0; i < d; i++) {
      temp[permutation[i]] = out[i];
    }

    // Step 3: Block-diagonal Hadamard
    int offset = 0;
    for (int blockSize : blockSizes) {
      fwht(temp, offset, blockSize);
      offset += blockSize;
    }

    System.arraycopy(temp, 0, out, 0, d);
  }

  /**
   * Applies the inverse rotation: out = SignFlip⁻¹(Permute⁻¹(BlockHadamard⁻¹(y))). Since
   * Hadamard is self-inverse (up to scaling) and we normalize, this exactly inverts rotate().
   */
  public void inverseRotate(float[] y, float[] out) {
    // Step 1: Inverse block-diagonal Hadamard (same as forward — Hadamard is self-inverse)
    float[] temp = new float[d];
    System.arraycopy(y, 0, temp, 0, d);
    int offset = 0;
    for (int blockSize : blockSizes) {
      fwht(temp, offset, blockSize);
      offset += blockSize;
    }

    // Step 2: Inverse permute
    for (int i = 0; i < d; i++) {
      out[i] = temp[permutation[i]];
    }

    // Step 3: Inverse sign flip (same as forward — signs are self-inverse)
    for (int i = 0; i < d; i++) {
      if (signs[i]) {
        out[i] = -out[i];
      }
    }
  }

  /**
   * In-place Fast Walsh-Hadamard Transform on a contiguous block of the array. The transform is
   * normalized by 1/√blockSize so that it preserves the L2 norm.
   */
  private static void fwht(float[] data, int offset, int n) {
    for (int len = 1; len < n; len <<= 1) {
      for (int i = 0; i < n; i += len << 1) {
        for (int j = 0; j < len; j++) {
          int u = offset + i + j;
          int v = u + len;
          float a = data[u];
          float b = data[v];
          data[u] = a + b;
          data[v] = a - b;
        }
      }
    }
    // Normalize by 1/√n to preserve L2 norm
    float scale = (float) (1.0 / Math.sqrt(n));
    for (int i = 0; i < n; i++) {
      data[offset + i] *= scale;
    }
  }

  /** Returns the dimension this rotation operates on. */
  public int dimension() {
    return d;
  }

  /** Returns the block sizes used in the block-diagonal Hadamard. */
  public int[] blockSizes() {
    return blockSizes.clone();
  }
}
