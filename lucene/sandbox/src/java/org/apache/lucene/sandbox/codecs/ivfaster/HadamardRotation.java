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
package org.apache.lucene.sandbox.codecs.ivfaster;

import java.util.Random;

/**
 * A randomized orthogonal rotation built from random sign flips, a random permutation, and a
 * block-diagonal Fast Walsh-Hadamard Transform (FWHT).
 *
 * <p>The transform {@code out = R * in} is the composition {@code R = F * P * S} where:
 *
 * <ul>
 *   <li>{@code S} flips the sign of each component by a fixed random {@code ±1},
 *   <li>{@code P} applies a fixed random (Fisher-Yates) permutation, and
 *   <li>{@code F} applies a normalized FWHT independently over each power-of-two block that
 *       composes the dimension (the set bits of {@code dim}; e.g. {@code 768 -> [512][256]}).
 * </ul>
 *
 * <p>Each factor is orthogonal, so {@code R} is orthogonal: it preserves L2 norm and dot products
 * (hence Euclidean, cosine and dot-product similarities are unchanged), and its inverse is its
 * transpose, {@code R^-1 = R^T}. The rotation is fully determined by {@code (dim, seed)} and is
 * immutable and thread-safe; {@link #rotate} and {@link #inverseRotate} only write to the
 * caller-provided output array.
 *
 * <p>The purpose of the rotation is to "Gaussianize" the per-dimension value distribution so that
 * fixed quantization bins are used more evenly, improving scalar-quantization accuracy. As a
 * side-effect, because the permutation spreads information uniformly across positions, any fixed
 * prefix of the rotated coordinates is an unbiased random projection of the input, which makes
 * truncated-dimension distance estimation valid.
 *
 * @lucene.experimental
 */
public final class HadamardRotation {

  private final int dim;

  /** Random ±1 per dimension (the diagonal of {@code S}). */
  private final float[] signs;

  /**
   * Fisher-Yates permutation; {@code perm[i]} is the source index gathered into position {@code i}.
   */
  private final int[] perm;

  /** Start offset of each power-of-two FWHT block. */
  private final int[] blockOffsets;

  /** Length of each FWHT block (a power of two). */
  private final int[] blockLengths;

  private HadamardRotation(
      int dim, float[] signs, int[] perm, int[] blockOffsets, int[] blockLengths) {
    this.dim = dim;
    this.signs = signs;
    this.perm = perm;
    this.blockOffsets = blockOffsets;
    this.blockLengths = blockLengths;
  }

  /**
   * Builds the rotation for the given dimension, deterministically seeded. The same {@code (dim,
   * seed)} always produces the same rotation.
   */
  public static HadamardRotation create(int dim, long seed) {
    if (dim < 1) {
      throw new IllegalArgumentException("dim must be >= 1, got " + dim);
    }
    Random random = new Random(seed);

    float[] signs = new float[dim];
    for (int i = 0; i < dim; i++) {
      signs[i] = random.nextBoolean() ? 1f : -1f;
    }

    // Fisher-Yates permutation.
    int[] perm = new int[dim];
    for (int i = 0; i < dim; i++) {
      perm[i] = i;
    }
    for (int i = dim - 1; i > 0; i--) {
      int j = random.nextInt(i + 1);
      int tmp = perm[i];
      perm[i] = perm[j];
      perm[j] = tmp;
    }

    // Decompose dim into power-of-two blocks (its set bits), largest first.
    int numBlocks = Integer.bitCount(dim);
    int[] blockOffsets = new int[numBlocks];
    int[] blockLengths = new int[numBlocks];
    int offset = 0;
    int b = 0;
    for (int bit = Integer.highestOneBit(dim); bit != 0; bit >>>= 1) {
      if ((dim & bit) != 0) {
        blockOffsets[b] = offset;
        blockLengths[b] = bit;
        offset += bit;
        b++;
      }
    }
    assert offset == dim;

    return new HadamardRotation(dim, signs, perm, blockOffsets, blockLengths);
  }

  /** The dimension this rotation operates on. */
  public int dimension() {
    return dim;
  }

  /**
   * Applies the forward rotation {@code out = R * in}. {@code in} is not modified; {@code in} and
   * {@code out} must both have length {@link #dimension()} and must not be the same array.
   */
  public void rotate(float[] in, float[] out) {
    checkArgs(in, out);
    // Combined sign-flip + permutation gather: out[i] = sign[perm[i]] * in[perm[i]].
    for (int i = 0; i < dim; i++) {
      int src = perm[i];
      out[i] = signs[src] * in[src];
    }
    // Block-diagonal normalized FWHT, in place on out.
    for (int blk = 0; blk < blockOffsets.length; blk++) {
      fwht(out, blockOffsets[blk], blockLengths[blk]);
    }
  }

  /**
   * Applies the inverse rotation {@code out = R^T * in}. {@code in} is not modified; {@code in} and
   * {@code out} must both have length {@link #dimension()} and must not be the same array.
   */
  public void inverseRotate(float[] in, float[] out) {
    checkArgs(in, out);
    // F is symmetric and its own inverse (normalized), so apply it first, on a copy.
    float[] f = new float[dim];
    System.arraycopy(in, 0, f, 0, dim);
    for (int blk = 0; blk < blockOffsets.length; blk++) {
      fwht(f, blockOffsets[blk], blockLengths[blk]);
    }
    // Inverse permutation then sign-flip (scatter): out[perm[i]] = sign[perm[i]] * f[i].
    for (int i = 0; i < dim; i++) {
      int dst = perm[i];
      out[dst] = signs[dst] * f[i];
    }
  }

  private void checkArgs(float[] in, float[] out) {
    if (in.length != dim || out.length != dim) {
      throw new IllegalArgumentException(
          "in/out length must equal dim=" + dim + ", got in=" + in.length + " out=" + out.length);
    }
    if (in == out) {
      throw new IllegalArgumentException("in and out must be different arrays");
    }
  }

  /**
   * In-place normalized Fast Walsh-Hadamard Transform over {@code a[offset, offset+len)}, where
   * {@code len} is a power of two. Normalized by {@code 1/sqrt(len)} so the transform is orthogonal
   * and self-inverse. A length-1 block is the identity.
   */
  private static void fwht(float[] a, int offset, int len) {
    for (int h = 1; h < len; h <<= 1) {
      for (int i = 0; i < len; i += h << 1) {
        for (int j = i; j < i + h; j++) {
          int p = offset + j;
          int q = p + h;
          float x = a[p];
          float y = a[q];
          a[p] = x + y;
          a[q] = x - y;
        }
      }
    }
    if (len > 1) {
      float scale = (float) (1.0 / Math.sqrt(len));
      for (int i = offset; i < offset + len; i++) {
        a[i] *= scale;
      }
    }
  }
}
