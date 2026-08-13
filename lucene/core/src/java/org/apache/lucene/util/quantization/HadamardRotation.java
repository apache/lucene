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
import java.util.concurrent.ConcurrentHashMap;

/**
 * A randomized orthogonal rotation: random sign flips, a random permutation, then a Fast
 * Walsh-Hadamard Transform. Being orthogonal it preserves L2 norms and inner products, so it is
 * safe to apply to index and query vectors alike. Rotated components tend toward a normal
 * distribution, which is what scalar quantizers assume, so it helps most on skewed or sparse
 * inputs.
 *
 * <p>Costs {@code O(d log d)} per vector. Non-power-of-2 dimensions use a block-diagonal FWHT over
 * the binary decomposition of {@code d}.
 *
 * <p>Deterministic for a given {@code (dim, seed)}, so all segments share one rotation and
 * quantized bytes can be copied during merge. Instances are immutable and thread-safe.
 *
 * @lucene.experimental
 */
public final class HadamardRotation {

  private final int dim;
  private final int[] blockSizes;
  private final int[] permutation;
  private final boolean[] signFlips;

  private HadamardRotation(int dim, int[] blockSizes, int[] permutation, boolean[] signFlips) {
    this.dim = dim;
    this.blockSizes = blockSizes;
    this.permutation = permutation;
    this.signFlips = signFlips;
  }

  /**
   * Creates a deterministic rotation for the given dimension and seed. The same (dim, seed) pair
   * always produces the same rotation. Instances are immutable and thread-safe.
   */
  public static HadamardRotation create(int dim, long seed) {
    if (dim < 1) {
      throw new IllegalArgumentException("dim must be >= 1, got " + dim);
    }
    int[] blocks = decomposeIntoPowerOfTwoBlocks(dim);
    Random rng = new Random(seed);

    // Fisher-Yates shuffle for the permutation.
    int[] permutation = new int[dim];
    for (int i = 0; i < dim; i++) {
      permutation[i] = i;
    }
    for (int i = dim - 1; i > 0; i--) {
      int j = rng.nextInt(i + 1);
      int tmp = permutation[i];
      permutation[i] = permutation[j];
      permutation[j] = tmp;
    }

    boolean[] signs = new boolean[dim];
    for (int i = 0; i < dim; i++) {
      signs[i] = rng.nextBoolean();
    }

    return new HadamardRotation(dim, blocks, permutation, signs);
  }

  private static final ConcurrentHashMap<Integer, HadamardRotation> BY_DIMENSION =
      new ConcurrentHashMap<>();

  /**
   * Returns the deterministic seed used by {@link #forDimension(int)} for the given dimension.
   * Exposed so callers (e.g. {@link org.apache.lucene.codecs.PreconditioningKnnVectorsFormat}) can
   * persist the seed alongside the rotated vectors, decoupling on-disk indices from any future
   * change to the seed-mix function: future readers reconstruct the rotation from the persisted
   * seed via {@link #create(int, long)} rather than re-deriving from the dimension.
   */
  public static long seedForDimension(int dim) {
    long seed = (long) dim * 0x9E3779B97F4A7C15L;
    seed ^= (seed >>> 30);
    seed *= 0xBF58476D1CE4E5B9L;
    seed ^= (seed >>> 27);
    seed *= 0x94D049BB133111EBL;
    seed ^= (seed >>> 31);
    return seed;
  }

  /**
   * Returns a shared rotation instance for the given dimension. All fields with the same dimension
   * share a single rotation, avoiding redundant heap usage across fields and segments. The seed is
   * derived deterministically from the dimension via {@link #seedForDimension(int)}.
   */
  public static HadamardRotation forDimension(int dimension) {
    if (dimension < 1) {
      throw new IllegalArgumentException("dim must be >= 1, got " + dimension);
    }
    return BY_DIMENSION.computeIfAbsent(dimension, d -> create(d, seedForDimension(d)));
  }

  /**
   * Decomposes {@code d} into a list of power-of-2 block sizes that sum to {@code d}. For example,
   * {@code 768 = 512 + 256}, {@code 100 = 64 + 32 + 4}. This is used to build a block-diagonal
   * Hadamard transform when {@code d} is not a power of two.
   */
  static int[] decomposeIntoPowerOfTwoBlocks(int d) {
    int count = Integer.bitCount(d);
    int[] out = new int[count];
    int idx = 0;
    for (int bit = 30; bit >= 0; bit--) {
      int size = 1 << bit;
      if ((d & size) != 0) {
        out[idx++] = size;
      }
    }
    return out;
  }

  /** Returns the dimension this rotation operates on. */
  public int dimension() {
    return dim;
  }

  /**
   * Applies the rotation: {@code out = R * in}. Preserves L2 norm. {@code in} and {@code out} must
   * both have length {@link #dimension()}. {@code in} and {@code out} may be the same array. A new
   * scratch buffer is allocated internally; for hot paths prefer {@link #rotate(float[], float[],
   * float[])} to reuse a caller-owned scratch buffer.
   */
  public void rotate(float[] in, float[] out) {
    rotate(in, out, new float[dim]);
  }

  /**
   * Applies the rotation: {@code out = R * in}, using the provided {@code scratch} buffer of length
   * {@link #dimension()}. This overload is thread-safe as long as the scratch buffer is not shared
   * across concurrent invocations.
   */
  public void rotate(float[] in, float[] out, float[] scratch) {
    if (in.length != dim || out.length != dim || scratch.length != dim) {
      throw new IllegalArgumentException(
          "vector length mismatch: in="
              + in.length
              + ", out="
              + out.length
              + ", scratch="
              + scratch.length
              + ", expected="
              + dim);
    }

    // Step 1: apply sign flips into scratch.
    for (int i = 0; i < dim; i++) {
      scratch[i] = signFlips[i] ? -in[i] : in[i];
    }

    // Step 2: apply permutation from scratch into out.
    for (int i = 0; i < dim; i++) {
      out[permutation[i]] = scratch[i];
    }

    // Step 3: block-diagonal FWHT, in-place on 'out'.
    int offset = 0;
    for (int blockSize : blockSizes) {
      fwht(out, offset, blockSize);
      offset += blockSize;
    }
  }

  /**
   * Applies the inverse rotation: {@code out = R^T * in}. Since the composed transform is
   * orthogonal, the inverse equals the transpose. Internally, because FWHT (normalized) is
   * self-inverse and sign flips / permutations are also self-inverse, the inverse just applies the
   * steps in reverse order. Useful when a caller wants to recover the original vector from a stored
   * rotated one.
   */
  public void inverseRotate(float[] in, float[] out, float[] scratch) {
    if (in.length != dim || out.length != dim || scratch.length != dim) {
      throw new IllegalArgumentException(
          "vector length mismatch: in="
              + in.length
              + ", out="
              + out.length
              + ", scratch="
              + scratch.length
              + ", expected="
              + dim);
    }

    // Copy in to scratch and FWHT each block (self-inverse).
    System.arraycopy(in, 0, scratch, 0, dim);
    int offset = 0;
    for (int blockSize : blockSizes) {
      fwht(scratch, offset, blockSize);
      offset += blockSize;
    }

    // Inverse permutation: if forward did out[perm[i]] = scratch[i], then inverse does
    // result[i] = scratch[perm[i]].
    for (int i = 0; i < dim; i++) {
      out[i] = scratch[permutation[i]];
    }

    // Inverse sign flip (sign flips are self-inverse).
    for (int i = 0; i < dim; i++) {
      if (signFlips[i]) {
        out[i] = -out[i];
      }
    }
  }

  /** Allocating convenience overload of {@link #inverseRotate(float[], float[], float[])}. */
  public void inverseRotate(float[] in, float[] out) {
    inverseRotate(in, out, new float[dim]);
  }

  /**
   * In-place Fast Walsh-Hadamard Transform on a contiguous block, normalized by {@code 1/sqrt(n)}
   * so that the transform is orthogonal (preserves L2 norm).
   */
  private static void fwht(float[] a, int offset, int n) {
    // Standard iterative butterfly.
    for (int len = 1; len < n; len <<= 1) {
      for (int i = 0; i < n; i += len << 1) {
        for (int j = 0; j < len; j++) {
          int u = offset + i + j;
          int v = u + len;
          float x = a[u];
          float y = a[v];
          a[u] = x + y;
          a[v] = x - y;
        }
      }
    }
    // Normalize so that the transform is orthogonal (1/sqrt(n) per element).
    float scale = (float) (1.0 / Math.sqrt(n));
    for (int i = 0; i < n; i++) {
      a[offset + i] *= scale;
    }
  }
}
