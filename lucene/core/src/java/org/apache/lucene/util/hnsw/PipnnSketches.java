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
package org.apache.lucene.util.hnsw;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.Float16VectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.VectorUtil;

/**
 * Low dimensional random projections ("sketches") of every vector, used by {@link
 * PipnnGraphBuilder} to hash the direction from a node to a candidate neighbor.
 *
 * <p>With {@code m} random hyperplanes {@code r_j}, the sign of {@code <r_j, c - p>} equals the
 * sign of {@code sketch(c)_j - sketch(p)_j}, so the {@code m}-bit locality sensitive hash of the
 * residual {@code c - p} only needs the two precomputed sketches rather than the full vectors.
 *
 * @lucene.internal
 */
final class PipnnSketches implements Accountable {

  private static final long BASE_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(PipnnSketches.class);

  private final int bits;
  private final float[] sketches;

  private PipnnSketches(int bits, float[] sketches) {
    this.bits = bits;
    this.sketches = sketches;
  }

  /** Single threaded {@link #compute(KnnVectorValues, int, int, long, TaskExecutor, int)}. */
  static PipnnSketches compute(KnnVectorValues vectors, int size, int bits, long seed)
      throws IOException {
    return compute(vectors, size, bits, seed, null, 1);
  }

  /**
   * Projects the first {@code size} vectors onto {@code bits} random Gaussian directions.
   *
   * @param bits number of hash bits, in [1, 16]
   * @param executor optional executor; when set, the vectors are split into {@code numWorkers}
   *     ranges projected in parallel. The result does not depend on the scheduling.
   */
  static PipnnSketches compute(
      KnnVectorValues vectors, int size, int bits, long seed, TaskExecutor executor, int numWorkers)
      throws IOException {
    if (bits <= 0 || bits > 16) {
      throw new IllegalArgumentException("bits must be in [1, 16], got " + bits);
    }
    final int dim = vectors.dimension();
    final float[][] directions = randomDirections(bits, dim, seed);
    final float[] sketches = new float[Math.multiplyExact(size, bits)];
    if (executor == null || numWorkers <= 1 || size < 2 * CHUNK) {
      project(vectors, 0, size, directions, sketches);
    } else {
      final AtomicInteger next = new AtomicInteger();
      List<Callable<Void>> tasks = new ArrayList<>(numWorkers);
      for (int w = 0; w < numWorkers; w++) {
        tasks.add(
            () -> {
              KnnVectorValues copy = vectors.copy();
              int from;
              while ((from = next.getAndAdd(CHUNK)) < size) {
                project(copy, from, Math.min(size, from + CHUNK), directions, sketches);
              }
              return null;
            });
      }
      executor.invokeAll(tasks);
    }
    return new PipnnSketches(bits, sketches);
  }

  /** Vectors projected per unit of parallel work. */
  private static final int CHUNK = 4096;

  private static void project(
      KnnVectorValues vectors, int from, int to, float[][] directions, float[] sketches)
      throws IOException {
    final int bits = directions.length;
    final float[] scratch = new float[vectors.dimension()];
    for (int ord = from; ord < to; ord++) {
      float[] vector = asFloats(vectors, ord, scratch);
      for (int j = 0; j < bits; j++) {
        sketches[ord * bits + j] = VectorUtil.dotProduct(directions[j], vector);
      }
    }
  }

  /** Hash of the direction from {@code from} to {@code to}: one bit per projection. */
  int hash(int from, int to) {
    final int baseFrom = from * bits;
    final int baseTo = to * bits;
    int h = 0;
    for (int j = 0; j < bits; j++) {
      if (sketches[baseTo + j] > sketches[baseFrom + j]) {
        h |= 1 << j;
      }
    }
    return h;
  }

  int bits() {
    return bits;
  }

  private static float[][] randomDirections(int count, int dim, long seed) {
    SplittableRandom random = new SplittableRandom(seed);
    float[][] directions = new float[count][dim];
    for (float[] direction : directions) {
      for (int i = 0; i < dim; i++) {
        direction[i] = (float) random.nextGaussian();
      }
    }
    return directions;
  }

  private static float[] asFloats(KnnVectorValues vectors, int ord, float[] scratch)
      throws IOException {
    switch (vectors.getEncoding()) {
      case FLOAT32:
        return ((FloatVectorValues) vectors).vectorValue(ord);
      case BYTE:
        {
          byte[] bytes = ((ByteVectorValues) vectors).vectorValue(ord);
          for (int i = 0; i < scratch.length; i++) {
            scratch[i] = bytes[i];
          }
          return scratch;
        }
      case FLOAT16:
        {
          short[] halves = ((Float16VectorValues) vectors).vectorValue(ord);
          for (int i = 0; i < scratch.length; i++) {
            scratch[i] = Float.float16ToFloat(halves[i]);
          }
          return scratch;
        }
    }
    throw new AssertionError("unknown encoding " + vectors.getEncoding());
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(sketches);
  }
}
