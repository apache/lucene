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

package org.apache.lucene.sandbox.codecs.ivfaster_evo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.NamedThreadFactory;

/**
 * Approximate Lloyd clustering using Nitrox2 candidates, FP verification and a movement-bound
 * reaper. A document is re-routed only when its primary and runner-up cells have moved far enough
 * to trade places; a re-route always re-measures the incumbent, so it never moves to a worse cell.
 */
final class Clustering {
  private static final int DEFAULT_SHORTLIST = 32;
  private static final double DEFAULT_CONVERGE_FRACTION = 0.005;
  private static final int MAX_ITERATIONS = 1000;
  private static final double SOAR_LAMBDA = 1.0;

  record Result(
      float[][] centroids, int[] assignments, int iterations, int initialRouted, int[][] spill) {}

  record Options(int shortlist, double convergeFraction, boolean reaper, InfoStream infoStream) {
    static final Options DEFAULT =
        new Options(DEFAULT_SHORTLIST, DEFAULT_CONVERGE_FRACTION, true, InfoStream.NO_OUTPUT);

    Options {
      if (shortlist < 2
          || !Double.isFinite(convergeFraction)
          || convergeFraction < 0
          || convergeFraction > 1) {
        throw new IllegalArgumentException("invalid routing options");
      }
    }
  }

  private final Options options;
  private final int count, carried;
  private final float[][] centroids;
  private final int[] seedAssignments, assignments, runnerUp;
  private final double[] gaps, slack, nearest, maxSlack;
  private final Cascade cascade;

  /** Production entry point: default shortlist, 0.5% convergence, reaper, up to 8 workers. */
  static Result cluster(
      FloatVectorValues input,
      int numCentroids,
      VectorSimilarityFunction similarity,
      float[][] initialCentroids,
      int[] seedAssignments,
      int spillBits,
      double spillMargin,
      InfoStream infoStream)
      throws IOException {
    int cores = Math.min(8, Runtime.getRuntime().availableProcessors());
    return cluster(
        input,
        numCentroids,
        similarity,
        initialCentroids,
        seedAssignments,
        spillBits,
        spillMargin,
        Math.min(cores, Math.max(1, input.size() / 4096)),
        new Options(DEFAULT_SHORTLIST, DEFAULT_CONVERGE_FRACTION, true, infoStream));
  }

  static Result cluster(
      FloatVectorValues input,
      int numCentroids,
      VectorSimilarityFunction similarity,
      float[][] initialCentroids,
      int[] seedAssignments,
      int spillBits,
      double spillMargin,
      int workers,
      Options options)
      throws IOException {
    if (workers < 1) throw new IllegalArgumentException("workers must be positive");
    if (spillBits < 0) throw new IllegalArgumentException("spillBits must be non-negative");
    if (input.size() == 0) return new Result(new float[0][], new int[0], 0, 0, null);
    Clustering state =
        new Clustering(
            input, numCentroids, similarity, initialCentroids, seedAssignments, spillBits, options);
    boolean cosine = similarity == VectorSimilarityFunction.COSINE;
    try (Ranges ranges =
        new Ranges(input, cosine, state.centroids.length, Math.min(workers, input.size()))) {
      return state.run(ranges, similarity, spillBits, spillMargin);
    }
  }

  /** Seeds centroids (inherited first, then sampled) and carries donor assignments. */
  private Clustering(
      FloatVectorValues input,
      int numCentroids,
      VectorSimilarityFunction similarity,
      float[][] initialCentroids,
      int[] seedAssignments,
      int spillBits,
      Options options)
      throws IOException {
    this.options = options;
    this.seedAssignments = seedAssignments;
    count = input.size();
    int dim = input.dimension();
    // Only cosine needs an extra scratch vector; never normalize the reader's shared buffer.
    float[] normalized = similarity == VectorSimilarityFunction.COSINE ? new float[dim] : null;
    int seedCount = initialCentroids == null ? 0 : initialCentroids.length;
    if (seedCount > numCentroids) {
      throw new IllegalArgumentException("seed has more centroids than configured");
    }
    int k = Math.max(seedCount, Math.min(numCentroids, count));
    centroids = new float[k][];
    for (int c = 0; c < seedCount; c++) {
      if (initialCentroids[c].length != dim) {
        throw new IllegalArgumentException("seed dimension differs from vectors");
      }
      centroids[c] = initialCentroids[c].clone();
    }
    // Partial Fisher-Yates sampling needs O(k), rather than O(N), permutation state.
    Map<Integer, Integer> seeds = new HashMap<>();
    Random random = new Random(42);
    for (int c = seedCount; c < k; c++) {
      int sampled = c - seedCount;
      int j = sampled + random.nextInt(count - sampled);
      int seed = seeds.getOrDefault(j, j);
      seeds.put(j, seeds.getOrDefault(sampled, sampled));
      centroids[c] = vector(input, seed, normalized).clone();
      if (similarity != VectorSimilarityFunction.EUCLIDEAN) normalize(centroids[c]);
    }
    assignments = new int[count];
    Arrays.fill(assignments, -1);
    gaps = new double[count];
    nearest = spillBits == 0 ? null : new double[count];
    slack = new double[count];
    maxSlack = spillBits == 0 ? null : new double[count];
    runnerUp = new int[count];
    Arrays.fill(runnerUp, -1);
    int carried = 0;
    if (seedAssignments != null) {
      if (seedAssignments.length != count) {
        throw new IllegalArgumentException("seed assignments differ from vector count");
      }
      for (int i = 0; i < count; i++) {
        int cell = seedAssignments[i];
        if (cell < -1 || cell >= seedCount) {
          throw new IllegalArgumentException("invalid seed assignment");
        }
        if (cell >= 0) {
          assignments[i] = cell;
          // Carry donor membership without routing it against its own centroids again.
          gaps[i] = Double.POSITIVE_INFINITY;
          carried++;
        }
      }
    }
    this.carried = carried;
    long keep = Math.max((long) options.shortlist, 3L * (1L + spillBits));
    cascade =
        new Cascade(
            centroids,
            input instanceof StagedVectors.Values staged && staged.rotated(),
            (int) Math.min(k, keep));
  }

  private Result run(
      Ranges ranges, VectorSimilarityFunction similarity, int spillBits, double spillMargin)
      throws IOException {
    int k = centroids.length, dim = centroids[0].length;
    int convergeAt = (int) (options.convergeFraction * count);
    long started = System.nanoTime();
    ranges.run(w -> route(w, false));
    log(ranges, "initialRouted=" + (count - carried), " shortlist=" + cascade.keep, started);
    // No distance bounds were persisted for carried members. Their first reaper pass must
    // establish fresh bounds after the means move; subsequent passes can skip them normally.
    if (seedAssignments != null) {
      for (int i = 0; i < count; i++) {
        if (seedAssignments[i] >= 0) gaps[i] = 0;
      }
    }
    double[][] sums = new double[k][dim];
    int[] sizes = new int[k];
    // Accumulate once. Routing maintains these unnormalized sums using the vector it already
    // read for each moved document, even when centroid positions themselves are normalized.
    ranges.run(
        w -> {
          for (int i = w.start; i < w.end; i++) {
            int c = assignments[i];
            w.counts[c]++;
            float[] value = vector(w.values, i, w.normalized);
            for (int d = 0; d < dim; d++) w.sums[c][d] += value[d];
          }
          return 0;
        });
    ranges.combine(sums, sizes);
    for (int iteration = 1; iteration <= MAX_ITERATIONS; iteration++) {
      started = System.nanoTime();
      double[] movement = new double[k];
      double maxMovement = 0;
      for (int c = 0; c < k; c++) {
        // Retaining an empty centroid avoids introducing an unrelated reseeding policy.
        if (sizes[c] == 0) continue;
        float[] mean = new float[dim];
        for (int d = 0; d < dim; d++) mean[d] = (float) (sums[c][d] / sizes[c]);
        if (similarity != VectorSimilarityFunction.EUCLIDEAN) normalize(mean);
        movement[c] = distance(centroids[c], mean);
        maxMovement = Math.max(maxMovement, movement[c]);
        centroids[c] = mean;
      }
      cascade.refresh(centroids);
      // Accumulate movement since each document's distances were last measured. Each cell's
      // distance changes by at most its own centroid's displacement, so the primary and runner-up
      // can trade places only if their gap is within the sum of the two. A third cell overtaking
      // both is not covered: like the cascade itself, the reaper is approximate.
      for (int i = 0; i < count; i++) {
        int second = runnerUp[i];
        slack[i] += second < 0 ? 2 * maxMovement : movement[assignments[i]] + movement[second];
        // Spill asks about every cell, not one pair, so it needs the maximum movement.
        if (maxSlack != null) maxSlack[i] += maxMovement;
      }
      int changed = ranges.run(w -> route(w, true));
      ranges.combine(sums, sizes);
      log(ranges, "iteration=" + iteration, " moved=" + changed + " stopAt=" + convergeAt, started);
      if (changed <= convergeAt) {
        int[][] extras = spillBits == 0 || k == 1 ? null : new int[count][];
        if (extras != null) {
          int limit = Math.min(spillBits, k - 1);
          ranges.run(w -> spill(w, limit, spillMargin, extras));
        }
        return new Result(centroids, assignments, iteration, count - carried, extras);
      }
    }
    throw new IOException(
        "IVFasterEvo clustering did not converge after " + MAX_ITERATIONS + " iterations");
  }

  /** Reports and resets the per-pass worker counters. */
  private void log(Ranges ranges, String prefix, String suffix, long started) {
    long routed = 0, verified = 0;
    for (Worker w : ranges.workers) {
      routed += w.routed;
      verified += w.verified;
      w.routed = w.verified = 0;
    }
    if (options.infoStream.isEnabled("IVFE") == false) {
      return;
    }
    double seconds = (System.nanoTime() - started) / 1e9;
    String counters = " routed=" + routed + " fpComparisons=" + verified;
    options.infoStream.message("IVFE", prefix + counters + suffix + " seconds=" + seconds);
  }

  private int route(Worker worker, boolean accumulate) throws IOException {
    int changed = 0;
    for (int i = worker.start; i < worker.end; i++) {
      if (assignments[i] >= 0
          && (gaps[i] == Double.POSITIVE_INFINITY
              || (options.reaper && gaps[i] > slack[i] + 1e-12 * (1 + gaps[i])))) {
        continue;
      }
      // Check the reaper before touching vector storage.
      float[] value = vector(worker.values, i, worker.normalized);
      worker.routed++;
      cascade.select(value, worker, i);
      int best = assignments[i] < 0 ? (int) worker.candidates[0] : assignments[i];
      double first = distance(value, centroids[best]);
      worker.verified++;
      double second = Double.POSITIVE_INFINITY;
      int secondCell = -1;
      for (int candidate = 0; candidate < cascade.keep; candidate++) {
        int c = (int) worker.candidates[candidate];
        if (c == best) continue;
        double dist = distance(value, centroids[c]);
        worker.verified++;
        if (dist < first) {
          second = first;
          secondCell = best;
          first = dist;
          best = c;
        } else if (dist < second) {
          second = dist;
          secondCell = c;
        }
      }
      if (assignments[i] != best) {
        if (accumulate) {
          int old = assignments[i];
          worker.counts[old]--;
          worker.counts[best]++;
          for (int d = 0; d < value.length; d++) {
            worker.sums[old][d] -= value[d];
            worker.sums[best][d] += value[d];
          }
        }
        assignments[i] = best;
        changed++;
      }
      if (nearest != null) {
        nearest[i] = first;
        maxSlack[i] = 0;
      }
      runnerUp[i] = secondCell;
      gaps[i] = second - first;
      slack[i] = 0;
    }
    return changed;
  }

  // Widen the final reaper pass only. With S the maximum cell movement accumulated since the
  // document was measured, every cell's distance has moved by at most S, so
  //   fresh gap >= stale gap - 2S   and   fresh primary distance <= stale distance + S.
  // A document is provably interior, and keeps a single cell, when
  //   stale gap > (margin - 1) * stale distance + (margin + 1) * S.
  //
  // Boundary documents spill by SOAR loss (Sun et al., NeurIPS 2023) rather than by distance:
  //   loss(c) = |v - c|^2 + lambda * (r . (v - c))^2 / |r|^2, where r = v - primary.
  // The next-nearest centroids tend to lie along the primary residual, so a query that misses
  // the primary misses them too; the penalty prefers cells covering complementary directions.
  private int spill(Worker worker, int limit, double spillMargin, int[][] extras)
      throws IOException {
    int[] cells = new int[limit];
    double[] losses = new double[limit];
    double[] distances = new double[cascade.keep];
    float[] residual = new float[centroids[0].length];
    for (int i = worker.start; i < worker.end; i++) {
      if (gaps[i]
          > (spillMargin - 1) * nearest[i]
              + (spillMargin + 1) * maxSlack[i]
              + 1e-12 * (1 + nearest[i])) continue;
      float[] value = vector(worker.values, i, worker.normalized);
      float[] primary = centroids[assignments[i]];
      double threshold = spillMargin * distance(value, primary);
      cascade.select(value, worker, i);
      boolean boundary = false;
      for (int candidate = 0; candidate < cascade.keep; candidate++) {
        int c = (int) worker.candidates[candidate];
        distances[candidate] = c == assignments[i] ? 0 : distance(value, centroids[c]);
        boundary |= c != assignments[i] && distances[candidate] <= threshold;
      }
      if (boundary == false) continue;
      double residualNorm = 0;
      for (int d = 0; d < residual.length; d++) {
        residual[d] = value[d] - primary[d];
        residualNorm += (double) residual[d] * residual[d];
      }
      int size = 0;
      for (int candidate = 0; candidate < cascade.keep; candidate++) {
        int c = (int) worker.candidates[candidate];
        if (c == assignments[i]) continue;
        // The penalty is non-negative, so the squared distance alone is a lower bound.
        double loss = distances[candidate] * distances[candidate];
        if (size == limit && loss >= losses[limit - 1]) continue;
        if (residualNorm > 0) {
          double parallel = 0;
          for (int d = 0; d < residual.length; d++) {
            parallel += ((double) value[d] - centroids[c][d]) * residual[d];
          }
          loss += SOAR_LAMBDA * parallel * parallel / residualNorm;
        }
        int pos = size;
        while (pos > 0 && loss < losses[pos - 1]) pos--;
        if (pos >= limit) continue;
        for (int j = Math.min(size, limit - 1); j > pos; j--) {
          cells[j] = cells[j - 1];
          losses[j] = losses[j - 1];
        }
        cells[pos] = c;
        losses[pos] = loss;
        size = Math.min(size + 1, limit);
      }
      extras[i] = ArrayUtil.copyOfSubArray(cells, 0, size);
    }
    return 0;
  }

  /** Nitrox2 shortlist of the nearest centroid codes, ties broken by cell ordinal. */
  private static final class Cascade {
    final TierCodec codec;
    final byte[][] codes;
    final int keep;

    private final boolean rotated;

    Cascade(float[][] centroids, boolean rotated, int keep) {
      this.rotated = rotated;
      this.keep = keep;
      codec = new TierCodec(IVFasterEvoVectorsFormat.Tier.NITROX2, centroids[0].length);
      codes = new byte[centroids.length][];
      for (int c = 0; c < codes.length; c++) codes[c] = encode(centroids[c]);
    }

    private byte[] encode(float[] centroid) {
      if (rotated == false) return codec.encode(centroid);
      byte[] code = new byte[codec.bytes];
      codec.encodeRotatedNitrox(centroid.clone(), code);
      return code;
    }

    void refresh(float[][] centroids) {
      if (keep == codes.length) return;
      for (int c = 0; c < codes.length; c++) codes[c] = encode(centroids[c]);
    }

    void select(float[] vector, Worker worker, int ord) throws IOException {
      if (keep == codes.length) {
        for (int c = 0; c < keep; c++) worker.candidates[c] = c;
        return;
      }
      // Staged records already hold the document's code; other inputs are encoded on the fly.
      if (worker.values instanceof StagedVectors.Values staged) staged.coarseCode(ord, worker.code);
      else codec.encodeNitrox(vector, worker.code, worker.rotated);
      int size = 0;
      for (int c = 0; c < codes.length; c++) {
        long key = ((long) VectorKernels.INSTANCE.hamming(worker.code, codes[c]) << 32) | c;
        if (size == keep && key >= worker.candidates[keep - 1]) continue;
        int pos = size;
        while (pos > 0 && key < worker.candidates[pos - 1]) pos--;
        for (int j = Math.min(size, keep - 1); j > pos; j--) {
          worker.candidates[j] = worker.candidates[j - 1];
        }
        worker.candidates[pos] = key;
        size = Math.min(size + 1, keep);
      }
    }
  }

  @FunctionalInterface
  private interface RangeTask {
    int run(Worker worker) throws IOException;
  }

  private static final class Worker {
    final FloatVectorValues values;
    final float[] normalized, rotated;
    final int start, end;
    final double[][] sums;
    final int[] counts;
    final byte[] code;
    final long[] candidates;
    long routed, verified;

    Worker(FloatVectorValues values, boolean cosine, int k, int start, int end) {
      int dim = values.dimension();
      this.values = values;
      this.normalized = cosine ? new float[dim] : null;
      this.rotated = new float[dim];
      this.start = start;
      this.end = end;
      this.sums = new double[k][dim];
      this.counts = new int[k];
      this.code = new byte[TierCodec.nitroxBytes(dim)];
      this.candidates = new long[k];
    }
  }

  /** Disjoint ordinal ranges; centroids stay read-only until every worker has finished. */
  private static final class Ranges implements AutoCloseable {
    final List<Worker> workers = new ArrayList<>();
    final ExecutorService pool;
    final TaskExecutor executor;

    Ranges(FloatVectorValues input, boolean cosine, int k, int count) throws IOException {
      int size = input.size();
      // Clone on the caller thread: no worker shares a reader's mutable scratch vector or cursor.
      for (int w = 0; w < count; w++) {
        int start = (int) ((long) size * w / count);
        int end = (int) ((long) size * (w + 1) / count);
        workers.add(new Worker(count == 1 ? input : input.copy(), cosine, k, start, end));
      }
      pool =
          count == 1
              ? null
              : Executors.newFixedThreadPool(count - 1, new NamedThreadFactory("ivfe-route"));
      executor = new TaskExecutor(pool == null ? Runnable::run : pool);
    }

    int run(RangeTask task) throws IOException {
      List<Callable<Integer>> tasks = new ArrayList<>();
      for (Worker worker : workers) tasks.add(() -> task.run(worker));
      int changed = 0;
      for (int result : executor.invokeAll(tasks)) changed += result;
      return changed;
    }

    void combine(double[][] sums, int[] sizes) {
      // Fixed range order makes reduction independent of worker scheduling. These are deltas,
      // not partial means; only normalize the derived centroid positions after this barrier.
      for (Worker worker : workers) {
        for (int c = 0; c < sizes.length; c++) {
          sizes[c] += worker.counts[c];
          worker.counts[c] = 0;
          for (int d = 0; d < sums[c].length; d++) {
            sums[c][d] += worker.sums[c][d];
            worker.sums[c][d] = 0;
          }
        }
      }
      for (int c = 0; c < sizes.length; c++) {
        assert sizes[c] >= 0;
        if (sizes[c] == 0) Arrays.fill(sums[c], 0);
      }
    }

    @Override
    public void close() {
      if (pool != null) pool.close();
    }
  }

  private static float[] vector(FloatVectorValues values, int ord, float[] normalized)
      throws IOException {
    float[] value = values.vectorValue(ord);
    if (normalized == null) return value;
    System.arraycopy(value, 0, normalized, 0, value.length);
    normalize(normalized);
    return normalized;
  }

  static double distance(float[] a, float[] b) {
    return VectorKernels.INSTANCE.distance(a, b);
  }

  static void normalize(float[] vector) {
    double norm = 0;
    for (float v : vector) {
      norm += (double) v * v;
    }
    if (norm == 0) {
      vector[0] = 1;
    } else {
      norm = Math.sqrt(norm);
      for (int d = 0; d < vector.length; d++) {
        vector[d] = (float) (vector[d] / norm);
      }
    }
  }
}
