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
package org.apache.lucene.sandbox.codecs.segmentivf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.sandbox.codecs.segmentivf.Centroids.CentroidCodes;
import org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsWriter.StagedVectors;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.ArrayUtil;

/**
 * Builds IVF cells and spill assignments with iterative spherical clustering.
 *
 * <p>Movement bounds avoid rerouting vectors whose nearest-cell decision cannot have changed, and
 * spill cells preserve recall for vectors near cell boundaries.
 */
final class Clustering {
  static final long SEED = 42L, FIX = 1L << 30;
  static final int SHORTLIST = 8, MAX_ITERS = 10;
  static final float MARGIN =
      Float.parseFloat(System.getProperty("segmentivf.spillMargin", "1.05"));
  static final float REAP_MARGIN = 1.02f, SOAR_LAMBDA = 1f, CONVERGE_FRACTION = 0.005f;

  private Clustering() {}

  record WarmState(int[] assignment, int[] cell2, float[] d1, float[] d2) {}

  record Result(float[][] centroids, int[] cells, int stride, int[] assignment, int[] cell2) {
    int cellCount(int i) {
      int n = 0;
      while (n < stride && cells[i * stride + n] >= 0) n++;
      return n;
    }

    int cell(int i, int k) {
      return cells[i * stride + k];
    }
  }

  static Result cluster(StagedVectors src, int nlist, float[][] seed, WarmState warm, int spillBits)
      throws IOException {
    Run run = new Run(src, nlist, seed, spillBits);
    run.routeAll(warm);
    int count = src.count, convergeAt = (int) (CONVERGE_FRACTION * count), iterations = 0;
    do {
      run.updateCentroids();
    } while (run.reap(false) > convergeAt && ++iterations < MAX_ITERS);
    run.reap(true);
    // Vectors the final reap skipped, or all vectors when spilling is off, keep only their primary.
    for (int i = 0; i < count; i++) {
      if (run.cells[i * run.stride] < 0) run.cells[i * run.stride] = run.assignment[i];
    }
    return new Result(run.centroids, run.cells, run.stride, run.assignment, run.cell2);
  }

  private static final class Run {
    final StagedVectors src;
    final int count, dim, nlist, spillBits, stride;
    final float[][] centroids;
    final CentroidCodes codes;
    final int[] assignment, cell2, cells, members;
    final float[] d1, d2, movement, docSlack, docMaxSlack;
    final long[] sums;
    final Object[] locks;

    Run(StagedVectors src, int nlist, float[][] seed, int spillBits) throws IOException {
      this.src = src;
      this.nlist = nlist;
      this.spillBits = spillBits;
      count = src.count;
      dim = src.dim;
      stride = 1 + spillBits;
      centroids = seed != null ? new float[seed.length][] : sampleCentroids();
      if (centroids.length != nlist) {
        throw new IllegalArgumentException(
            "seed has " + centroids.length + " centroids but nlist is " + nlist);
      }
      for (int c = 0; c < nlist; c++) {
        if (seed != null) centroids[c] = ArrayUtil.copyOfSubArray(seed[c], 0, dim);
        normalize(centroids[c]);
      }
      codes = new CentroidCodes(centroids, dim, null);
      assignment = new int[count];
      cell2 = new int[count];
      d1 = new float[count];
      d2 = new float[count];
      docSlack = new float[count];
      docMaxSlack = new float[count];
      movement = new float[nlist];
      members = new int[nlist];
      sums = new long[Math.multiplyExact(nlist, dim)];
      cells = new int[Math.multiplyExact(count, stride)];
      Arrays.fill(cells, -1);
      locks = new Object[Math.min(1024, Integer.highestOneBit(Math.max(1, nlist)) << 1)];
      for (int i = 0; i < locks.length; i++) locks[i] = new Object();
    }

    float[][] sampleCentroids() throws IOException {
      int k = Math.min(nlist, count);
      Random random = new Random(SEED);
      int[] pick = new int[k];
      for (int i = 0; i < k; i++) pick[i] = i;
      for (int i = k; i < count; i++) {
        int j = random.nextInt(i + 1);
        if (j < k) pick[j] = i;
      }
      float[][] out = new float[nlist][dim];
      StagedVectors.Cursor cur = src.cursor();
      for (int c = 0; c < nlist && k > 0; c++) {
        cur.load(pick[c % k]);
        System.arraycopy(cur.vector(), 0, out[c], 0, dim);
      }
      return out;
    }

    void apply(int cell, float[] v, int sign) {
      int base = cell * dim;
      synchronized (locks[cell & (locks.length - 1)]) {
        for (int d = 0; d < dim; d++) sums[base + d] += sign * Math.round((double) v[d] * FIX);
        members[cell] += sign;
      }
    }

    void routeAll(WarmState warm) throws IOException {
      int shortlist = Math.min(nlist, SHORTLIST);
      Parallel.overRange(
          count,
          (lo, hi) -> {
            CentroidCodes.Scratch scratch = new CentroidCodes.Scratch(dim, nlist, shortlist);
            CentroidCodes.Routing routing = new CentroidCodes.Routing(2);
            StagedVectors.Cursor cur = src.cursor();
            for (int i = lo; i < hi; i++) {
              cur.load(i);
              float[] vector = cur.vector();
              int carried = warm == null ? -1 : warm.assignment[i];
              if (carried >= 0 && carried < nlist) {
                assignment[i] = carried;
                if (Float.isNaN(warm.d1[i])) {
                  // The seed centroids may combine several source segments, so a carried
                  // runner-up is stale. Treat it as adjacent to force a safe first reap.
                  d1[i] = d2[i] = codes.exactDistance(vector, carried);
                  cell2[i] = -1;
                } else {
                  d1[i] = warm.d1[i];
                  d2[i] = warm.d2[i];
                  cell2[i] = warm.cell2[i];
                }
              } else {
                cur.coarseInto(scratch.qCode);
                codes.routePacked(vector, shortlist, 2, routing, scratch);
                assignment[i] = routing.count > 0 ? routing.cells[0] : 0;
                d1[i] = routing.d1;
                d2[i] = routing.d2;
                cell2[i] = routing.cell2;
              }
              apply(assignment[i], vector, 1);
            }
          });
    }

    void updateCentroids() throws IOException {
      Parallel.overRange(
          nlist,
          (from, to) -> {
            float[] prev = new float[dim];
            for (int c = from; c < to; c++) {
              movement[c] = 0f;
              if (members[c] == 0) continue;
              float[] cent = centroids[c];
              System.arraycopy(cent, 0, prev, 0, dim);
              double inv = 1.0 / ((double) members[c] * FIX), moved = 0;
              for (int d = 0; d < dim; d++) cent[d] = (float) (sums[c * dim + d] * inv);
              normalize(cent);
              for (int d = 0; d < dim; d++) {
                double delta = (double) cent[d] - prev[d];
                moved += delta * delta;
              }
              movement[c] = (float) Math.sqrt(moved);
            }
          });
      codes.encodeAll();
      float maxMove = 0f;
      for (float m : movement) if (m > maxMove) maxMove = m;
      for (int i = 0; i < count; i++) {
        docSlack[i] += cell2[i] >= 0 ? movement[assignment[i]] + movement[cell2[i]] : 2f * maxMove;
        docMaxSlack[i] += maxMove;
      }
    }

    int reap(boolean last) throws IOException {
      boolean withSpill = last && spillBits > 0;
      int keep = withSpill ? stride : 2;
      int shortlist = Math.min(nlist, SHORTLIST);
      AtomicInteger changed = new AtomicInteger();
      Parallel.overRange(
          count,
          (lo, hi) -> {
            CentroidCodes.Scratch scratch = new CentroidCodes.Scratch(dim, nlist, shortlist);
            CentroidCodes.Routing routing = new CentroidCodes.Routing(keep);
            int[] cands = new int[1 + keep];
            float[] residual = new float[dim], loss = new float[spillBits];
            int localChanged = 0;
            StagedVectors.Cursor cur = src.cursor();
            for (int i = lo; i < hi; i++) {
              float gap = d2[i] - d1[i], wide = docMaxSlack[i] * (2f + MARGIN);
              if (gap > docSlack[i]
                  && CentroidCodes.withinMargin(d1[i], d2[i], REAP_MARGIN) == false
                  && (withSpill == false || gap > (MARGIN - 1f) * Math.abs(d1[i]) + wide)) {
                continue;
              }
              cur.load(i);
              float[] vector = cur.vector();
              cur.coarseInto(scratch.qCode);
              codes.routePacked(vector, shortlist, keep, routing, scratch);
              int incumbent = assignment[i], n = routing.count;
              float incumbentDist = codes.exactDistance(vector, incumbent);
              int[] spillCands = routing.cells;
              if (n > 0 && routing.d1 <= incumbentDist) {
                if (routing.cells[0] != incumbent) {
                  localChanged++;
                  apply(incumbent, vector, -1);
                  apply(routing.cells[0], vector, 1);
                }
                assignment[i] = routing.cells[0];
                d1[i] = routing.d1;
                d2[i] = routing.d2;
                cell2[i] = routing.cell2;
              } else {
                d1[i] = incumbentDist;
                d2[i] = n > 0 ? routing.d1 : Float.MAX_VALUE;
                cell2[i] = n > 0 ? routing.cells[0] : -1;
                cands[0] = incumbent;
                System.arraycopy(routing.cells, 0, cands, 1, n);
                spillCands = cands;
                n++;
              }
              docSlack[i] = docMaxSlack[i] = 0f;
              if (withSpill) spill(vector, spillCands, n, i, residual, loss);
            }
            changed.addAndGet(localChanged);
          });
      return changed.get();
    }

    void spill(float[] vector, int[] cands, int nCand, int doc, float[] r1, float[] bestLoss) {
      int base = doc * stride;
      Arrays.fill(cells, base, base + stride, -1);
      int primary = cells[base] = nCand > 0 ? cands[0] : 0;
      if (nCand <= 1 || CentroidCodes.withinMargin(d1[doc], d2[doc], MARGIN) == false) return;
      float[] pc = centroids[primary];
      double r1NormSq = 0;
      for (int d = 0; d < dim; d++) {
        r1[d] = vector[d] - pc[d];
        r1NormSq += (double) r1[d] * r1[d];
      }
      if (r1NormSq == 0) {
        for (int i = 1; i < nCand && i <= spillBits; i++) cells[base + i] = cands[i];
        return;
      }
      double invR1NormSq = 1.0 / r1NormSq;
      int filled = 0;
      for (int ci = 1; ci < nCand; ci++) {
        int c = cands[ci];
        if (c == primary || c < 0) continue;
        float[] cc = centroids[c];
        double resNormSq = 0, dotR1 = 0;
        for (int d = 0; d < dim; d++) {
          double e = vector[d] - cc[d];
          resNormSq += e * e;
          dotR1 += e * r1[d];
        }
        float loss = (float) (resNormSq + SOAR_LAMBDA * dotR1 * dotR1 * invR1NormSq);
        if (filled == spillBits && loss >= bestLoss[spillBits - 1]) continue;
        int pos = filled < spillBits ? filled++ : spillBits - 1;
        for (; pos > 0 && bestLoss[pos - 1] > loss; pos--) {
          bestLoss[pos] = bestLoss[pos - 1];
          cells[base + 1 + pos] = cells[base + pos];
        }
        bestLoss[pos] = loss;
        cells[base + 1 + pos] = c;
      }
    }
  }

  private static void normalize(float[] v) {
    double norm = 0;
    for (float x : v) norm += (double) x * x;
    if (norm == 0) return;
    float inv = (float) (1.0 / Math.sqrt(norm));
    for (int d = 0; d < v.length; d++) v[d] *= inv;
  }

  static final class Parallel {
    private static final int MIN_PER_THREAD = 4096;
    static final int WORKERS =
        Math.max(
            1,
            Integer.getInteger(
                "segmentivf.buildThreads", Runtime.getRuntime().availableProcessors()));
    private static final TaskExecutor EXEC =
        new TaskExecutor(
            Executors.newFixedThreadPool(
                WORKERS, Thread.ofPlatform().name("segmentivf-build-", 0).daemon(true).factory()));

    private Parallel() {}

    interface RangeTask {
      void run(int from, int to) throws IOException;
    }

    static void overRange(int count, RangeTask body) throws IOException {
      overRange(count, MIN_PER_THREAD, body);
    }

    static void overRange(int count, int minPerThread, RangeTask body) throws IOException {
      int tasks = Math.min(WORKERS, Math.max(1, count / minPerThread));
      if (tasks <= 1) {
        body.run(0, count);
        return;
      }
      int chunk = (count + tasks - 1) / tasks;
      List<Callable<Void>> work = new ArrayList<>(tasks);
      for (int t = 0; t < tasks; t++) {
        int from = t * chunk, to = Math.min(count, from + chunk);
        if (from >= to) continue;
        work.add(
            () -> {
              body.run(from, to);
              return null;
            });
      }
      EXEC.invokeAll(work);
    }
  }

  /**
   * Retains recent centroid state so later segments can warm-start clustering.
   *
   * <p>Flushes and merges create different physical parts of the same index. Reusing compatible
   * centroids and assignments keeps that earlier clustering work from being discarded and reduces
   * repeated indexing cost. Only segments this process flushed or merged are seeds, never segments
   * found on disk: a writer opened with {@code OpenMode.CREATE} still has the previous index's
   * files in its directory until its first commit, and must not cluster from them.
   */
  static final class HotStart {
    private static final Map<Directory, Map<String, List<Seed>>> INDEXES = new WeakHashMap<>();

    record Seed(
        String segment,
        String lineage,
        int vectors,
        float[][] centroids,
        int[] members,
        int[] assignment,
        int[] cell2) {}

    private static String key(FieldInfo info) {
      return info.name + "/" + info.getVectorDimension() + "/" + info.getVectorSimilarityFunction();
    }

    static synchronized void clear() {
      INDEXES.clear();
    }

    static synchronized void publish(
        Directory dir,
        String segment,
        String lineage,
        FieldInfo info,
        Clustering.Result cl,
        int vectors) {
      put(dir, segment, lineage, info, vectors, cl.centroids(), cl.assignment(), cl.cell2());
    }

    private static void put(
        Directory dir,
        String segment,
        String lineage,
        FieldInfo info,
        int vectors,
        float[][] centroids,
        int[] assignment,
        int[] cell2) {
      var fields = INDEXES.computeIfAbsent(dir, _ -> new HashMap<>());
      List<Seed> seeds = fields.computeIfAbsent(key(info), _ -> new ArrayList<>());
      seeds.removeIf(seed -> seed.segment.equals(segment));
      int[] members = new int[centroids.length];
      if (assignment != null) {
        for (int cell : assignment) if (cell >= 0 && cell < members.length) members[cell]++;
      }
      seeds.add(new Seed(segment, lineage, vectors, centroids, members, assignment, cell2));
      seeds.sort(Comparator.comparingInt(Seed::vectors).reversed());
    }

    static synchronized Seed snapshot(Directory dir, String segment, FieldInfo info) {
      for (Seed seed : INDEXES.getOrDefault(dir, Map.of()).getOrDefault(key(info), List.of())) {
        if (seed.segment.equals(segment)) return seed;
      }
      return null;
    }

    record Source(float[][] centroids, int[] members, String lineage, int vectors) {}

    /**
     * Returns the largest source clustered into {@code nlist} cells, or -1. Every segment trains
     * exactly the configured cell count, so this only skips segments of another configuration.
     * Flushes and merges pick their donor with this same rule.
     */
    static int donor(Source[] sources, int nlist) {
      int donor = -1;
      for (int i = 0; i < sources.length; i++) {
        if (sources[i] == null || sources[i].centroids.length != nlist) continue;
        if (donor < 0 || sources[i].vectors > sources[donor].vectors) donor = i;
      }
      return donor;
    }

    static boolean sameLineage(Source source, Source donor) {
      return source == donor
          || (source != null
              && source.lineage != null
              && source.lineage.equals(donor.lineage)
              && source.centroids.length == donor.centroids.length);
    }

    static synchronized Seed seed(Directory dir, String writing, FieldInfo info, int nlist)
        throws IOException {
      List<Seed> seeds = INDEXES.getOrDefault(dir, Map.of()).get(key(info));
      if (seeds == null) return null;
      Set<String> live = new HashSet<>();
      for (String file : dir.listAll()) live.add(IndexFileNames.parseSegmentName(file));
      seeds.removeIf(s -> s.segment.equals(writing) || live.contains(s.segment) == false);
      Source[] sources = new Source[seeds.size()];
      for (int i = 0; i < sources.length; i++) {
        Seed s = seeds.get(i);
        sources[i] = new Source(s.centroids, s.members, s.lineage, s.vectors);
      }
      int donor = donor(sources, nlist);
      if (donor < 0) return null;
      Seed d = seeds.get(donor);
      return new Seed(
          d.segment, d.lineage, d.vectors, weightedCentroids(sources, donor), null, null, null);
    }

    static float[][] weightedCentroids(Source[] sources, int donor) {
      float[][] from = sources[donor].centroids;
      int nlist = from.length, dim = from[0].length;
      float[][] seed = new float[nlist][dim];
      long[] weights = new long[nlist];
      for (Source source : sources) {
        // sameLineage is false for a null source, so it also guards the members check.
        if (sameLineage(source, sources[donor]) == false || source.members == null) continue;
        for (int c = 0; c < nlist; c++) {
          int weight = source.members[c];
          if (weight == 0) continue;
          weights[c] += weight;
          float[] centroid = source.centroids[c], target = seed[c];
          for (int d = 0; d < dim; d++) target[d] += weight * centroid[d];
        }
      }
      for (int c = 0; c < nlist; c++) {
        if (weights[c] == 0) System.arraycopy(from[c], 0, seed[c], 0, dim);
        else normalize(seed[c]);
      }
      return seed;
    }

    private static void normalize(float[] vector) {
      double norm = 0;
      for (float value : vector) norm += (double) value * value;
      if (norm == 0) return;
      float scale = (float) (1.0 / Math.sqrt(norm));
      for (int d = 0; d < vector.length; d++) vector[d] *= scale;
    }
  }
}
