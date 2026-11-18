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

import java.util.Arrays;
import org.apache.lucene.util.VectorUtil;

/**
 * A segment's centroids and their codes, so a vector can be routed to its nearest cells: shortlist
 * by coarse distance, then rank the shortlist exactly.
 *
 * <p>Each centroid has one code, in the coarse tier's centroid encoding ({@link
 * CoarseCodec#encodeCentroid}), for S1 its sign bits. A vector encoded the same way, or a prepared
 * query, which leads with its centroid code, is compared with them by {@link
 * CoarseCodec#centroidDistance}, so routing while indexing, the centroid graph's construction and
 * its walk at query time make the same comparison. (S1's query takes its sign bits from its INT8
 * levels, so a component too small to reach one level reads as 0 where a vector's code reads its
 * sign: the walk only shortlists, and the exact ranking below settles the order.) The coarse
 * distance only has to keep the nearest cells in a shortlist; the shortlist is ranked by the exact
 * dot product with the float centroids, which are unit length, so the exact distance is the negated
 * dot product.
 *
 * <p>Indexing routes every vector by scanning all centroid codes ({@link #routeByShortlist}); a
 * query instead walks the {@link CentroidGraph} over the same codes and ranks what it found with
 * {@link #rankCandidates}.
 */
final class CentroidCodes {
  /** Centroid codes are scored this many at a time. */
  private static final int TILE = 512;

  final float[][] centroids;
  final CoarseCodec codec;
  final int nlist;

  /** Bytes of a centroid code: {@code codec.centroidBytes}. */
  final int codeBytes;

  /** Every centroid's code, in cell order. */
  final byte[] codes;

  CentroidCodes(float[][] centroids, CoarseCodec codec) {
    this.nlist = centroids.length;
    this.centroids = centroids;
    this.codec = codec;
    this.codeBytes = codec.centroidBytes;
    this.codes = new byte[nlist * codeBytes];
    encodeAll();
  }

  /** Re-encodes every centroid; clustering calls this after each centroid update. */
  void encodeAll() {
    for (int c = 0; c < nlist; c++) {
      codec.encodeCentroid(centroids[c], codes, c * codeBytes);
    }
  }

  /** Prepares {@code vector} for comparison with the centroid codes. */
  void prepare(float[] vector, byte[] prepared) {
    codec.encodeQuery(vector, prepared, 0);
  }

  /** As {@link #prepare(float[], byte[])}, reusing the query's INT8 {@code levels}. */
  void prepare(float[] vector, QueryLevels levels, byte[] prepared) {
    codec.encodeQuery(vector, levels, prepared, 0);
  }

  /** Writes the exact distance from {@code vector} to each of {@code count} candidate cells. */
  void rankCandidates(float[] vector, int[] candidates, int count, float[] out) {
    for (int i = 0; i < count; i++) {
      out[i] = exactDistance(vector, candidates[i]);
    }
  }

  /**
   * The result of {@link #routeByShortlist}: the {@code count} nearest cells in order, and the
   * exact distances to the nearest and second-nearest cells, which clustering uses for its movement
   * bounds and spill decision; a missing second distance is {@code Float.MAX_VALUE}.
   */
  static final class Routing {
    final int[] cells;
    int count;
    float nearestDist;
    float secondDist;

    Routing(int capacity) {
      cells = new int[capacity];
    }
  }

  /** Per-thread buffers for {@link #routeByShortlist}, sized once per clustering pass. */
  static final class Scratch {
    final int[] coarseDist = new int[TILE];

    /** Every centroid's coarse distance, by cell. */
    final int[] distances;

    /** How many centroids are at each coarse distance; zero outside the last routing's range. */
    final int[] counts;

    final int[] shortlistCells;
    final float[] shortlistDist;

    /** The routed vector, prepared as a query. */
    final byte[] query;

    Scratch(CentroidCodes codes, int shortlistSize) {
      distances = new int[codes.nlist];
      counts = new int[codes.codec.bins];
      shortlistCells = new int[shortlistSize];
      shortlistDist = new float[shortlistSize];
      query = new byte[codes.codec.queryBytes];
    }
  }

  /**
   * Routes one vector while indexing: scans every centroid's code, keeps the {@code shortlistSize}
   * nearest, ranks those exactly, and reports the best {@code keep} cells in {@code out}. {@code
   * scratch.query} must already hold the prepared vector.
   *
   * <p>Coarse distances are small integers, so the shortlist is selected by counting, not with a
   * heap: one pass records every distance and counts how many centroids have each, the counts give
   * the distance at which the shortlist fills, and a second pass collects the centroids nearer than
   * it and enough of those at it, in cell order.
   */
  void routeByShortlist(float[] vector, int shortlistSize, int keep, Routing out, Scratch scratch) {
    int want = Math.min(shortlistSize, nlist);
    int[] coarseDist = scratch.coarseDist;
    int[] distances = scratch.distances;
    int[] counts = scratch.counts;
    int nearest = Integer.MAX_VALUE;
    int farthest = 0;
    for (int base = 0; base < nlist; base += TILE) {
      int rows = Math.min(TILE, nlist - base);
      codec.centroidDistances(scratch.query, codes, base * codeBytes, rows, coarseDist);
      for (int r = 0; r < rows; r++) {
        int distance = coarseDist[r];
        distances[base + r] = distance;
        counts[distance]++;
        nearest = Math.min(nearest, distance);
        farthest = Math.max(farthest, distance);
      }
    }
    // The distance at which the shortlist fills, and how many centroids at it still fit.
    int bound = nearest;
    int nearer = 0;
    while (nearer + counts[bound] < want) {
      nearer += counts[bound];
      bound++;
    }
    int ties = want - nearer;
    Arrays.fill(counts, nearest, farthest + 1, 0);
    int[] cells = scratch.shortlistCells;
    float[] dists = scratch.shortlistDist;
    int n = 0;
    for (int c = 0; c < nlist && n < want; c++) {
      int distance = distances[c];
      if (distance < bound || (distance == bound && ties-- > 0)) {
        cells[n] = c;
        dists[n] = exactDistance(vector, c);
        n++;
      }
    }
    CentroidGraph.sortByDistance(dists, cells, n);
    out.count = Math.min(keep, n);
    System.arraycopy(cells, 0, out.cells, 0, out.count);
    out.nearestDist = n > 0 ? dists[0] : Float.MAX_VALUE;
    out.secondDist = n > 1 ? dists[1] : Float.MAX_VALUE;
  }

  /** The negated dot product with centroid {@code cell}, so smaller is nearer. */
  float exactDistance(float[] vector, int cell) {
    return -VectorUtil.dotProduct(vector, centroids[cell]);
  }

  /**
   * Whether {@code secondDist} is within {@code ratio} of {@code nearestDist}: a ratio of 1.05
   * accepts a gap of up to 5% of the nearest distance. An unknown second cell is never within.
   */
  static boolean withinMargin(float nearestDist, float secondDist, float ratio) {
    return secondDist != Float.MAX_VALUE
        && secondDist - nearestDist <= (ratio - 1f) * Math.abs(nearestDist);
  }
}
