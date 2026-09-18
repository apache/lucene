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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;

/**
 * Experimental IVF vector format: approximate Lloyd clustering with a movement-bound reaper,
 * hot-start seeding from the largest compatible segment in the latest commit, merges that carry the
 * largest input's surviving primary assignments, boundary spill, and a single-layer NSW graph over
 * Nitrox2 centroid codes. Spill copies never train the means.
 *
 * <p>The coarse tier is always Nitrox2 (Hadamard rotation and two thermometer bit planes); the fine
 * tier is {@link Tier#U8} or {@link Tier#FP32}. Both are independent of centroid placement, so each
 * document is encoded once at ingestion and copied unchanged on compatible merges; re-clustering
 * never re-quantizes. Coarse and fine records occupy separate sections of the {@code .ivd} file
 * with matching cell-ordered slots, and the checksummed {@code .ive} file holds tier IDs, document
 * mappings, centroids, primary assignments, graph links and postings.
 *
 * <p>Search scores the coarse tier over {@code numProbes} cells, or a per-query {@link
 * SearchStrategy} override, then reranks the survivors with the fine tier; the collector's visited
 * count is the number of coarse records scored, spill duplicates included. Nitrox2 is an angular
 * sketch, so coarse ranking can lose recall, particularly for vectors with unequal norms. This
 * format has no compatibility guarantee with previous experimental versions.
 *
 * @lucene.experimental
 */
public final class IVFasterEvoVectorsFormat extends KnnVectorsFormat {
  static final String CODEC_NAME = "IVFasterEvo";
  static final String EXTENSION = "ive";
  static final int VERSION = 3;
  private final CommittedCentroids committedCentroids = new CommittedCentroids();
  final int numCentroids;
  final int numProbes;
  final int spillBits;
  final double spillMargin;
  final Tier fineTier;

  /**
   * Centroid-independent vector encodings. Every vector has a Nitrox2 coarse code; the fine tier is
   * configurable. Ordinals are persisted.
   */
  public enum Tier {
    /** Two thermometer bit planes per dimension after Hadamard rotation. Coarse-only. */
    NITROX2,
    /** Lucene per-vector optimized unsigned 8-bit quantization. */
    U8,
    /** Original float32 components. */
    FP32
  }

  /** Uses up to 64 centroids and probes up to 8 cells per search, with no spill and FP32 fine. */
  public IVFasterEvoVectorsFormat() {
    this(64, 8);
  }

  /**
   * Creates a format with the given maximum centroid count and number of cells to probe, with no
   * spill and FP32 fine records.
   */
  public IVFasterEvoVectorsFormat(int numCentroids, int numProbes) {
    this(numCentroids, numProbes, 0, 1.4, Tier.FP32);
  }

  /**
   * Creates a fully configured format.
   *
   * @param numCentroids maximum centroid count. Cold starts cap it at the number of vectors; hot
   *     starts retain inherited empty cells.
   * @param numProbes default number of cells to probe, stored in the segment; {@code 1 <= numProbes
   *     <= numCentroids}. See {@link SearchStrategy} for a per-query override.
   * @param spillBits maximum extra cells per boundary vector, chosen after convergence by SOAR loss
   *     (lambda 1), which prefers cells complementary to the primary residual. Spill copies never
   *     train the means.
   * @param spillMargin a vector is on a boundary, and spills, when its runner-up cell lies within
   *     this ratio of its primary distance, {@code >= 1}.
   * @param fineTier {@link Tier#U8} or {@link Tier#FP32}. U8 keeps no full-precision copy.
   */
  public IVFasterEvoVectorsFormat(
      int numCentroids, int numProbes, int spillBits, double spillMargin, Tier fineTier) {
    super(CODEC_NAME);
    if (fineTier != Tier.U8 && fineTier != Tier.FP32) {
      throw new IllegalArgumentException("fineTier must be U8 or FP32");
    }
    this.fineTier = fineTier;
    if (Double.isFinite(spillMargin) == false || spillMargin < 1) {
      throw new IllegalArgumentException("spillMargin must be finite and >= 1");
    }
    this.spillMargin = spillMargin;
    if (spillBits < 0) throw new IllegalArgumentException("spillBits must be non-negative");
    this.spillBits = spillBits;
    if (numCentroids < 1 || numProbes < 1 || numProbes > numCentroids) {
      throw new IllegalArgumentException("require 1 <= numProbes <= numCentroids");
    }
    this.numCentroids = numCentroids;
    this.numProbes = numProbes;
  }

  /** Per-query probe count, overriding the segment default without rebuilding the index. */
  public static final class SearchStrategy extends org.apache.lucene.search.knn.KnnSearchStrategy {
    final int numProbes;

    /** Creates a strategy probing at most this many cells. */
    public SearchStrategy(int numProbes) {
      if (numProbes < 1) throw new IllegalArgumentException("numProbes must be positive");
      this.numProbes = numProbes;
    }

    @Override
    public void nextVectorsBlock() {}

    @Override
    public boolean equals(Object other) {
      return other instanceof SearchStrategy strategy && strategy.numProbes == numProbes;
    }

    @Override
    public int hashCode() {
      return Integer.hashCode(numProbes);
    }
  }

  @Override
  public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
    return new IVFasterEvoVectorsWriter(
        state, this, committedCentroids.snapshot(state.segmentInfo.dir));
  }

  @Override
  public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
    return new IVFasterEvoVectorsReader(state);
  }

  @Override
  public int getMaxDimensions(String fieldName) {
    return DEFAULT_MAX_DIMENSIONS;
  }

  /** Index-local, immutable centroid snapshots from published commits, never NRT readers. */
  static final class CommittedCentroids {
    private record Key(String field, int dimension, VectorSimilarityFunction similarity) {
      Key(FieldInfo info) {
        this(info.name, info.getVectorDimension(), info.getVectorSimilarityFunction());
      }
    }

    record Seed(float[][] centroids, int liveVectors) {}

    record Snapshot(long generation, Map<Key, List<Seed>> fields) {
      Seed seed(FieldInfo info, int maxCentroids) {
        for (Seed seed : fields.getOrDefault(new Key(info), List.of())) {
          if (seed.centroids.length <= maxCentroids) return seed;
        }
        return null;
      }
    }

    private final Map<Directory, Snapshot> snapshots = new WeakHashMap<>();

    synchronized Snapshot snapshot(Directory directory) throws IOException {
      long generation = SegmentInfos.getLastCommitGeneration(directory);
      Snapshot cached = snapshots.get(directory);
      if (cached != null && cached.generation == generation) return cached;
      Map<Key, List<Seed>> fields = new HashMap<>();
      if (generation >= 0) {
        // DirectoryReader handles a concurrent commit/deletion-policy race when opening the
        // latest commit. Copy only centroids and counts; no readers or file handles are retained.
        try (DirectoryReader reader = DirectoryReader.open(directory)) {
          generation = reader.getIndexCommit().getGeneration();
          for (var leaf : reader.leaves()) {
            CodecReader segment = (CodecReader) leaf.reader();
            for (FieldInfo info : segment.getFieldInfos()) {
              if (info.hasVectorValues() == false) continue;
              if (segment.getVectorReader().unwrapReaderForField(info.name)
                  instanceof IVFasterEvoVectorsReader evo) {
                var values = evo.getFloatVectorValues(info.name);
                int live = values.size();
                if (segment.getLiveDocs() != null) {
                  live = 0;
                  for (int ord = 0; ord < values.size(); ord++) {
                    if (segment.getLiveDocs().get(values.ordToDoc(ord))) live++;
                  }
                }
                if (live > 0) {
                  float[][] centroids = evo.centroids(info.name);
                  float[][] copy = new float[centroids.length][];
                  for (int c = 0; c < copy.length; c++) copy[c] = centroids[c].clone();
                  fields
                      .computeIfAbsent(new Key(info), _ -> new ArrayList<>())
                      .add(new Seed(copy, live));
                }
              }
            }
          }
        }
      }
      fields.replaceAll(
          (_, seeds) ->
              seeds.stream()
                  .sorted(Comparator.comparingInt(Seed::liveVectors).reversed())
                  .toList());
      Snapshot snapshot = new Snapshot(generation, Map.copyOf(fields));
      snapshots.put(directory, snapshot);
      return snapshot;
    }
  }
}
