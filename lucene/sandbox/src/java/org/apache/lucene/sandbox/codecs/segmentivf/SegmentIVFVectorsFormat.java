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
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.knn.KnnSearchStrategy;

/**
 * Two-tier inverted-file (IVF) vector format: a Nitrox2 Hamming scan over the probed cells followed
 * by an INT8 or FP32 rerank of a bounded shortlist.
 *
 * <p>{@code nlist} is the target number of cells per segment (at most one per vector), {@code
 * nprobe} is the default number of cells a query visits, {@code spillBits} is the maximum number of
 * boundary cells each vector is also written to, and {@link FineTier} selects the rerank encoding.
 * Use {@link SearchStrategy} to override the probe count per query.
 *
 * @lucene.experimental
 */
public final class SegmentIVFVectorsFormat extends KnnVectorsFormat {
  static final String NAME = "SegmentIVFVectorsFormat";
  static final String META_CODEC_NAME = NAME + "Meta", DATA_CODEC_NAME = NAME + "Data";
  static final String META_EXTENSION = "ivfm", DATA_EXTENSION = "ivfd";
  static final int VERSION_CURRENT = 0, DIRECT_MONOTONIC_BLOCK_SHIFT = 16;

  /** Maximum configurable number of cells per field. */
  public static final int MAX_NLIST = 0xFFFF;

  static final float DEFAULT_PROBE_MARGIN = 0.75f;

  final int nlist, nprobe, spillBits;
  final FineTier fineTier;

  /** Encoding of the fine rerank tier. */
  public enum FineTier {
    INT8,
    FP32
  }

  /** Creates a format with 1000 cells, 32 probes, one spill cell, and INT8 reranking. */
  public SegmentIVFVectorsFormat() {
    this(1000, 32);
  }

  /** Creates a format with custom cell and probe counts, one spill cell, and INT8 reranking. */
  public SegmentIVFVectorsFormat(int nlist, int nprobe) {
    this(nlist, nprobe, 1, FineTier.INT8);
  }

  /** Creates a fully configured format. */
  public SegmentIVFVectorsFormat(int nlist, int nprobe, int spillBits, FineTier fineTier) {
    super(NAME);
    if (nlist < 1 || nlist > MAX_NLIST || nprobe < 1 || spillBits < 0 || fineTier == null) {
      throw new IllegalArgumentException(
          "require 1 <= nlist <= "
              + MAX_NLIST
              + ", nprobe >= 1, spillBits >= 0 and non-null fineTier; got nlist="
              + nlist
              + " nprobe="
              + nprobe
              + " spillBits="
              + spillBits
              + " fineTier="
              + fineTier);
    }
    this.nlist = nlist;
    this.nprobe = nprobe;
    this.spillBits = spillBits;
    this.fineTier = fineTier;
  }

  /**
   * Per-query probe count and probe margin.
   *
   * <p>The query visits at most {@code numProbes} cells, stopping early at the first cell whose
   * centroid distance exceeds {@code probeMargin} times the nearest cell's; a margin of 1 disables
   * the cutoff.
   */
  public static final class SearchStrategy extends KnnSearchStrategy {
    final int numProbes;
    final float probeMargin;

    /** Creates a search strategy with the default probe margin. */
    public SearchStrategy(int numProbes) {
      this(numProbes, DEFAULT_PROBE_MARGIN);
    }

    /** Creates a search strategy with explicit probes and margin. */
    public SearchStrategy(int numProbes, float probeMargin) {
      // Negated so NaN is rejected.
      if (numProbes < 1 || (probeMargin > 0 && probeMargin <= 1) == false) {
        throw new IllegalArgumentException(
            "require numProbes >= 1 and 0 < probeMargin <= 1; got numProbes="
                + numProbes
                + " probeMargin="
                + probeMargin);
      }
      this.numProbes = numProbes;
      this.probeMargin = probeMargin;
    }

    /** Advances no state because cells are probed in a single pass. */
    @Override
    public void nextVectorsBlock() {}

    /** Compares probe count and margin. */
    @Override
    public boolean equals(Object other) {
      return other instanceof SearchStrategy strategy
          && strategy.numProbes == numProbes
          && Float.compare(strategy.probeMargin, probeMargin) == 0;
    }

    /** Hashes probe count and margin. */
    @Override
    public int hashCode() {
      return 31 * numProbes + Float.hashCode(probeMargin);
    }
  }

  /** Creates the segment vector writer. */
  @Override
  public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
    return new SegmentIVFVectorsWriter(state, this);
  }

  /** Opens the segment vector reader. */
  @Override
  public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
    return new SegmentIVFVectorsReader(state);
  }

  /** Returns Lucene's default maximum vector dimension. */
  @Override
  public int getMaxDimensions(String fieldName) {
    return DEFAULT_MAX_DIMENSIONS;
  }

  /** Formats the SegmentIVF configuration. */
  @Override
  public String toString() {
    String head = NAME + "(nlist=" + nlist + " nprobe=" + nprobe + " spillBits=" + spillBits;
    return head + " fineTier=" + fineTier + ")";
  }
}
