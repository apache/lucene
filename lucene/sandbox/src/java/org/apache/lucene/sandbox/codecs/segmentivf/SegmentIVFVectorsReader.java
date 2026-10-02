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

import static org.apache.lucene.codecs.CodecUtil.checkIndexHeader;
import static org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.DATA_CODEC_NAME;
import static org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.DATA_EXTENSION;
import static org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.DEFAULT_PROBE_MARGIN;
import static org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.DIRECT_MONOTONIC_BLOCK_SHIFT;
import static org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.META_CODEC_NAME;
import static org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.META_EXTENSION;
import static org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.VERSION_CURRENT;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.apache.lucene.util.packed.DirectMonotonicReader.loadMeta;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.AddressLayout;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Float16VectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.sandbox.codecs.segmentivf.Centroids.CentroidCodes;
import org.apache.lucene.sandbox.codecs.segmentivf.Centroids.CentroidGraph;
import org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.FineTier;
import org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.SearchStrategy;
import org.apache.lucene.sandbox.codecs.segmentivf.Tiers.CodeRecord;
import org.apache.lucene.sandbox.codecs.segmentivf.Tiers.FineCodec;
import org.apache.lucene.sandbox.codecs.segmentivf.Tiers.HadamardRotation;
import org.apache.lucene.sandbox.codecs.segmentivf.Tiers.Nitrox2;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.packed.DirectMonotonicReader;

/**
 * Searches SegmentIVF fields by probing cells, scanning coarse codes, and reranking a shortlist.
 *
 * <p>The coarse scan is intentionally bandwidth-oriented: compact Nitrox2 rows are processed with
 * XOR and popcount, then only a bounded set of survivors reaches the more expensive fine scorer.
 * Filtered search chooses between scanning selected cells and visiting accepted documents directly.
 */
final class SegmentIVFVectorsReader extends KnnVectorsReader {
  /** Coarse candidates fine-reranked per requested neighbor, and never fewer than MIN_RERANK. */
  static final int RERANK_PER_K = 7, MIN_RERANK = 100;

  /** Returns how many coarse candidates to fine-rerank for a top-{@code k} search. */
  static long rerankCount(int k) {
    return Math.max(MIN_RERANK, (long) RERANK_PER_K * k);
  }

  private static final int ADMIT_BLOCK = 256, FILTERED_PROBE_MULTIPLIER = 8;
  private static final int VERIFY_MIN = 64, VERIFY_MULTIPLIER = 2;
  private static final Kernels K = Kernels.INSTANCE;
  private static final ValueLayout.OfInt INT_LE =
      ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

  /**
   * A segment's deduplicated coarse shortlist: record slots and their coarse distances, plus the
   * search that gathered them, whose encoded query the rerank reuses.
   */
  record Candidates(int[] slots, int[] distances, Field.Search search) {
    static final Candidates EMPTY = new Candidates(new int[0], new int[0], null);
  }

  private final Map<String, Field> fields = new HashMap<>();
  private final IndexInput data;
  private final Uring uring;
  private final Arena pinned = Arena.ofShared();
  private final AtomicLong pinnedBytes = new AtomicLong();
  private long fineBytes;
  private boolean closed;
  private final String segment;
  private final int segmentMaxDoc;

  SegmentIVFVectorsReader(SegmentReadState state) throws IOException {
    String name = segment = state.segmentInfo.name, sfx = state.segmentSuffix;
    segmentMaxDoc = state.segmentInfo.maxDoc();
    byte[] id = state.segmentInfo.getId();
    String metaName = IndexFileNames.segmentFileName(name, sfx, META_EXTENSION);
    try (ChecksumIndexInput meta = state.directory.openChecksumInput(metaName)) {
      Throwable prior = null;
      try {
        checkIndexHeader(meta, META_CODEC_NAME, VERSION_CURRENT, VERSION_CURRENT, id, sfx);
        for (int number = meta.readInt(); number != -1; number = meta.readInt()) {
          FieldInfo info = state.fieldInfos.fieldInfo(number);
          if (info == null) throw new CorruptIndexException("invalid field number " + number, meta);
          fields.put(info.name, new Field(meta, info));
        }
      } catch (Throwable t) {
        prior = t;
      } finally {
        CodecUtil.checkFooter(meta, prior);
      }
    }
    String dataName = IndexFileNames.segmentFileName(name, sfx, DATA_EXTENSION);
    data = state.directory.openInput(dataName, state.context);
    try {
      checkIndexHeader(data, DATA_CODEC_NAME, VERSION_CURRENT, VERSION_CURRENT, id, sfx);
      CodecUtil.retrieveChecksum(data);
    } catch (Throwable t) {
      IOUtils.closeWhileSuppressingExceptions(t, data);
      throw t;
    }
    uring = openUring(state, dataName);
    for (Field field : fields.values()) fineBytes += field.sections[2] - field.sections[1];
    Uring.addFine(fineBytes);
    if (state.context.context() != IOContext.Context.MERGE) {
      for (Field field : fields.values()) field.maybePin();
    }
  }

  /** Opens batched fine-record reads for a data file of a file-system directory, if possible. */
  private static Uring openUring(SegmentReadState state, String name) {
    if (FilterDirectory.unwrap(state.directory) instanceof FSDirectory fs) {
      try {
        return Uring.open(fs.getDirectory().resolve(name));
      } catch (IOException _) {
        // fine records are read through the mapped input instead
      }
    }
    return null;
  }

  final class Field {
    final VectorSimilarityFunction similarity;
    final FineTier fineTier;
    final int dim, nlist, count, nprobe, spillBits, recordLen, coarseBytes, docIdOffset;
    final long rotationSeed;
    final long[] sections;
    final DirectMonotonicReader.Meta postingOffsets;
    final HadamardRotation rotation;
    final FineCodec fine;

    RandomAccessInput records, slotDocs;
    MemorySegment recordsSeg; // the whole mapped records section, or null
    MemorySegmentAccessInput coarseAccess;
    MemorySegment coarseSeg;
    // Off-heap copies published by the pinning thread; searches use the mapped sections until then.
    volatile MemorySegment pinnedCoarse, pinnedSlotDocs;
    private final AtomicBoolean pinning = new AtomicBoolean();
    private volatile long lastPinAttempt;
    int[] cellStart, ordToSlot, ordToDoc, allCells, primaryCells;
    float[][] centroids;
    private volatile CentroidCodes codes;
    CentroidGraph graph;

    Field(ChecksumIndexInput meta, FieldInfo info) throws IOException {
      similarity = info.getVectorSimilarityFunction();
      fineTier = FineTier.values()[meta.readByte()];
      dim = meta.readVInt();
      nlist = meta.readVInt();
      count = meta.readVInt();
      rotationSeed = meta.readLong();
      nprobe = meta.readVInt();
      spillBits = meta.readVInt();
      // Sections: centroids, records, coarse, graph, ordToSlot, slotDoc, posting offsets.
      sections = new long[7];
      for (int s = 0; s < sections.length; s++) sections[s] = meta.readVLong();
      postingOffsets = nlist == 0 ? null : loadMeta(meta, nlist + 1, DIRECT_MONOTONIC_BLOCK_SHIFT);
      if (dim != info.getVectorDimension()) throw new CorruptIndexException("dimension", meta);
      rotation = HadamardRotation.create(dim, rotationSeed);
      fine = new FineCodec(fineTier, dim);
      recordLen = CodeRecord.length(fine.codeBytes);
      coarseBytes = Nitrox2.bytesPerVector(dim);
      docIdOffset = fine.codeBytes;
    }

    private RandomAccessInput section(int s) throws IOException {
      return data.randomAccessSlice(sections[s], sections[s + 1] - sections[s]);
    }

    synchronized Field open() throws IOException {
      if (records != null) return this;
      if (section(2) instanceof MemorySegmentAccessInput in) {
        coarseAccess = in;
        coarseSeg = segmentOrNull(in, 0, in.length());
      }
      cellStart = new int[nlist + 1];
      if (nlist > 0) {
        long postings = sections[sections.length - 1];
        RandomAccessInput tail = data.randomAccessSlice(postings, data.length() - postings);
        var offsets = DirectMonotonicReader.getInstance(postingOffsets, tail);
        for (int c = 0; c <= nlist; c++) cellStart[c] = (int) (offsets.get(c) / Integer.BYTES);
      }
      slotDocs = section(5);
      IndexInput all = data.clone();
      centroids = new float[nlist][dim];
      all.seek(sections[0]);
      for (float[] centroid : centroids) all.readFloats(centroid, 0, dim);
      records = section(1);
      if (records instanceof MemorySegmentAccessInput in) {
        recordsSeg = segmentOrNull(in, 0, in.length());
      }
      return this;
    }

    /**
     * Queues the off-heap copy of the coarse and slot-to-document sections when the pin budget has
     * room, retrying at most once per second: a merged segment is pinned once its sources close.
     * Searches never wait for the copy.
     */
    void maybePin() {
      if (count == 0 || pinnedCoarse != null || pinning.get()) return;
      long now = System.nanoTime();
      if (lastPinAttempt != 0 && now - lastPinAttempt < 1_000_000_000L) return;
      if (pinning.getAndSet(true)) return;
      lastPinAttempt = now == 0 ? 1 : now;
      long coarseLength = sections[3] - sections[2];
      long docsLength = sections[6] - sections[5];
      if (Uring.reservePinned(coarseLength + docsLength) == false) {
        pinning.set(false);
        return;
      }
      pinnedBytes.addAndGet(coarseLength + docsLength);
      Uring.PINNER.execute(
          () -> {
            try {
              if (docsLength > 0) pinnedSlotDocs = pin(sections[5], docsLength);
              pinnedCoarse = pin(sections[2], coarseLength);
            } catch (IOException | RuntimeException _) {
              // the reader closed mid-copy; its reservation is released on close
            }
          });
    }

    /** Copies a section off-heap, where the page cache cannot evict it. */
    private MemorySegment pin(long offset, long length) throws IOException {
      MemorySegment copy = pinned.allocate(length, 64); // cache-line aligned for the SIMD scans
      IndexInput in = data.clone();
      in.seek(offset);
      byte[] chunk = new byte[1 << 20];
      for (long at = 0; at < length; at += chunk.length) {
        int n = (int) Math.min(chunk.length, length - at);
        in.readBytes(chunk, 0, n);
        MemorySegment.copy(chunk, 0, copy, ValueLayout.JAVA_BYTE, at, n);
      }
      return copy;
    }

    private int docAt(RandomAccessInput docs, int slot) throws IOException {
      MemorySegment pinned = pinnedSlotDocs;
      return pinned != null ? pinned.getAtIndex(INT_LE, slot) : docs.readInt((long) slot * 4);
    }

    private synchronized void loadOrdToSlot() throws IOException {
      if (ordToSlot != null) return;
      int[] slots = new int[count];
      IndexInput all = data.clone();
      all.seek(sections[4]);
      all.readInts(slots, 0, count);
      ordToSlot = slots;
    }

    private synchronized void loadOrdinalMappings() throws IOException {
      if (ordToDoc != null) return;
      loadOrdToSlot();
      int[] docs = new int[count];
      for (int ord = 0; ord < count; ord++) docs[ord] = docAt(slotDocs, ordToSlot[ord]);
      ordToDoc = docs;
    }

    /**
     * Returns a mapped slice, or null when no single mapping covers it. The slice is rebased as a
     * plain native segment scoped to this reader, so the SIMD kernels see one segment type whether
     * or not a section is pinned: once they have seen both, every coarse scan runs ~16% slower.
     */
    @SuppressWarnings("restricted")
    private MemorySegment segmentOrNull(MemorySegmentAccessInput in, long offset, long length) {
      if (length == 0) return null;
      try {
        MemorySegment mapped = in.segmentSliceOrNull(offset, length);
        if (mapped == null) return null;
        return MemorySegment.ofAddress(mapped.address()).reinterpret(length, pinned, null);
      } catch (IOException _) {
        return null;
      }
    }

    /**
     * Returns the mapping holding one cell run: the whole coarse section when mappable, otherwise a
     * slice rebased to the run, or null when neither is mapped. Offsets start at {@code runBase}.
     */
    private MemorySegment coarseRun(int slotBase, int rows) {
      if (coarseSeg != null || coarseAccess == null) return coarseSeg;
      return segmentOrNull(coarseAccess, (long) slotBase * coarseBytes, (long) rows * coarseBytes);
    }

    private long runBase(int slotBase) {
      return coarseSeg == null ? 0 : (long) slotBase * coarseBytes;
    }

    private synchronized void loadCodes() throws IOException {
      if (codes != null) return;
      allCells = new int[nlist];
      for (int c = 0; c < nlist; c++) allCells[c] = c;
      long graphLength = sections[4] - sections[3];
      graph = graphLength == 0 ? null : CentroidGraph.read(section(3), dim, graphLength);
      codes = new CentroidCodes(centroids, dim, fine);
    }

    synchronized int cellOf(int ord) throws IOException {
      loadOrdToSlot();
      if (primaryCells == null) {
        primaryCells = new int[count];
        for (int o = 0; o < count; o++) {
          primaryCells[o] = records.readInt((long) ordToSlot[o] * recordLen + docIdOffset + 4);
        }
      }
      return primaryCells[ord];
    }

    /**
     * Per-query state for cell selection, coarse admission, deduplication, and fine reranking. Each
     * search opens its own slices, which shadow the field's: positional reads through a shared
     * slice are not thread-safe on every directory implementation.
     */
    final class Search {
      final RandomAccessInput records = section(1), coarse = section(2);
      final RandomAccessInput slotDocs = section(5);
      // One snapshot per query, so a copy published mid-query cannot mix offsets.
      final MemorySegment pinnedCoarse = Field.this.pinnedCoarse;
      final float[] rotated;
      final byte[] qCode;
      final FineCodec.Query fine;
      final KnnCollector collector;
      final Scratch scratch = Scratch.LOCAL.get();
      final int bins = coarseBytes * 8 + 2, shortlist, pool;
      final int[] histogram = scratch.histogram = ArrayUtil.growNoCopy(scratch.histogram, bins);
      int size, admitted, threshold = bins - 1;
      Candidates gathered;

      Search(float[] target, KnnCollector collector, int k) throws IOException {
        maybePin();
        this.collector = collector;
        shortlist = (int) Math.min(count, rerankCount(k));
        pool = Math.multiplyExact(shortlist, 1 + spillBits);
        scratch.reserve(shortlist);
        Arrays.fill(histogram, 0, bins, 0);
        if (codes == null) loadCodes();
        rotated = new float[dim];
        qCode = new byte[coarseBytes];
        rotation.rotate(VectorUtil.l2normalize(ArrayUtil.copyOfSubArray(target, 0, dim)), rotated);
        Nitrox2.encode(rotated, dim, qCode, 0);
        fine = Field.this.fine.query(rotated, similarity);
      }

      /** A rerank of {@code from}'s shortlist on this thread, reusing its encoded query. */
      private Search(Search from, KnnCollector collector) throws IOException {
        this.collector = collector;
        shortlist = from.shortlist;
        pool = from.pool;
        rotated = from.rotated;
        qCode = from.qCode;
        fine = from.fine;
      }

      /** Fine-reranks {@code slots}, taken from this search's shortlist, into {@code collector}. */
      void rerankInto(int[] slots, KnnCollector collector) throws IOException {
        new Search(this, collector).rerank(slots, slots.length);
      }

      void run(AcceptDocs acceptDocs) throws IOException {
        var strategy = collector.getSearchStrategy();
        if (strategy instanceof SearchStrategy s) run(s.numProbes, s.probeMargin, acceptDocs);
        else run(nprobe, DEFAULT_PROBE_MARGIN, acceptDocs);
      }

      private void run(int probe, float margin, AcceptDocs acceptDocs) throws IOException {
        probe = Math.min(probe, nlist);
        Bits accept = acceptDocs == null ? null : acceptDocs.bits();
        if (accept instanceof BitSet filter) {
          int cost = acceptDocs.cost();
          double parity =
              Math.sqrt((double) shortlist * cellStart[nlist] * coarseBytes / recordLen);
          if (cost > (int) Math.max(shortlist, Math.min(Integer.MAX_VALUE, parity))) {
            filteredScan(selectCells(probe, 1f), filter, cost, probe);
            return;
          }
          boolean dense = count == segmentMaxDoc;
          if (dense) loadOrdToSlot();
          else loadOrdinalMappings();
          int[] slots = new int[64];
          int n = 0;
          DocIdSetIterator accepted = acceptDocs.iterator();
          for (int doc = accepted.nextDoc(); doc != NO_MORE_DOCS; doc = accepted.nextDoc()) {
            int ord = dense ? doc : Arrays.binarySearch(ordToDoc, doc);
            if (ord < 0) continue;
            slots = ArrayUtil.grow(slots, n + 1);
            slots[n++] = ordToSlot[ord];
          }
          if (collector == null) admitAll(slots, n);
          else rerank(slots, n);
        } else {
          // A segment no larger than the rerank pool scans every slot: its mostly empty cells would
          // spend probes on cells holding nothing.
          scan(cellStart[nlist] <= pool ? allCells : selectCells(probe, margin), accept);
        }
      }

      private int[] selectCells(int probe, float margin) {
        int[] candidates = allCells;
        int got = nlist;
        if (graph != null) {
          candidates = scratch.candidates = ArrayUtil.growNoCopy(scratch.candidates, nlist);
          int[] coarse = scratch.coarse = ArrayUtil.growNoCopy(scratch.coarse, nlist);
          int ef = Math.max(CentroidGraph.MIN_EF, probe * CentroidGraph.EF_MULTIPLIER);
          got = graph.search(qCode, ef, candidates, coarse);
          int cap = Math.max(VERIFY_MIN, probe * VERIFY_MULTIPLIER);
          if (cap < got) {
            int[] counts = new int[bins];
            for (int i = 0; i < got; i++) counts[coarse[i]]++;
            int below = 0, bound = 0;
            while (below + counts[bound] <= cap) below += counts[bound++];
            int n = 0, ties = cap - below;
            for (int i = 0; i < got && n < cap; i++) {
              if (coarse[i] < bound || (coarse[i] == bound && ties-- > 0)) {
                candidates[n] = candidates[i];
                coarse[n++] = coarse[i];
              }
            }
            got = n;
          }
        }
        long[] ranked = rank(candidates, got);
        int keep = Math.min(probe, got);
        if (margin != 1f && keep > 1) {
          float bound = scratch.exact[(int) ranked[0]] * margin;
          int k = 1;
          while (k < keep && scratch.exact[(int) ranked[k]] <= bound) k++;
          keep = k;
        }
        int[] cells = new int[keep];
        for (int i = 0; i < keep; i++) cells[i] = candidates[(int) ranked[i]];
        return cells;
      }

      private long[] rank(int[] cells, int n) {
        float[] distances = scratch.exact = ArrayUtil.grow(scratch.exact, n);
        codes.rankCandidates(rotated, cells, n, distances);
        long[] ranked = new long[n];
        for (int i = 0; i < n; i++) {
          ranked[i] = ((long) NumericUtils.floatToSortableInt(distances[i] + 0f) << 32) | i;
        }
        Arrays.sort(ranked);
        return ranked;
      }

      private int prefetch(int[] cells, int n) throws IOException {
        int total = 0;
        for (int i = 0; i < n; i++) {
          int start = cellStart[cells[i]], rows = cellStart[cells[i] + 1] - start;
          if (rows == 0) continue;
          total += rows;
          if (pinnedCoarse == null) {
            coarse.prefetch((long) start * coarseBytes, (long) rows * coarseBytes);
          }
        }
        return total;
      }

      private void tighten() {
        while (threshold > 0 && admitted - histogram[threshold] >= pool) {
          admitted -= histogram[threshold--];
        }
      }

      private void scan(int[] cells, Bits liveDocs) throws IOException {
        int total = prefetch(cells, cells.length);
        long[] packed = scratch.packed = ArrayUtil.growNoCopy(scratch.packed, total);
        for (int cell : cells) {
          int base = cellStart[cell], rows = cellStart[cell + 1] - base;
          int[] distances = scratch.distances = ArrayUtil.growNoCopy(scratch.distances, rows);
          distances(base, rows, distances);
          for (int from = 0; from < rows; from += ADMIT_BLOCK) {
            int block = Math.min(ADMIT_BLOCK, rows - from);
            int n = K.filterAtMost(distances, from, block, threshold, scratch.kept);
            for (int i = 0; i < n; i++) {
              int row = from + scratch.kept[i];
              histogram[distances[row]]++;
              packed[size++] = ((long) distances[row] << 32) | (base + row);
            }
            admitted += n;
            tighten();
          }
        }
        rerankPool(Math.min(pool, total), liveDocs);
      }

      /**
       * Admits every given slot by its coarse distance, so directly visited filter matches join a
       * cross-segment shortlist on the same terms as scanned candidates.
       */
      private void admitAll(int[] slots, int n) throws IOException {
        long[] packed = scratch.packed = ArrayUtil.growNoCopy(scratch.packed, n);
        int[] distance = new int[1];
        for (int i = 0; i < n; i++) {
          distances(slots[i], 1, distance);
          histogram[distance[0]]++;
          packed[size++] = ((long) distance[0] << 32) | slots[i];
        }
        rerankPool(Math.min(pool, n), null);
      }

      private MemorySegment coarseRun(int slotBase, int rows) {
        return pinnedCoarse != null ? pinnedCoarse : Field.this.coarseRun(slotBase, rows);
      }

      private long runBase(int slotBase) {
        return pinnedCoarse != null ? (long) slotBase * coarseBytes : Field.this.runBase(slotBase);
      }

      private void distances(int base, int rows, int[] out) throws IOException {
        MemorySegment run = coarseRun(base, rows);
        if (run != null) {
          K.hamming(qCode, run, runBase(base), rows, out);
          return;
        }
        byte[] code = scratch.bytes = ArrayUtil.growNoCopy(scratch.bytes, coarseBytes);
        for (int row = 0; row < rows; row++) {
          coarse.readBytes((long) (base + row) * coarseBytes, code, 0, coarseBytes);
          out[row] = K.hamming(qCode, code, 0);
        }
      }

      private void rerankPool(int need, Bits live) throws IOException {
        int[] next = scratch.prefix = ArrayUtil.growNoCopy(scratch.prefix, bins + 1);
        int cut = 0, n = 0;
        for (next[0] = 0; cut < bins && next[cut] + histogram[cut] < need; cut++) {
          next[cut + 1] = next[cut] + histogram[cut];
        }
        int ties = need - next[cut];
        long[] ordered = scratch.ordered = ArrayUtil.growNoCopy(scratch.ordered, need);
        for (int i = 0; i < size; i++) {
          int d = (int) (scratch.packed[i] >>> 32);
          if (d < cut || (d == cut && ties-- > 0)) ordered[next[d]++] = scratch.packed[i];
        }
        scratch.newDedup();
        int[] kept = scratch.shortlist, distances = scratch.shortlistDistances;
        for (int i = 0; i < next[cut] && n < shortlist; i++) {
          int slot = (int) ordered[i], doc = docAt(slotDocs, slot);
          if ((live == null || live.get(doc)) && scratch.addDistinct(doc)) {
            distances[n] = (int) (ordered[i] >>> 32);
            kept[n++] = slot;
          }
        }
        if (collector != null) rerank(kept, n);
        else
          gathered =
              new Candidates(
                  ArrayUtil.copyOfSubArray(kept, 0, n),
                  ArrayUtil.copyOfSubArray(distances, 0, n),
                  this);
      }

      private void rerank(int[] slots, int n) throws IOException {
        if (n == 0) return;
        long[] offsets = scratch.offsets = ArrayUtil.growNoCopy(scratch.offsets, n);
        float[] scores = scratch.scores = ArrayUtil.grow(scratch.scores, n);
        MemorySegment source = readRecords(slots, n, offsets);
        fine.score(source, offsets, n, scores);
        for (int i = 0; i < n; i++) {
          collector.collect(source.get(INT_LE, offsets[i] + docIdOffset), scores[i]);
        }
        collector.incVisitedCount(n);
      }

      /**
       * Returns memory holding the fine records of {@code slots}, scored in place, with each
       * record's start in {@code offsets}: one io_uring batch when memory is short, else the mapped
       * section, copying only on directories that cannot map it.
       */
      private MemorySegment readRecords(int[] slots, int n, long[] offsets) throws IOException {
        int len = recordLen;
        if (uring != null && Uring.useUring()) {
          long[] positions = new long[n];
          for (int i = 0; i < n; i++) positions[i] = sections[1] + (long) slots[i] * len;
          try {
            MemorySegment buffer = uring.readBatch(positions, len);
            for (int i = 0; i < n; i++) offsets[i] = (long) i * len;
            return buffer;
          } catch (IOException _) {
            // fall back to the mapped input
          }
        }
        if (recordsSeg != null) {
          for (int i = 0; i < n; i++) offsets[i] = (long) slots[i] * len;
          return recordsSeg;
        }
        byte[] raw =
            scratch.bytes = ArrayUtil.growNoCopy(scratch.bytes, Math.multiplyExact(n, len));
        for (int i = 0; i < n; i++) {
          records.readBytes((long) slots[i] * len, raw, i * len, len);
          offsets[i] = (long) i * len;
        }
        return MemorySegment.ofArray(raw);
      }

      private void filteredScan(int[] seed, BitSet filter, int cost, int probe) throws IOException {
        int targetProbe = probe;
        if (cost > 0 && cost < count) {
          double exponent = 0.7185 - 0.0578 * Math.log(Math.max(2, count / nlist));
          exponent = Math.min(0.50, Math.max(0.15, exponent));
          double widened = Math.ceil(probe * Math.pow((double) count / cost, exponent));
          targetProbe = (int) Math.min(nlist, Math.max(widened, probe));
        }
        int first = Math.max(1, seed.length);
        int maxProbe = Math.max(targetProbe, Math.min(nlist, first * FILTERED_PROBE_MULTIPLIER));
        boolean[] probed = new boolean[nlist];
        long[] ranked = null;
        int served = 0, rankedAt = 0, distinct = 0;
        scratch.newDedup();
        byte[] code = new byte[coarseBytes];
        long survivors = 0;
        int[] batch = new int[maxProbe];
        for (int done = 0, want = first; done < maxProbe; want <<= 1) {
          int n = 0;
          for (want = Math.min(want, maxProbe - done); n < want; ) {
            if (served == seed.length && ranked == null) ranked = rank(allCells, nlist);
            if (rankedAt == nlist) break;
            int cell = ranked == null ? seed[served++] : (int) ranked[rankedAt++];
            if (probed[cell] == false) batch[n++] = cell;
            probed[cell] = true;
          }
          if (n == 0) break;
          done += n;
          scratch.packed = ArrayUtil.grow(scratch.packed, size + prefetch(batch, n));
          for (int c = 0; c < n; c++) {
            int base = cellStart[batch[c]], end = cellStart[batch[c] + 1];
            MemorySegment run = coarseRun(base, end - base);
            long runBase = runBase(base);
            for (int slot = base; slot < end; slot++) {
              int doc = docAt(slotDocs, slot);
              if (filter.get(doc) == false) continue;
              int d;
              if (run != null) {
                d = K.hamming(qCode, run, runBase + (long) (slot - base) * coarseBytes);
              } else {
                coarse.readBytes((long) slot * coarseBytes, code, 0, coarseBytes);
                d = K.hamming(qCode, code, 0);
              }
              survivors++;
              if (distinct < shortlist && scratch.addDistinct(doc)) distinct++;
              if (d > threshold) continue;
              histogram[d]++;
              scratch.packed[size++] = ((long) d << 32) | slot;
              if (++admitted > pool) tighten();
            }
          }
          if (done >= targetProbe && distinct >= shortlist) break;
        }
        rerankPool((int) Math.min(pool, survivors), null);
      }
    }

    final class Values extends FloatVectorValues {
      private final RandomAccessInput records = section(1), coarse = section(2);
      private final byte[] record = new byte[recordLen];
      private final float[] rotated = new float[dim], value = new float[dim];

      Values() throws IOException {}

      void copyRow(int ord, int docId, byte[] dest, int offset) throws IOException {
        loadOrdToSlot();
        records.readBytes((long) ordToSlot[ord] * recordLen, dest, offset, recordLen);
        BitUtil.VH_LE_INT.set(dest, offset + docIdOffset, docId);
        BitUtil.VH_LE_INT.set(dest, offset + CodeRecord.primaryCellOffset(docIdOffset), 0);
        coarse.readBytes(
            (long) ordToSlot[ord] * coarseBytes, dest, offset + recordLen, coarseBytes);
      }

      @Override
      public int dimension() {
        return dim;
      }

      @Override
      public int size() {
        return count;
      }

      @Override
      public float[] vectorValue(int ord) throws IOException {
        rotation.inverseRotate(rotatedValue(ord), value);
        return value;
      }

      private float[] rotatedValue(int ord) throws IOException {
        loadOrdToSlot();
        records.readBytes((long) ordToSlot[ord] * recordLen, record, 0, recordLen);
        fine.decode(record, 0, rotated);
        VectorUtil.l2normalize(rotated, false);
        return rotated;
      }

      @Override
      public int ordToDoc(int ord) {
        if (count == segmentMaxDoc) return ord;
        try {
          loadOrdinalMappings();
        } catch (IOException ioe) {
          throw new UncheckedIOException(ioe);
        }
        return ordToDoc[ord];
      }

      @Override
      public FloatVectorValues copy() throws IOException {
        return new Values();
      }

      @Override
      public DocIndexIterator iterator() {
        return count == segmentMaxDoc ? createDenseIterator() : createSparseIterator();
      }

      /**
       * Creates an exact scorer over decoded vectors. The rotation is orthogonal, so the query is
       * rotated once instead of inverse-rotating every scored vector.
       */
      @Override
      public VectorScorer scorer(float[] target) throws IOException {
        Values values = new Values();
        DocIndexIterator iterator = values.iterator();
        float[] query = new float[dim];
        rotation.rotate(target, query);
        return new VectorScorer() {
          @Override
          public float score() throws IOException {
            return similarity.compare(query, values.rotatedValue(iterator.index()));
          }

          @Override
          public DocIdSetIterator iterator() {
            return iterator;
          }
        };
      }
    }
  }

  private static final class Scratch {
    static final ThreadLocal<Scratch> LOCAL = ThreadLocal.withInitial(Scratch::new);

    int[] histogram = new int[0], prefix = new int[0], distances = new int[0], coarse = new int[0];
    int[] candidates = new int[0], kept = new int[ADMIT_BLOCK];
    int[] shortlist = new int[0], shortlistDistances = new int[0];
    int[] dedupKeys = new int[0], dedupStamps = new int[0];
    long[] packed = new long[0], ordered = new long[0], offsets = new long[0];
    byte[] bytes = new byte[0];
    float[] exact = new float[0], scores = new float[0];
    int dedupStamp, dedupMask;

    /** Sizes the shortlist and a dedup table at most a quarter full for {@code n} documents. */
    void reserve(int n) {
      shortlist = ArrayUtil.growNoCopy(shortlist, n);
      shortlistDistances = ArrayUtil.growNoCopy(shortlistDistances, n);
      int capacity = Integer.highestOneBit(Math.max(n, 16)) << 2;
      if (dedupKeys.length < capacity) {
        dedupKeys = new int[capacity];
        dedupStamps = new int[capacity];
        dedupMask = capacity - 1;
        dedupStamp = 0;
      }
    }

    void newDedup() {
      if (++dedupStamp == 0) {
        Arrays.fill(dedupStamps, 0);
        dedupStamp = 1;
      }
    }

    boolean addDistinct(int doc) {
      int h = (doc * 0x9E3779B9) >>> 1 & dedupMask;
      for (; dedupStamps[h] == dedupStamp; h = (h + 1) & dedupMask) {
        if (dedupKeys[h] == doc) return false;
      }
      dedupStamps[h] = dedupStamp;
      dedupKeys[h] = doc;
      return true;
    }
  }

  @Override
  public void search(String name, float[] target, KnnCollector collector, AcceptDocs acceptDocs)
      throws IOException {
    Field field = field(name);
    if (field != null) field.new Search(target, collector, collector.k()).run(acceptDocs);
  }

  /**
   * Runs this segment's filtered or unfiltered coarse search for {@code strategy} and returns its
   * deduplicated shortlist of accepted documents, without reading any fine record, for a
   * cross-segment rerank.
   */
  Candidates candidates(
      String name, float[] target, int k, SearchStrategy strategy, AcceptDocs accept)
      throws IOException {
    Field field = field(name);
    if (field == null) return Candidates.EMPTY;
    Field.Search search = field.new Search(target, null, k);
    search.run(strategy.numProbes, strategy.probeMargin, accept);
    return search.gathered == null ? Candidates.EMPTY : search.gathered;
  }

  /** Fine-reranks {@code slots}, taken from {@code from}'s shortlist, into {@code collector}. */
  void rerank(Candidates from, int[] slots, KnnCollector collector) throws IOException {
    from.search().rerankInto(slots, collector);
  }

  Field field(String name) throws IOException {
    Field field = fields.get(name);
    return field == null || field.count == 0 ? null : field.open();
  }

  boolean readsFineDirectly() {
    return uring != null;
  }

  String segment() {
    return segment;
  }

  @Override
  public FloatVectorValues getFloatVectorValues(String name) throws IOException {
    Field field = fields.get(name);
    return field == null ? null : field.open().new Values();
  }

  @Override
  public ByteVectorValues getByteVectorValues(String field) {
    return null;
  }

  @Override
  public Float16VectorValues getFloat16VectorValues(String field) {
    return null;
  }

  @Override
  public void search(String field, byte[] target, KnnCollector collector, AcceptDocs acceptDocs) {
    throw new UnsupportedOperationException("SegmentIVF supports only FLOAT32 vectors");
  }

  @Override
  public void search(String field, short[] target, KnnCollector collector, AcceptDocs acceptDocs) {
    throw new UnsupportedOperationException("SegmentIVF supports only FLOAT32 vectors");
  }

  @Override
  public void checkIntegrity(MergePolicy.OneMerge merge) throws IOException {
    CodecUtil.checksumEntireFile(data);
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) return; // a second close must not close a reused file descriptor
    closed = true;
    Uring.addFine(-fineBytes);
    Uring.releasePinned(pinnedBytes.get());
    IOUtils.close(pinned::close, data, uring); // the arena first: it also scopes mapped views
  }

  /**
   * Batched buffered reads through Linux io_uring via raw syscalls, for a rerank's scattered fine
   * records when memory is short (see {@link #useUring}): page-cache misses run in parallel while
   * cached records still come from the page cache. Rings and buffers are per thread; each reader
   * owns only its file descriptor. Also holds the memory policy.
   */
  @SuppressWarnings("restricted")
  static final class Uring implements Closeable {
    private static final int RING_ENTRIES = 1024;
    private static final long SYS_IO_URING_SETUP = 425, SYS_IO_URING_ENTER = 426;
    private static final long OFF_SQ_RING = 0L, OFF_CQ_RING = 0x8000000L, OFF_SQES = 0x10000000L;
    private static final int GETEVENTS = 1, OP_READ = 22, FEAT_SINGLE_MMAP = 1;
    private static final int PROT_RW = 1 | 2, MAP_SHARED = 1, O_RDONLY = 0;
    private static final int SQE_SIZE = 64, CQE_SIZE = 16, PARAMS_SIZE = 120, EINTR = 4;
    private static final boolean LINUX =
        System.getProperty("os.name", "").toLowerCase(Locale.ROOT).startsWith("linux");

    /**
     * Linux native bindings are held separately so loading the memory policy on another platform
     * never tries to resolve libc symbols.
     */
    private static final class Native {
      static final MethodHandle SYSCALL, MMAP, OPEN, CLOSE;

      static {
        ValueLayout.OfLong j = ValueLayout.JAVA_LONG;
        ValueLayout.OfInt i = ValueLayout.JAVA_INT;
        AddressLayout p = ValueLayout.ADDRESS;
        SYSCALL = libc("syscall", FunctionDescriptor.of(j, j, j, j, j, j, j, j), 1);
        MMAP = libc("mmap", FunctionDescriptor.of(p, p, j, i, i, i, j), -1);
        OPEN = libc("open", FunctionDescriptor.of(i, p, i), 2);
        CLOSE = libc("close", FunctionDescriptor.of(i, i), -1);
      }

      private static MethodHandle libc(String name, FunctionDescriptor fd, int firstVariadicArg) {
        Linker linker = Linker.nativeLinker();
        MemorySegment symbol = linker.defaultLookup().find(name).orElseThrow();
        return firstVariadicArg < 0
            ? linker.downcallHandle(symbol, fd)
            : linker.downcallHandle(symbol, fd, Linker.Option.firstVariadicArg(firstVariadicArg));
      }
    }

    private static final ThreadLocal<Ring> RINGS = ThreadLocal.withInitial(Ring::create);

    // Memory policy. Mapped fine reads are fastest while cached, but each miss is a synchronous
    // fault; once the fine tiers outgrow what the heap and pinned coarse codes leave, or the cgroup
    // reports memory pressure (PSI some avg10, on above ON and off below OFF percent), reranks use
    // one batched submission instead. Rechecked at most once per second.
    private static final long REFRESH_NANOS = 1_000_000_000L;
    private static final double PSI_ON = 1.0, PSI_OFF = 0.1;
    private static final AtomicLong FINE_BYTES = new AtomicLong(), PINNED_BYTES = new AtomicLong();
    private static final Path CGROUP = cgroup();

    /** Test hook: when non-null, overrides the fine-read decision. */
    static volatile Boolean forceUring;

    private static volatile long checkedAt = System.nanoTime() - REFRESH_NANOS;
    private static volatile boolean uring;

    static void addFine(long bytes) {
      FINE_BYTES.addAndGet(bytes);
    }

    /** Reserves pinned memory if it fits in 3/4 of what the memory limit leaves beyond the heap. */
    static boolean reservePinned(long bytes) {
      long limit = (memoryLimit() - Runtime.getRuntime().maxMemory()) / 4 * 3;
      for (long used = PINNED_BYTES.get(); used + bytes <= limit; used = PINNED_BYTES.get()) {
        if (PINNED_BYTES.compareAndSet(used, used + bytes)) return true;
      }
      return false;
    }

    /** Copies pinned sections off the search threads, one segment at a time. */
    static final ExecutorService PINNER =
        Executors.newSingleThreadExecutor(
            Thread.ofPlatform().name("segmentivf-pin").daemon(true).factory());

    static void releasePinned(long bytes) {
      PINNED_BYTES.addAndGet(-bytes);
    }

    /** Returns whether fine records should currently be read with io_uring. */
    static boolean useUring() {
      Boolean forced = forceUring;
      if (forced != null) return forced;
      long now = System.nanoTime();
      if (now - checkedAt >= REFRESH_NANOS) {
        checkedAt = now;
        long room = memoryLimit() - Runtime.getRuntime().maxMemory() - PINNED_BYTES.get();
        double psi = pressure();
        uring = FINE_BYTES.get() > room || psi > (uring ? PSI_OFF : PSI_ON);
      }
      return uring;
    }

    /** The smallest cgroup {@code memory.max} on the path to the root, else physical memory. */
    private static long memoryLimit() {
      long limit = Long.MAX_VALUE;
      for (Path dir = CGROUP;
          dir != null && dir.startsWith("/sys/fs/cgroup");
          dir = dir.getParent()) {
        String max = read(dir.resolve("memory.max"));
        if (max != null && max.isEmpty() == false && max.equals("max") == false) {
          limit = Math.min(limit, Long.parseLong(max));
        }
      }
      if (limit != Long.MAX_VALUE) return limit;
      String meminfo = read(Path.of("/proc/meminfo"));
      if (meminfo != null && meminfo.startsWith("MemTotal:")) {
        String kb = meminfo.substring(9, meminfo.indexOf('\n')).replace("kB", "").trim();
        return Long.parseLong(kb) * 1024;
      }
      return Long.MAX_VALUE;
    }

    /** This cgroup's {@code some avg10} memory pressure in percent, or 0 when unavailable. */
    private static double pressure() {
      String psi = CGROUP == null ? null : read(CGROUP.resolve("memory.pressure"));
      if (psi == null) psi = read(Path.of("/proc/pressure/memory"));
      if (psi == null || psi.startsWith("some avg10=") == false) return 0;
      return Double.parseDouble(psi.substring(11, psi.indexOf(' ', 11)));
    }

    private static Path cgroup() {
      String line = read(Path.of("/proc/self/cgroup"));
      if (line == null || line.startsWith("0::") == false) return null;
      String rel = line.substring(3, line.indexOf('\n') < 0 ? line.length() : line.indexOf('\n'));
      return Path.of("/sys/fs/cgroup" + rel);
    }

    private static String read(Path file) {
      try {
        List<String> lines = Files.readAllLines(file, StandardCharsets.US_ASCII);
        return String.join("\n", lines).trim();
      } catch (IOException | SecurityException _) {
        return null;
      }
    }

    private final int fd;

    private Uring(int fd) {
      this.fd = fd;
    }

    static Uring open(Path path) throws IOException {
      if (LINUX == false) throw new IOException("io_uring is unsupported on this platform");
      int fd;
      try (Arena arena = Arena.ofConfined()) {
        fd = (int) Native.OPEN.invokeExact(arena.allocateFrom(path.toString()), O_RDONLY);
      } catch (Throwable t) {
        throw new IOException("open failed for " + path, t);
      }
      if (fd < 0) throw new IOException("open failed for " + path);
      try {
        RINGS.get();
      } catch (RuntimeException e) {
        closeFd(fd);
        throw new IOException("io_uring unavailable", e);
      }
      return new Uring(fd);
    }

    /**
     * Reads {@code length} bytes at every position in one submission, record {@code k} landing at
     * {@code k * length} of the returned per-thread buffer, valid until this thread's next batch.
     */
    MemorySegment readBatch(long[] positions, int length) throws IOException {
      int n = positions.length;
      long[] bufferOffset = new long[n];
      int[] lengths = new int[n];
      for (int k = 0; k < n; k++) {
        bufferOffset[k] = (long) k * length;
        lengths[k] = length;
      }
      Ring ring = RINGS.get();
      MemorySegment buffer = ring.ensure((long) n * length);
      for (int done = 0; done < n; ) {
        int batch = Math.min(RING_ENTRIES, n - done);
        ring.read(fd, positions, lengths, lengths, bufferOffset, done, batch);
        done += batch;
      }
      return buffer;
    }

    @Override
    public void close() throws IOException {
      closeFd(fd);
    }

    private static void closeFd(int fd) throws IOException {
      int rc;
      try {
        rc = (int) Native.CLOSE.invokeExact(fd);
      } catch (Throwable t) {
        throw new IOException("close failed", t);
      }
      if (rc != 0) throw new IOException("close failed");
    }

    private static long syscall(long n, long a, long b, long c, long d) throws Throwable {
      return (long) Native.SYSCALL.invokeExact(n, a, b, c, d, 0L, 0L);
    }

    private static final class Ring {
      final int ringFd, sqTail, sqArray, sqMask, cqHead, cqTail, cqCqes, cqMask;
      final MemorySegment sq, cq, sqes;
      MemorySegment buffer = MemorySegment.NULL;

      private Ring(
          int ringFd,
          MemorySegment params,
          MemorySegment sq,
          MemorySegment cq,
          MemorySegment sqes) {
        this.ringFd = ringFd;
        this.sq = sq;
        this.cq = cq;
        this.sqes = sqes;
        sqTail = params.get(ValueLayout.JAVA_INT, 44);
        sqMask = sq.get(ValueLayout.JAVA_INT, params.get(ValueLayout.JAVA_INT, 48));
        sqArray = params.get(ValueLayout.JAVA_INT, 64);
        cqHead = params.get(ValueLayout.JAVA_INT, 80);
        cqTail = params.get(ValueLayout.JAVA_INT, 84);
        cqMask = cq.get(ValueLayout.JAVA_INT, params.get(ValueLayout.JAVA_INT, 88));
        cqCqes = params.get(ValueLayout.JAVA_INT, 100);
      }

      static Ring create() {
        MemorySegment params = Arena.ofAuto().allocate(PARAMS_SIZE);
        try {
          long ringFd = syscall(SYS_IO_URING_SETUP, RING_ENTRIES, params.address(), 0, 0);
          if (ringFd < 0) throw new IllegalStateException("io_uring_setup errno=" + -ringFd);
          int sqEntries = params.get(ValueLayout.JAVA_INT, 0);
          int cqEntries = params.get(ValueLayout.JAVA_INT, 4);
          boolean single = (params.get(ValueLayout.JAVA_INT, 20) & FEAT_SINGLE_MMAP) != 0;
          long sqSize = params.get(ValueLayout.JAVA_INT, 64) + (long) sqEntries * Integer.BYTES;
          long cqSize = params.get(ValueLayout.JAVA_INT, 100) + (long) cqEntries * CQE_SIZE;
          MemorySegment sq = map(single ? Math.max(sqSize, cqSize) : sqSize, ringFd, OFF_SQ_RING);
          MemorySegment cq = single ? sq : map(cqSize, ringFd, OFF_CQ_RING);
          MemorySegment sqes = map((long) sqEntries * SQE_SIZE, ringFd, OFF_SQES);
          return new Ring((int) ringFd, params, sq, cq, sqes);
        } catch (RuntimeException e) {
          throw e;
        } catch (Throwable t) {
          throw new IllegalStateException("io_uring setup failed", t);
        }
      }

      MemorySegment ensure(long size) {
        if (buffer.byteSize() < size) buffer = Arena.ofAuto().allocate(size, Long.BYTES);
        return buffer;
      }

      void read(
          int fd,
          long[] start,
          int[] length,
          int[] needed,
          long[] bufferOffset,
          int from,
          int count)
          throws IOException {
        int tail = sq.get(ValueLayout.JAVA_INT, sqTail);
        for (int k = 0; k < count; k++) {
          int index = (tail + k) & sqMask;
          long sqe = (long) index * SQE_SIZE;
          sqes.asSlice(sqe, SQE_SIZE).fill((byte) 0);
          sqes.set(ValueLayout.JAVA_BYTE, sqe, (byte) OP_READ);
          sqes.set(ValueLayout.JAVA_INT, sqe + 4, fd);
          sqes.set(ValueLayout.JAVA_LONG, sqe + 8, start[from + k]);
          sqes.set(ValueLayout.JAVA_LONG, sqe + 16, buffer.address() + bufferOffset[from + k]);
          sqes.set(ValueLayout.JAVA_INT, sqe + 24, length[from + k]);
          sqes.set(ValueLayout.JAVA_LONG, sqe + 32, from + k);
          sq.set(ValueLayout.JAVA_INT, sqArray + (long) index * Integer.BYTES, index);
        }
        VarHandle.fullFence();
        sq.set(ValueLayout.JAVA_INT, sqTail, tail + count);
        VarHandle.fullFence();
        int submitted = 0, reaped = 0;
        IOException failure = null;
        while (reaped < count) {
          long ret;
          try {
            ret = syscall(SYS_IO_URING_ENTER, ringFd, count - submitted, count - reaped, GETEVENTS);
          } catch (Throwable t) {
            throw new IOException("io_uring_enter failed", t);
          }
          if (ret < 0 && ret != -EINTR) throw new IOException("io_uring_enter errno=" + -ret);
          if (ret > 0) submitted += (int) ret;
          VarHandle.fullFence();
          int head = cq.get(ValueLayout.JAVA_INT, cqHead),
              end = cq.get(ValueLayout.JAVA_INT, cqTail);
          for (; head != end; head++, reaped++) {
            long cqe = cqCqes + (long) (head & cqMask) * CQE_SIZE;
            int entry = (int) cq.get(ValueLayout.JAVA_LONG, cqe);
            int res = cq.get(ValueLayout.JAVA_INT, cqe + 8);
            if (res < needed[entry] && failure == null) {
              failure = new IOException("io_uring read res=" + res + " want=" + needed[entry]);
            }
          }
          VarHandle.fullFence();
          cq.set(ValueLayout.JAVA_INT, cqHead, head);
        }
        if (failure != null) throw failure;
      }

      private static MemorySegment map(long length, long fd, long offset) throws Throwable {
        MemorySegment p =
            (MemorySegment)
                Native.MMAP.invokeExact(
                    MemorySegment.NULL, length, PROT_RW, MAP_SHARED, (int) fd, offset);
        if (p.address() == -1L) throw new IllegalStateException("io_uring mmap failed");
        return p.reinterpret(length);
      }
    }
  }
}
