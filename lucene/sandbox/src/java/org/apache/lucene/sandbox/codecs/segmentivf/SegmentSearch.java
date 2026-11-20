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

import static org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.SECTION_COARSE;
import static org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.SECTION_FINE;
import static org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.SECTION_SLOT_DOCS;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.SearchStrategy;
import org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsReader.Candidates;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IntroSelector;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.VectorUtil;

/**
 * One query's search of one field of one segment.
 *
 * <p>An unfiltered search runs in three steps:
 *
 * <ol>
 *   <li><b>Select cells.</b> Walk the centroid graph for candidate cells, rank them exactly, and
 *       keep the nearest {@code probes} ({@link #selectCells}).
 *   <li><b>Scan.</b> Score every slot of the selected cells by coarse distance and admit the
 *       nearest into a {@link CoarsePool} of {@code shortlist * (1 + spillBits)} slots ({@link
 *       #scan}).
 *   <li><b>Rerank.</b> Take the nearest pooled slots, dropping spill duplicates of a document,
 *       until the shortlist is full, then score their fine codes ({@link #shortlistPool}).
 * </ol>
 *
 * <p>A filter reaches the search as the accepted documents' bit set ({@link FilterPlan.Mode}). One
 * whose matches are few is searched exactly, by reranking every match ({@link #exactFiltered}).
 * Otherwise the selected cells are scanned for matches by testing each slot's document against the
 * bit set ({@link #scanBitSetRound}). With widening, the search probes more cells in rounds until
 * enough matches are found ({@link #probeInRounds}). One that still finds fewer than {@code k},
 * with cells left unprobed, scores every match by coarse code instead ({@link
 * #coarseExactFiltered}): the matches may be far from the query, in cells the probes never reach.
 *
 * <p>Without a collector, a search stops before the rerank and keeps its shortlist as {@link
 * #gathered}, for {@link SegmentIVFKnnQuery} to merge with other segments' and rerank only the
 * index-wide best.
 */
final class SegmentSearch {
  /** A widening filtered search probes at most this many times the first round's cells. */
  private static final int FILTERED_PROBE_MULTIPLIER = 8;

  /** Graph candidates ranked exactly: {@code max(RANK_MIN, RANK_MULTIPLIER * probes)}. */
  private static final int RANK_MIN = 64;

  private static final int RANK_MULTIPLIER = 2;

  /** Fine codes are read and scored this many at a time, bounding a rerank's buffers. */
  private static final int RERANK_BLOCK = 1024;

  private final IVFFieldReader field;
  private final FineCodeIO fineCodeIO;
  // Held from construction until the scan, or for a rerank-only search the rerank, ends.
  private final SearchScratch scratch = SearchScratch.acquire();
  private final KnnCollector collector;
  private final SearchStrategy strategy;

  // Slices, each opened on first use: mapped sections are read in place and need none.
  private RandomAccessInput codes;
  private RandomAccessInput coarse;
  private RandomAccessInput slotDocs;

  /** The normalized, rotated query. */
  private final float[] rotated;

  /** The prepared coarse query, leading with its centroid code; not yet anchored. */
  private final byte[] coarseQuery;

  /**
   * The coarse query anchored to {@link #anchoredCell}'s centroid. Anchoring writes only a few
   * trailing floats, so one copy is re-anchored as the scan moves between cells.
   */
  private byte[] anchoredQuery;

  private int anchoredCell = -1;

  /** Each cell's exact {@code q.a} once {@link #rankCells} has ranked it; NaN until then. */
  private float[] anchorDots;

  private final FineCodec.Query fineQuery;

  /** The number of nearest documents asked for. */
  private final int k;

  /** Distinct documents to rerank: {@code min(count, strategy.rerankCount(k))}. */
  private final int shortlistSize;

  /** Slots to pool: a document may fill up to {@code 1 + spillBits} of them. */
  private final int poolSize;

  private final CoarsePool pool;

  // A filtered scan's progress: matches scored, and distinct matching documents seen, up to the
  // shortlist size.
  private long survivors;
  private int distinct;

  /** Without a collector, the deduplicated shortlist this search gathered. */
  Candidates gathered;

  /**
   * A search of {@code field} for the {@code k} nearest to {@code target}, with {@code strategy},
   * or with the collector's strategy or the field's defaults when it is null.
   */
  SegmentSearch(
      IVFFieldReader field,
      FineCodeIO fineCodeIO,
      float[] target,
      KnnCollector collector,
      int k,
      SearchStrategy strategy)
      throws IOException {
    this.field = field;
    this.fineCodeIO = fineCodeIO;
    this.collector = collector;
    this.k = k;
    // A search that fails to construct never runs, so nothing else would release the scratch.
    try {
      if (target.length != field.dim) {
        throw new IllegalArgumentException(
            "vector query dimension: "
                + target.length
                + " differs from field dimension: "
                + field.dim);
      }
      if (strategy == null
          && collector != null
          && collector.getSearchStrategy() instanceof SearchStrategy own) {
        strategy = own;
      }
      if (strategy == null) {
        strategy = new SearchStrategy(field.nprobe);
      }
      this.strategy = strategy;
      shortlistSize = (int) Math.min(field.count, strategy.rerankCount(k));
      poolSize = Math.multiplyExact(shortlistSize, 1 + field.spillBits);
      scratch.reserve(shortlistSize);
      pool = new CoarsePool(scratch, field.coarseCodec.bins, poolSize);
      CentroidCodes centroidCodes = field.codes();
      // The rerank scores the query at its own length, rotated exactly as the exact scorer rotates
      // it (IVFVectorValues#scorer), so the two paths score a document alike. Cells and coarse
      // codes rank by direction, the unit query. Both tiers share one quantization of it.
      float[] scaled = new float[field.dim];
      field.rotation.rotate(target, scaled);
      // In double: the float sum of squares overflows for a finite query longer than ~1.8e19.
      double normSquared = 0;
      for (float x : scaled) {
        normSquared += (double) x * x;
      }
      double norm = Math.sqrt(normSquared);
      rotated = new float[field.dim];
      if (norm > 0) {
        for (int d = 0; d < field.dim; d++) {
          rotated[d] = (float) (scaled[d] / norm);
        }
      }
      QueryLevels levels = new QueryLevels(scaled, (float) Math.min(norm, Float.MAX_VALUE));
      coarseQuery = new byte[field.coarseCodec.queryBytes];
      centroidCodes.prepare(rotated, levels, coarseQuery);
      fineQuery = field.fine.query(scaled, field.similarity, levels);
    } catch (Throwable t) {
      scratch.release();
      throw t;
    }
  }

  /** A search that only reranks part of {@code from}'s shortlist, reusing its prepared query. */
  private SegmentSearch(SegmentSearch from, KnnCollector collector) {
    field = from.field;
    fineCodeIO = from.fineCodeIO;
    this.collector = collector;
    strategy = from.strategy;
    k = from.k;
    shortlistSize = from.shortlistSize;
    poolSize = from.poolSize;
    pool = null;
    rotated = from.rotated;
    coarseQuery = from.coarseQuery;
    fineQuery = from.fineQuery;
  }

  /** Fine-reranks {@code ords}, taken from this search's shortlist, into {@code collector}. */
  void rerankInto(int[] ords, KnnCollector collector) throws IOException {
    SegmentSearch rerank = new SegmentSearch(this, collector);
    try {
      rerank.rerank(ords, ords.length);
    } finally {
      rerank.scratch.release();
    }
  }

  /**
   * The first half of a cross-segment rerank: plans the fine reads of {@code ords}, taken from this
   * search's shortlist, or returns null when they are read mapped. {@link FineCodeIO#readAll} then
   * reads every segment's plan in one batch, and {@link #rerankInto(int[], FineCodeIO.Plan,
   * MemorySegment, KnnCollector)} scores each.
   */
  FineCodeIO.Plan planRerank(int[] ords) {
    SearchScratch planning = SearchScratch.acquire();
    try {
      return fineCodeIO.plan(field, ords, 0, ords.length, planning);
    } finally {
      planning.release();
    }
  }

  /**
   * The second half: fine-reranks {@code ords} into {@code collector}, their codes read into {@code
   * source} at {@code plan}'s offsets, or read mapped when there is no plan or the batch failed
   * ({@code source} null).
   */
  void rerankInto(int[] ords, FineCodeIO.Plan plan, MemorySegment source, KnnCollector collector)
      throws IOException {
    SegmentSearch rerank = new SegmentSearch(this, collector);
    try {
      if (plan != null && source != null) {
        rerank.score(ords, 0, ords.length, source, plan.offsets, field.ordToDocIfSparse());
        collector.incVisitedCount(ords.length);
      } else {
        rerank.rerankMapped(ords);
      }
    } finally {
      rerank.scratch.release();
    }
  }

  // ---- Entry points.

  /** Searches with the accepted documents as bits, testing each probed slot against a filter. */
  void run(AcceptDocs acceptDocs) throws IOException {
    try {
      scan(acceptDocs);
    } finally {
      scratch.release();
    }
  }

  private void scan(AcceptDocs acceptDocs) throws IOException {
    int probes = Math.min(strategy.numProbes, field.nlist);
    Bits accept = acceptDocs == null ? null : acceptDocs.bits();
    if (accept instanceof BitSet filter) {
      int matches = acceptDocs.cost();
      if (matches <= exactFilterLimit()) {
        exactFiltered(acceptDocs.iterator());
      } else {
        int[] seed = selectCells(probes);
        int probed =
            probeInRounds(
                seed,
                matches,
                probes,
                strategy.widen,
                (cells, n) -> scanBitSetRound(cells, n, filter));
        if (tooFewMatches(probed)) {
          coarseExactFiltered(acceptDocs.iterator());
        } else {
          shortlistPool((int) Math.min(poolSize, survivors));
        }
      }
    } else {
      scanUnfiltered(probes, accept);
    }
  }

  /**
   * Whether a probed filtered search that probed {@code probed} cells found fewer than {@code k}
   * matching documents while cells were left unprobed, so that the matches it missed may hold
   * nearer ones than it found. Such a search falls back to {@link #coarseExactFiltered}.
   */
  private boolean tooFewMatches(int probed) {
    return distinct < Math.min(k, shortlistSize) && probed < field.nlist;
  }

  /**
   * The most filter matches searched exactly. Reranking every match reads one fine code each;
   * scanning reads a coarse code per probed slot. This is roughly where the two break even: the
   * geometric mean of the shortlist and the slot count, scaled by how much smaller a coarse code is
   * than a fine one.
   */
  private long exactFilterLimit() {
    double totalSlots = field.cellStart[field.nlist];
    double breakEven =
        Math.sqrt(shortlistSize * totalSlots * field.coarseBytes / field.fine.recordBytes);
    return (long) Math.max(shortlistSize, Math.min(Integer.MAX_VALUE, breakEven));
  }

  private void scanUnfiltered(int probes, Bits liveDocs) throws IOException {
    if (field.cellStart[field.nlist] <= poolSize) {
      // A segment no larger than the pool scans every slot: its mostly empty cells would waste
      // probes.
      scan(field.allCells, liveDocs);
    } else {
      scan(selectCells(probes), liveDocs);
    }
  }

  /**
   * Scans {@code cells}, then shortlists the pool. The cells were chosen to hold pool-many slots,
   * but deleted documents' slots count among them and are not admitted. While the pool holds fewer
   * than pool-many live slots, the next nearest cells are scanned too: pool-many slots hold at
   * least shortlist-many documents, so the shortlist is as full as without deletions.
   */
  private void scan(int[] cells, Bits liveDocs) throws IOException {
    long total = scanCells(cells, cells.length, liveDocs);
    if (liveDocs != null && pool.admitted() < poolSize && cells.length < field.nlist) {
      boolean[] scanned = new boolean[field.nlist];
      for (int cell : cells) {
        scanned[cell] = true;
      }
      // Every cell, by exact distance: allCells[i] == i, so a ranked index is its cell.
      long[] ranked = rankCells(field.allCells, field.nlist, field.nlist);
      int[] next = new int[1];
      for (int i = 0; i < field.nlist && pool.admitted() < poolSize; i++) {
        int cell = (int) ranked[i];
        if (scanned[cell] == false) {
          next[0] = cell;
          total += scanCells(next, 1, liveDocs);
        }
      }
    }
    shortlistPool((int) Math.min(poolSize, total));
  }

  // ---- Cell selection.

  /**
   * Selects the {@code probes} cells nearest the query. The graph proposes candidates by coarse
   * distance, which are ranked exactly. Without a graph, every cell is a candidate.
   *
   * <p>Cells too few to fill the pool are followed by the next nearest ({@link #fillPool}).
   *
   * <p>Small segments are probed too, not scanned whole: the cell count is fixed by the format, so
   * a small segment's cells are small, and each probed cell costs a whole block of coarse codes and
   * an exact centroid product however few slots it holds. Scanning every cell of many small
   * segments multiplies that by the cell count.
   */
  private int[] selectCells(int probes) throws IOException {
    int[] candidates = field.allCells;
    int found = field.nlist;
    if (field.graph != null) {
      candidates = scratch.graphCells = ArrayUtil.growNoCopy(scratch.graphCells, field.nlist);
      int[] distances =
          scratch.graphDistances = ArrayUtil.growNoCopy(scratch.graphDistances, field.nlist);
      int ef = Math.max(CentroidGraph.MIN_EF, probes * CentroidGraph.EF_MULTIPLIER);
      found = field.graph.search(coarseQuery, ef, candidates, distances);
      int limit = Math.max(RANK_MIN, probes * RANK_MULTIPLIER);
      found = keepNearestByCoarse(candidates, distances, found, limit);
    }
    int keep = Math.min(probes, found);
    long[] ranked = rankCells(candidates, found, keep);
    int[] cells = new int[keep];
    for (int i = 0; i < keep; i++) {
      cells[i] = candidates[(int) ranked[i]];
    }
    return fillPool(cells);
  }

  /**
   * {@code cells}, followed, if they hold fewer slots than the pool, by the nearest other cells
   * until they hold as many. Pool-many slots hold at least shortlist-many documents, however they
   * spill, so a large {@code k} whose shortlist outgrows the probed cells still fills it. Rare: it
   * takes a shortlist near the probed cells' size, and ranking every cell is then the lesser cost.
   */
  private int[] fillPool(int[] cells) throws IOException {
    int nlist = field.nlist;
    long slots = 0;
    for (int cell : cells) {
      slots += field.cellStart[cell + 1] - field.cellStart[cell];
    }
    if (slots >= poolSize || cells.length == nlist) {
      return cells;
    }
    boolean[] chosen = new boolean[nlist];
    for (int cell : cells) {
      chosen[cell] = true;
    }
    int[] filled = ArrayUtil.growExact(cells, nlist);
    int n = cells.length;
    // Every cell, by exact distance: allCells[i] == i, so a ranked index is its cell.
    long[] ranked = rankCells(field.allCells, nlist, nlist);
    for (int i = 0; i < nlist && slots < poolSize; i++) {
      int cell = (int) ranked[i];
      if (chosen[cell] == false) {
        chosen[cell] = true;
        filled[n++] = cell;
        slots += field.cellStart[cell + 1] - field.cellStart[cell];
      }
    }
    return ArrayUtil.copyOfSubArray(filled, 0, n);
  }

  /**
   * Keeps the {@code limit} candidates nearest by coarse distance, compacted in place in the order
   * the graph returned them, and returns how many are kept.
   *
   * <p>A counting selection: coarse distances are small integers, so counting candidates per
   * distance finds the bound at which the running count reaches {@code limit}. Candidates nearer
   * than the bound are kept, and at the bound only as many as fill the limit.
   */
  private int keepNearestByCoarse(int[] candidates, int[] distances, int found, int limit) {
    if (limit >= found) {
      return found;
    }
    int[] counts = new int[field.coarseCodec.bins];
    for (int i = 0; i < found; i++) {
      counts[distances[i]]++;
    }
    int nearer = 0;
    int bound = 0;
    while (nearer + counts[bound] <= limit) {
      nearer += counts[bound];
      bound++;
    }
    int ties = limit - nearer;
    int kept = 0;
    for (int i = 0; i < found && kept < limit; i++) {
      boolean keep = distances[i] < bound;
      if (distances[i] == bound && ties > 0) {
        ties--;
        keep = true;
      }
      if (keep) {
        candidates[kept] = candidates[i];
        distances[kept] = distances[i];
        kept++;
      }
    }
    return kept;
  }

  /**
   * Ranks {@code n} cells by exact distance, leaving their distances in {@link
   * SearchScratch#cellDistances}. Returns keys {@code sortableDistance << 32 | index}, the first
   * {@code top} of which are the nearest in order; the rest are unordered.
   */
  private long[] rankCells(int[] cells, int n, int top) throws IOException {
    float[] distances = scratch.cellDistances = ArrayUtil.grow(scratch.cellDistances, n);
    field.codes().rankCandidates(rotated, cells, n, distances);
    if (anchorDots == null) {
      anchorDots = new float[field.nlist];
      Arrays.fill(anchorDots, Float.NaN);
    }
    for (int i = 0; i < n; i++) {
      anchorDots[cells[i]] = -distances[i];
    }
    long[] ranked = new long[n];
    for (int i = 0; i < n; i++) {
      // + 0f turns -0 into 0, so the two sort together.
      ranked[i] = ((long) NumericUtils.floatToSortableInt(distances[i] + 0f) << 32) | i;
    }
    if (top < n) {
      selectSmallest(ranked, n, top);
    }
    Arrays.sort(ranked, 0, Math.min(top, n));
    return ranked;
  }

  /** Moves the {@code k} smallest of the first {@code n} values to the front, unordered. */
  static void selectSmallest(long[] values, int n, int k) {
    new IntroSelector() {
      long pivot;

      @Override
      protected void swap(int i, int j) {
        long swap = values[i];
        values[i] = values[j];
        values[j] = swap;
      }

      @Override
      protected void setPivot(int i) {
        pivot = values[i];
      }

      @Override
      protected int comparePivot(int j) {
        return Long.compare(pivot, values[j]);
      }
    }.select(0, n, k);
  }

  // ---- Coarse scoring.

  private RandomAccessInput coarse() throws IOException {
    if (coarse == null) {
      coarse = field.section(SECTION_COARSE);
    }
    return coarse;
  }

  /** The document in {@code slot}. */
  private int doc(int slot) throws IOException {
    if (slotDocs == null) {
      slotDocs = field.section(SECTION_SLOT_DOCS);
    }
    return field.docAt(slotDocs, slot);
  }

  /** The coarse query to compare with the codes of {@code cell}. */
  private byte[] cellQuery(int cell) {
    if (anchoredQuery == null) {
      anchoredQuery = coarseQuery.clone();
    }
    if (anchoredCell != cell) {
      if (anchorDots == null) {
        anchorDots = new float[field.nlist];
        Arrays.fill(anchorDots, Float.NaN);
      }
      // Ranking the cells computed q.a exactly; a cell reached otherwise computes it here, once.
      float anchorDot = anchorDots[cell];
      if (Float.isNaN(anchorDot)) {
        anchorDot = VectorUtil.dotProduct(rotated, field.centroids[cell]);
        anchorDots[cell] = anchorDot;
      }
      field.coarseCodec.anchorQuery(anchoredQuery, anchorDot, field.coarseParameters);
      anchoredCell = cell;
    }
    return anchoredQuery;
  }

  /** Coarse distances to every slot of {@code cell}, in slot order. */
  private void coarseDistances(int cell, int[] out) throws IOException {
    byte[] query = cellQuery(cell);
    int rows = field.cellStart[cell + 1] - field.cellStart[cell];
    MemorySegment mapped = field.coarseRegion(cell);
    if (mapped != null) {
      field.coarseCodec.distances(query, mapped, field.coarseRegionBase(cell), rows, out);
    } else {
      long start = field.coarseRegions[cell];
      MemorySegment copied = copy(start, field.coarseRegions[cell + 1] - start);
      field.coarseCodec.distances(query, copied, 0, rows, out);
    }
  }

  /**
   * The coarse distance to one slot. Unmapped, only the block holding it is copied: the exact
   * filtered paths visit slots in document order, across cells, so copying whole regions would read
   * a cell's codes for every match.
   */
  private int coarseDistance(int slot) throws IOException {
    int cell = field.cellOfSlot(slot);
    byte[] query = cellQuery(cell);
    int row = slot - field.cellStart[cell];
    MemorySegment mapped = field.coarseRegion(cell);
    if (mapped != null) {
      return field.coarseCodec.distance(query, mapped, field.coarseRegionBase(cell), row);
    }
    int blockRows = field.coarseCodec.blockRows();
    int blockRow = row - row % blockRows;
    long start = field.coarseRegions[cell] + field.coarseCodec.regionBytes(blockRow);
    MemorySegment copied = copy(start, field.coarseCodec.regionBytes(blockRows));
    return field.coarseCodec.distance(query, copied, 0, row - blockRow);
  }

  // The coarse codes last read without a mapping, kept for the next read of the same bytes.
  private long copiedStart = -1;
  private long copiedLength;
  private byte[] copiedBytes = new byte[0];
  private MemorySegment copiedSegment = MemorySegment.ofArray(copiedBytes);

  /** Reads {@code length} coarse bytes from {@code start}, unless just read, into the copy. */
  private MemorySegment copy(long start, long length) throws IOException {
    if (start != copiedStart || length != copiedLength) {
      int n = Math.toIntExact(length);
      if (copiedBytes.length < n) {
        copiedBytes = ArrayUtil.growNoCopy(copiedBytes, n);
        copiedSegment = MemorySegment.ofArray(copiedBytes);
      }
      copiedStart = -1; // a failed read leaves nothing cached
      coarse().readBytes(start, copiedBytes, 0, n);
      copiedStart = start;
      copiedLength = length;
    }
    return copiedSegment;
  }

  /** Prefetches the coarse codes of {@code n} cells and returns how many slots they hold. */
  private int prefetch(int[] cells, int n) throws IOException {
    int total = 0;
    for (int i = 0; i < n; i++) {
      int cell = cells[i];
      int rows = field.cellStart[cell + 1] - field.cellStart[cell];
      if (rows > 0) {
        total += rows;
        long start = field.coarseRegions[cell];
        coarse().prefetch(start, field.coarseRegions[cell + 1] - start);
      }
    }
    return total;
  }

  // ---- Unfiltered search.

  /** Admits the live slots of {@code n} cells into the pool; returns the slots scanned. */
  private int scanCells(int[] cells, int n, Bits liveDocs) throws IOException {
    int total = prefetch(cells, n);
    pool.reserve(total);
    CoarsePool.SlotFilter live = liveDocs == null ? null : slot -> liveDocs.get(doc(slot));
    for (int c = 0; c < n; c++) {
      int cell = cells[c];
      int firstSlot = field.cellStart[cell];
      int rows = field.cellStart[cell + 1] - firstSlot;
      int[] distances = scratch.rowDistances = ArrayUtil.growNoCopy(scratch.rowDistances, rows);
      coarseDistances(cell, distances);
      for (int from = 0; from < rows; from += SearchScratch.ADMIT_BLOCK) {
        int block = Math.min(SearchScratch.ADMIT_BLOCK, rows - from);
        pool.admitBlock(distances, from, block, firstSlot, live);
      }
    }
    return total;
  }

  // ---- Filtered search.

  /**
   * Searches a filter with few matches exactly: with a collector, every match is reranked; without,
   * every match joins the cross-segment shortlist by its primary slot's coarse distance. A probed
   * search pools a document by its nearest probed copy instead, so in the cross-segment cut this
   * segment's candidates rank slightly farther than a probed segment's: a cost to recall only,
   * since hits are scored by the rerank. The matches are accepted documents, so they are live.
   */
  private void exactFiltered(DocIdSetIterator matches) throws IOException {
    boolean dense = field.dense();
    int[] ordToSlot = field.ordToSlot();
    int[] ordToDoc = dense ? null : field.ordToDoc();
    int[] ids = new int[64];
    int n = 0;
    for (int doc = matches.nextDoc(); doc != NO_MORE_DOCS; doc = matches.nextDoc()) {
      int ord = dense ? doc : Arrays.binarySearch(ordToDoc, doc);
      if (ord < 0) {
        continue;
      }
      ids = ArrayUtil.grow(ids, n + 1);
      if (collector == null) {
        ids[n] = ordToSlot[ord];
      } else {
        ids[n] = ord;
      }
      n++;
    }
    if (collector == null) {
      poolAll(ids, n);
    } else {
      rerank(ids, n);
    }
  }

  /** Pools every given slot by its coarse distance, then shortlists the pool. */
  private void poolAll(int[] slots, int n) throws IOException {
    pool.reserve(n);
    for (int i = 0; i < n; i++) {
      pool.add(coarseDistance(slots[i]), slots[i]);
    }
    shortlistPool(Math.min(poolSize, n));
  }

  /**
   * Searches every filter match by its primary slot's coarse distance, then shortlists and reranks
   * as a probed search does: the fallback when probing found too few ({@link #tooFewMatches}).
   * Unlike {@link #exactFiltered}, which reranks every match from disk, only the shortlist is read,
   * so a filter with many matches costs a coarse scan of them, not a fine read of each.
   */
  private void coarseExactFiltered(DocIdSetIterator matches) throws IOException {
    pool.clear();
    boolean dense = field.dense();
    int[] ordToSlot = field.ordToSlot();
    int[] ordToDoc = dense ? null : field.ordToDoc();
    long admitted = 0;
    pool.reserve(RERANK_BLOCK);
    int room = RERANK_BLOCK;
    for (int doc = matches.nextDoc(); doc != NO_MORE_DOCS; doc = matches.nextDoc()) {
      int ord = dense ? doc : Arrays.binarySearch(ordToDoc, doc);
      if (ord < 0) {
        continue;
      }
      if (room == 0) {
        pool.reserve(RERANK_BLOCK);
        room = RERANK_BLOCK;
      }
      int slot = ordToSlot[ord];
      pool.admit(coarseDistance(slot), slot);
      room--;
      admitted++;
    }
    shortlistPool((int) Math.min(poolSize, admitted));
  }

  /** Scans one round of a filtered search; returns false to end the search. */
  private interface RoundScan {
    boolean scan(int[] cells, int n) throws IOException;
  }

  /**
   * Probes cells for filter matches, starting with the selected {@code seed} cells, then shortlists
   * the pool.
   *
   * <p>Without {@code widen}, the seed cells are probed once. With it, rounds double in size and
   * continue in exact distance order through every cell, until the target probe count is reached
   * and the shortlist can be filled. A selective filter leaves few matches per cell, so the target
   * grows as {@code probes * (count / matches)^e}, with an exponent fitted to the average cell
   * size, up to {@link #FILTERED_PROBE_MULTIPLIER} times the first round's cells, or the target if
   * larger.
   *
   * <p>The caller shortlists the pool, or falls back to an exact search ({@link #tooFewMatches}).
   *
   * @return the number of cells probed
   */
  private int probeInRounds(int[] seed, int matches, int probes, boolean widen, RoundScan rounds)
      throws IOException {
    int nlist = field.nlist;
    int firstRound = Math.max(1, seed.length);
    int targetProbes = firstRound;
    int maxProbes = firstRound;
    if (widen) {
      targetProbes = widenedProbes(matches, probes);
      maxProbes = Math.max(targetProbes, Math.min(nlist, firstRound * FILTERED_PROBE_MULTIPLIER));
    }
    boolean[] probed = new boolean[nlist];
    long[] ranked = null;
    int seedServed = 0;
    int rankedServed = 0;
    survivors = 0;
    distinct = 0;
    scratch.clearDedup();
    int[] batch = new int[maxProbes];
    int done = 0;
    for (int want = firstRound; done < maxProbes; want <<= 1) {
      // The next round's cells: the seed first, then every cell by exact distance.
      want = Math.min(want, maxProbes - done);
      int n = 0;
      while (n < want) {
        if (seedServed == seed.length && ranked == null) {
          ranked = rankCells(field.allCells, nlist, nlist);
        }
        if (rankedServed == nlist) {
          break;
        }
        int cell;
        if (ranked == null) {
          cell = seed[seedServed];
          seedServed++;
        } else {
          cell = (int) ranked[rankedServed];
          rankedServed++;
        }
        if (probed[cell] == false) {
          batch[n++] = cell;
        }
        probed[cell] = true;
      }
      if (n == 0) {
        break;
      }
      done += n;
      pool.reserve(prefetch(batch, n));
      if (rounds.scan(batch, n) == false) {
        break;
      }
      if (done >= targetProbes && distinct >= shortlistSize) {
        break;
      }
    }
    return done;
  }

  /** The probe count a filter matching {@code matches} documents widens to. */
  private int widenedProbes(int matches, int probes) {
    int count = field.count;
    if (matches <= 0 || matches >= count) {
      return probes;
    }
    double exponent = 0.7185 - 0.0578 * Math.log(Math.max(2, count / field.nlist));
    exponent = Math.min(0.50, Math.max(0.15, exponent));
    double widened = Math.ceil(probes * Math.pow((double) count / matches, exponent));
    return (int) Math.min(field.nlist, Math.max(widened, probes));
  }

  /** Records a filter match for the pool and for the round's stopping rule. */
  private void admitMatch(int doc, int distance, int slot) {
    survivors++;
    if (distinct < shortlistSize && scratch.addDistinct(doc)) {
      distinct++;
    }
    pool.admit(distance, slot);
  }

  /**
   * Scores every slot of {@code n} cells, then admits those whose document {@code filter} accepts:
   * a whole cell scores at block speed, faster than its matches one by one.
   */
  private boolean scanBitSetRound(int[] cells, int n, BitSet filter) throws IOException {
    for (int c = 0; c < n; c++) {
      int cell = cells[c];
      int firstSlot = field.cellStart[cell];
      int rows = field.cellStart[cell + 1] - firstSlot;
      int[] distances = scratch.rowDistances = ArrayUtil.growNoCopy(scratch.rowDistances, rows);
      coarseDistances(cell, distances);
      for (int row = 0; row < rows; row++) {
        int slot = firstSlot + row;
        int doc = doc(slot);
        if (filter.get(doc)) {
          admitMatch(doc, distances[row], slot);
        }
      }
    }
    return true;
  }

  // ---- Shortlist and rerank.

  /**
   * Takes the {@code need} nearest pooled slots and keeps the first slot of each document, up to
   * the shortlist size; then reranks them, or keeps them as {@link #gathered}. Every path pools
   * live documents only, rejecting deleted ones as it admits them, so none is checked here.
   */
  private void shortlistPool(int need) throws IOException {
    int selected = pool.selectNearest(need);
    long[] nearest = scratch.selected;
    scratch.clearDedup();
    int[] kept = scratch.shortlist;
    int[] distances = scratch.shortlistDistances;
    int[] ordToDoc = field.ordToDocIfSparse();
    int n = 0;
    for (int i = 0; i < selected && n < shortlistSize; i++) {
      int slot = (int) nearest[i];
      int doc = doc(slot);
      if (scratch.addDistinct(doc)) {
        distances[n] = (int) (nearest[i] >>> 32);
        if (ordToDoc == null) {
          kept[n] = doc;
        } else {
          kept[n] = Arrays.binarySearch(ordToDoc, doc);
        }
        n++;
      }
    }
    if (collector != null) {
      rerank(kept, n);
    } else {
      int[] shortlisted = ArrayUtil.copyOfSubArray(kept, 0, n);
      gathered = new Candidates(shortlisted, ArrayUtil.copyOfSubArray(distances, 0, n), this);
    }
  }

  /** Scores the codes of {@code ords}, read through the mapping, and collects them. */
  private void rerankMapped(int[] ords) throws IOException {
    int[] ordToDoc = field.ordToDocIfSparse();
    for (int from = 0; from < ords.length; from += RERANK_BLOCK) {
      int count = Math.min(RERANK_BLOCK, ords.length - from);
      long[] offsets = scratch.codeOffsets = ArrayUtil.growNoCopy(scratch.codeOffsets, count);
      MemorySegment source =
          fineCodeIO.readMapped(field, ords, from, count, offsets, this::codes, scratch);
      score(ords, from, count, source, offsets, ordToDoc);
    }
    collector.incVisitedCount(ords.length);
  }

  /** Scores the fine codes of {@code n} ordinals and collects them. */
  private void rerank(int[] ords, int n) throws IOException {
    int[] ordToDoc = field.ordToDocIfSparse();
    for (int from = 0; from < n; from += RERANK_BLOCK) {
      rerankBlock(ords, from, Math.min(RERANK_BLOCK, n - from), ordToDoc);
    }
    collector.incVisitedCount(n);
  }

  private void rerankBlock(int[] ords, int from, int count, int[] ordToDoc) throws IOException {
    long[] offsets = scratch.codeOffsets = ArrayUtil.growNoCopy(scratch.codeOffsets, count);
    MemorySegment source = fineCodeIO.read(field, ords, from, count, offsets, this::codes, scratch);
    score(ords, from, count, source, offsets, ordToDoc);
  }

  /** Scores {@code count} codes, code {@code i} at {@code offsets[i]} of {@code source}. */
  private void score(
      int[] ords, int from, int count, MemorySegment source, long[] offsets, int[] ordToDoc)
      throws IOException {
    float[] scores = scratch.scores = ArrayUtil.grow(scratch.scores, count);
    fineQuery.score(source, offsets, count, scores);
    for (int i = 0; i < count; i++) {
      int ord = ords[from + i];
      int doc = ordToDoc == null ? ord : ordToDoc[ord];
      collector.collect(doc, scores[i]);
    }
  }

  private RandomAccessInput codes() throws IOException {
    if (codes == null) {
      codes = field.section(SECTION_FINE);
    }
    return codes;
  }
}
