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

import static org.apache.lucene.sandbox.codecs.ivfaster_evo.IVFasterEvoVectorsFormat.CODEC_NAME;
import static org.apache.lucene.sandbox.codecs.ivfaster_evo.IVFasterEvoVectorsFormat.EXTENSION;
import static org.apache.lucene.sandbox.codecs.ivfaster_evo.IVFasterEvoVectorsFormat.VERSION;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.IntPredicate;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Float16VectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.sandbox.codecs.ivfaster_evo.IVFasterEvoVectorsFormat.Tier;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.FileDataHint;
import org.apache.lucene.store.FileTypeHint;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;

final class IVFasterEvoVectorsReader extends KnnVectorsReader {
  private record Field(
      float[][] centroids,
      byte[][] centroidCodes,
      TieredVectors vectors,
      int[][] graph,
      int[][] postings,
      int[] cellStarts,
      int[] primary,
      int numProbes) {}

  /** Coarse candidates retained per segment for fine reranking (or k, if larger). */
  private static final int SHORTLIST = 700;

  /** Graph beam width, as a multiple of the probe count, ranked exactly to choose the cells. */
  private static final int RESCORE_MULTIPLIER = 2;

  /** A filter of selectivity s multiplies the probe count by 1/s, up to this factor. */
  private static final int FILTER_PROBE_CAP = 8;

  /** Rows per bulk coarse scan. */
  private static final int BLOCK = 256;

  private final IndexInput data;
  private final IndexInput fineData; // the same file, opened for random access
  private final Map<String, Field> fields = new HashMap<>();
  private final int maxDoc;

  IVFasterEvoVectorsReader(SegmentReadState state) throws IOException {
    maxDoc = state.segmentInfo.maxDoc();
    byte[] id = state.segmentInfo.getId();
    String suffix = state.segmentSuffix;
    String dataFile = IndexFileNames.segmentFileName(state.segmentInfo.name, suffix, "ivd");
    data = state.directory.openInput(dataFile, IOContext.DEFAULT);
    IndexInput random = null;
    try {
      CodecUtil.checkIndexHeader(data, CODEC_NAME + "Data", VERSION, VERSION, id, suffix);
      CodecUtil.retrieveChecksum(data);
      // Fine records are fetched from scattered positions: see TieredVectors#input.
      random =
          state.directory.openInput(
              dataFile,
              IOContext.DEFAULT.withHints(
                  FileTypeHint.DATA, FileDataHint.KNN_VECTORS, DataAccessHint.RANDOM));
    } catch (Throwable t) {
      IOUtils.closeWhileSuppressingExceptions(t, data, random);
      throw t;
    }
    fineData = random;
    long dataEnd = data.length() - CodecUtil.footerLength();
    String file = IndexFileNames.segmentFileName(state.segmentInfo.name, suffix, EXTENSION);
    try (ChecksumIndexInput in = state.directory.openChecksumInput(file)) {
      Throwable prior = null;
      try {
        CodecUtil.checkIndexHeader(in, CODEC_NAME, VERSION, VERSION, id, suffix);
        for (int number = in.readInt(); number != -1; number = in.readInt()) {
          FieldInfo info = state.fieldInfos.fieldInfo(number);
          if (info == null
              || info.getVectorEncoding() != VectorEncoding.FLOAT32
              || info.getVectorDimension() == 0
              || fields.containsKey(info.name)) {
            throw new CorruptIndexException("invalid vector field: " + number, in);
          }
          int dim = info.getVectorDimension();
          int coarseId = in.readByte(), fineId = in.readByte();
          if (coarseId != Tier.NITROX2.ordinal() || fineId < 1 || fineId >= Tier.values().length) {
            throw new CorruptIndexException("invalid tier", in);
          }
          TierCodec coarse = new TierCodec(Tier.NITROX2, dim);
          TierCodec fine = new TierCodec(Tier.values()[fineId], dim);
          long offset = in.readVLong();
          int slotCount = in.readVInt();
          int[] docs = readInts(in, maxDoc, true);
          int count = docs.length;
          if (slotCount < count
              || offset < CodecUtil.indexHeaderLength(CODEC_NAME + "Data", suffix)
              || offset > dataEnd - (long) slotCount * (coarse.bytes + fine.bytes)) {
            throw new CorruptIndexException("invalid vector data offset", in);
          }
          int probes = in.readVInt();
          int k = in.readVInt();
          if (probes < 1
              || k < 0
              || (count == 0) != (k == 0)
              || (long) k * (dim * (long) Float.BYTES + 2) > in.length() - in.getFilePointer()) {
            throw new CorruptIndexException("invalid centroid/probe count", in);
          }
          int[] primary = new int[count];
          for (int ord = 0; ord < count; ord++) {
            primary[ord] = in.readVInt();
            if (primary[ord] >= k) throw new CorruptIndexException("invalid primary cell", in);
          }
          float[][] centroids = new float[k][dim];
          byte[][] codes = new byte[k][coarse.bytes];
          int[][] graph = new int[k][];
          int[][] postings = new int[k][];
          FixedBitSet seen = new FixedBitSet(count);
          int[] cellStarts = new int[k + 1];
          int[] primarySlots = new int[count];
          for (int cell = 0; cell < k; cell++) {
            for (int d = 0; d < dim; d++) {
              float value = Float.intBitsToFloat(in.readInt());
              if (Float.isFinite(value) == false)
                throw new CorruptIndexException("non-finite centroid", in);
              centroids[cell][d] = value;
            }
            in.readBytes(codes[cell], 0, coarse.bytes);
            graph[cell] = readInts(in, k, false);
            postings[cell] = readInts(in, count, true);
            long end = (long) cellStarts[cell] + postings[cell].length;
            if (end > slotCount) throw new CorruptIndexException("invalid slot count", in);
            cellStarts[cell + 1] = (int) end;
            for (int row = 0; row < postings[cell].length; row++) {
              int ord = postings[cell][row];
              if (primary[ord] == cell) {
                seen.set(ord);
                primarySlots[ord] = cellStarts[cell] + row;
              }
            }
          }
          if (seen.cardinality() != count || cellStarts[k] != slotCount) {
            throw new CorruptIndexException("missing vector ordinals", in);
          }
          VectorSimilarityFunction sim = info.getVectorSimilarityFunction();
          TieredVectors vectors =
              new TieredVectors(
                  coarse,
                  fine,
                  sim,
                  docs,
                  offset,
                  slotCount,
                  primarySlots,
                  data.clone(),
                  random.clone());
          fields.put(
              info.name,
              new Field(centroids, codes, vectors, graph, postings, cellStarts, primary, probes));
        }
      } catch (Throwable t) {
        prior = t;
      } finally {
        CodecUtil.checkFooter(in, prior);
      }
    } catch (Throwable t) {
      IOUtils.closeWhileSuppressingExceptions(t, data, fineData);
      throw t;
    }
  }

  private static int[] readInts(ChecksumIndexInput in, int limit, boolean sorted)
      throws IOException {
    int length = in.readVInt();
    if (length < 0 || length > limit) throw new CorruptIndexException("invalid list length", in);
    int[] result = new int[length];
    for (int i = 0; i < length; i++) {
      result[i] = in.readVInt();
      if (result[i] < 0 || result[i] >= limit || (sorted && i > 0 && result[i] <= result[i - 1])) {
        throw new CorruptIndexException("invalid list entry", in);
      }
    }
    return result;
  }

  float[][] centroids(String field) {
    return fields.get(field).centroids;
  }

  int[] assignments(String field) throws IOException {
    return fields.get(field).primary.clone();
  }

  @Override
  public void search(String field, float[] target, KnnCollector collector, AcceptDocs acceptDocs)
      throws IOException {
    Field data = fields.get(field);
    TieredVectors values = data.vectors.copy();
    if (target.length != values.dimension()) {
      throw new IllegalArgumentException("query vector dimension differs from field dimension");
    }
    if (values.size() == 0 || collector.k() == 0 || collector.earlyTerminated()) return;
    int requested =
        collector.getSearchStrategy() instanceof IVFasterEvoVectorsFormat.SearchStrategy strategy
            ? strategy.numProbes
            : data.numProbes;
    // Nitrox2 normalizes internally, so one query code serves graph descent and cell scans.
    byte[] query = values.coarse.encode(target);
    int probes = Math.min(requested, data.centroids.length);
    // The filter is applied before scoring, so masked-out rows cost nothing and no visit budget.
    Bits accept = acceptDocs == null ? null : acceptDocs.bits();
    if (accept != null) {
      // Probe more cells under a filter, so that about as many ADMISSIBLE rows are scanned as an
      // unfiltered query would see. cost() is the filter's exact popcount, which the query has
      // already computed; a live-docs-only AcceptDocs reports at least size(), hence no scaling.
      double selectivity = Math.min(1, acceptDocs.cost() / (double) values.size());
      double scaled = Math.ceil(probes / Math.max(selectivity, 1.0 / FILTER_PROBE_CAP));
      probes = (int) Math.min(data.centroids.length, scaled);
    }
    // A 2-bit sketch ranks a thousand centroids noisily, and a wrongly chosen cell costs recall no
    // scan can recover. So the graph only nominates cells: rank everything it visited by exact
    // distance to the float centroids, in the metric clustering assigned documents with.
    float[] routingTarget = target;
    if (values.similarity == VectorSimilarityFunction.COSINE) {
      routingTarget = target.clone();
      Clustering.normalize(routingTarget);
    }
    IntPredicate nonEmpty = cell -> data.postings[cell].length > 0;
    int[] visited =
        CentroidGraph.search(
            data.centroidCodes, data.graph, query, RESCORE_MULTIPLIER * probes, nonEmpty);
    long[] ranked = CentroidGraph.rank(data.centroids, routingTarget, visited);
    int[] cells = new int[Math.min(probes, ranked.length)];
    for (int i = 0; i < cells.length; i++) cells[i] = (int) ranked[i];
    // Cells are contiguous, so every byte range this query will scan is known before the scan
    // starts. Hint them all now: an index that does not fit in memory then streams its cells with
    // overlapping reads, rather than one synchronous page fault after another.
    for (int cell : cells) values.prefetch(data.cellStarts[cell], data.postings[cell].length, true);
    // The histogram keeps the exact best coarse candidates without a heap operation per row.
    HammingCandidates shortlist =
        new HammingCandidates(Math.max(SHORTLIST, collector.k()), values.coarse.bytes * 8);
    // Spill copies share a code, hence a distance, and the threshold only tightens: every copy
    // gets the same verdict. So an unfiltered scan resolves the document and deduplicates only
    // for the few rows that pass the threshold, never for the tens of thousands that do not. A
    // filtered scan has to resolve every row anyway, so it deduplicates at admission instead,
    // which keeps its visited count within the filter's cardinality (the query's visit limit).
    FixedBitSet seen = new FixedBitSet(maxDoc);
    byte[] block = new byte[BLOCK * values.coarse.bytes];
    boolean[] selected = new boolean[BLOCK];
    Arrays.fill(selected, true);
    int[] distances = new int[BLOCK];
    scan:
    for (int cell : cells) {
      int[] posting = data.postings[cell];
      for (int start = 0; start < posting.length; start += BLOCK) {
        // Collectors may also terminate for their own reasons, such as a timeout.
        if (collector.earlyTerminated()) break scan;
        long budget = collector.visitLimit() - collector.visitedCount();
        int rows = Math.min(BLOCK, posting.length - start);
        int scored = (int) Math.min(rows, budget);
        if (accept == null) {
          rows = scored; // rows past the budget are simply not scanned
        } else {
          scored = 0;
          for (int row = 0; row < rows; row++) {
            int doc = values.ordToDoc(posting[start + row]);
            selected[row] = scored < budget && accept.get(doc) && seen.getAndSet(doc) == false;
            if (selected[row]) scored++;
          }
          if (scored == 0) continue;
        }
        int slot = data.cellStarts[cell] + start;
        values.hammingBulk(slot, rows, query, selected, block, distances);
        collector.incVisitedCount(scored);
        for (int row = 0; row < rows; row++) {
          if (selected[row] && distances[row] <= shortlist.threshold()) {
            int doc = values.ordToDoc(posting[start + row]);
            if (accept != null || seen.getAndSet(doc) == false) {
              shortlist.add(doc, slot + row, distances[row]);
            }
          }
        }
      }
    }
    var fineScorer = values.fine.scorer(target, values.similarity);
    byte[] fineCode = new byte[values.fine.bytes];
    // Likewise for the fine tier, but only for the shortlist: sort it into file order (slot in
    // the high bits), hint every record, then read them.
    long[] records = shortlist.finish();
    for (int i = 0; i < records.length; i++) records[i] = Long.rotateLeft(records[i], 32);
    Arrays.sort(records);
    for (long record : records) values.prefetch((int) (record >>> 32), 1, false);
    for (long record : records) {
      values.readSlot((int) (record >>> 32), false, fineCode);
      collector.collect((int) record, (float) fineScorer.score(fineCode));
    }
  }

  @Override
  public TieredVectors getFloatVectorValues(String field) throws IOException {
    Field data = fields.get(field);
    return data == null ? null : data.vectors.copy();
  }

  @Override
  public ByteVectorValues getByteVectorValues(String field) {
    throw new UnsupportedOperationException("IVFasterEvo supports only FLOAT32 vectors");
  }

  @Override
  public Float16VectorValues getFloat16VectorValues(String field) {
    throw new UnsupportedOperationException("IVFasterEvo supports only FLOAT32 vectors");
  }

  @Override
  public void search(String field, byte[] target, KnnCollector collector, AcceptDocs acceptDocs) {
    throw new UnsupportedOperationException("IVFasterEvo supports only FLOAT32 vectors");
  }

  @Override
  public void search(String field, short[] target, KnnCollector collector, AcceptDocs acceptDocs) {
    throw new UnsupportedOperationException("IVFasterEvo supports only FLOAT32 vectors");
  }

  @Override
  public void checkIntegrity(MergePolicy.OneMerge merge) throws IOException {
    // The .ive file is fully read and checksummed at construction.
    CodecUtil.checksumEntireFile(data);
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(data, fineData);
  }

  /** Streaming integer-distance threshold selection, with document-ID ties. */
  static final class HammingCandidates {
    private final int limit;
    private final int[] histogram;
    private int threshold, admitted, size;
    private int[] distances = new int[1024];
    private long[] records = new long[1024];

    HammingCandidates(int limit, int maxDistance) {
      this.limit = limit;
      histogram = new int[maxDistance + 1];
      threshold = maxDistance;
    }

    /** No record farther than this can still enter the shortlist. */
    int threshold() {
      return threshold;
    }

    void add(int doc, int slot, int distance) {
      if (distance > threshold) return;
      if (size == records.length) {
        // Discard entries invalidated by a tighter threshold before growing scratch.
        int kept = 0;
        for (int i = 0; i < size; i++) {
          if (distances[i] <= threshold) {
            distances[kept] = distances[i];
            records[kept++] = records[i];
          }
        }
        size = kept;
        records = ArrayUtil.grow(records, size + 1);
        distances = ArrayUtil.growExact(distances, records.length);
      }
      records[size] = ((long) doc << 32) | slot;
      distances[size++] = distance;
      histogram[distance]++;
      admitted++;
      while (threshold > 0 && admitted - histogram[threshold] >= limit) {
        admitted -= histogram[threshold--];
      }
    }

    long[] finish() {
      long[] result = new long[Math.min(limit, admitted)];
      int count = 0, tied = 0;
      // Compact only the boundary bucket into existing scratch, then break ties by doc ID.
      for (int i = 0; i < size; i++) {
        if (distances[i] < threshold) result[count++] = records[i];
        else if (distances[i] == threshold) records[tied++] = records[i];
      }
      Arrays.sort(records, 0, tied);
      System.arraycopy(records, 0, result, count, result.length - count);
      return result;
    }
  }
}
