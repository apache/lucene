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

import static org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.SECTION_CELL_OFFSETS;
import static org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.SECTION_CENTROIDS;
import static org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.SECTION_COARSE;
import static org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.SECTION_FINE;
import static org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.SECTION_GRAPH;
import static org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.SECTION_ORD_TO_SLOT;
import static org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.SECTION_SLOT_DOCS;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.CoarseTier;
import org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.FieldMetadata;
import org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.FineTier;
import org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsWriter.HadamardRotation;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.packed.DirectMonotonicReader;

/**
 * The read side of one vector field in one segment: what {@link SegmentIVFVectorsReader} keeps per
 * field, and the only place a field's data sections ({@link SegmentIVFVectorsFormat}) are read.
 * Searches, reranks and {@link IVFVectorValues} go through it.
 *
 * <p>It loads in three stages, so opening a segment costs nothing per vector:
 *
 * <ol>
 *   <li><b>When the segment opens</b>, the constructor keeps the field's {@link FieldMetadata},
 *       parsed by {@link FieldMetadata#read}: tiers, dimension, {@code nlist}, counts and where
 *       each data section starts. It builds the field's codecs and rotation, and reads no data.
 *   <li><b>On first search</b>, {@link #open} reads the per-cell structures onto the heap (the
 *       cells' slot ranges from {@link SegmentIVFVectorsFormat#SECTION_CELL_OFFSETS}, the
 *       centroids, where each cell's coarse codes start, and the coarse tier's parameters) and maps
 *       the coarse codes, slot documents and fine codes.
 *   <li><b>On demand</b>, it loads the ordinal-to-slot and ordinal-to-document maps ({@link
 *       #ordToSlot}, {@link #ordToDoc}) and the centroid codes and graph ({@link #codes}).
 * </ol>
 *
 * <p>It also resolves the codec's addressing: an ordinal's primary slot, a slot's cell and
 * document, and a cell's region of coarse codes.
 *
 * <p>Sections are read in place from the mapped data file when the directory maps it, and through
 * {@link RandomAccessInput} slices otherwise. Reranks read fine codes off disk with io_uring, and
 * through the mapping where io_uring is unavailable ({@link FineCodeIO}).
 */
final class IVFFieldReader {
  private static final ValueLayout.OfInt INT_LE =
      ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

  private final SegmentIVFVectorsReader reader;

  final VectorSimilarityFunction similarity;
  final FineTier fineTier;
  final CoarseTier coarseTier;
  final int dim;
  final int nlist;
  final int count;
  final int nprobe;
  final int spillBits;
  final HadamardRotation rotation;
  final FineCodec fine;
  final CoarseCodec coarseCodec;

  /** {@code coarseCodec.codeBytes}. */
  final int coarseBytes;

  /** Where each data section starts; a section ends where the next starts. */
  private final long[] sections;

  private final DirectMonotonicReader.Meta cellOffsetsMeta;

  // Opened by open(): each section mapped as one segment when possible, else null.
  private boolean opened;
  MemorySegment fineSegment;
  private MemorySegmentAccessInput coarseAccess;
  private MemorySegment coarseSegment;
  private MemorySegment slotDocsSegment;
  private RandomAccessInput slotDocs;

  /** The first slot of each cell, plus the slot count at {@code nlist}. */
  int[] cellStart;

  float[][] centroids;

  /** The field's {@link CoarseCodec#parameters}; empty for a tier that keeps none. */
  float[] coarseParameters;

  /** Where each cell's region of coarse codes starts in the coarse section, and the end. */
  long[] coarseRegions;

  // Loaded on demand.
  private int[] ordToSlot;
  private int[] ordToDoc;
  int[] allCells;
  private volatile CentroidCodes codes;
  CentroidGraph graph;

  /**
   * The field's encoding. Only FLOAT32 fields hold vectors; a field of another encoding, which
   * SegmentIVF rejects, is written empty.
   */
  final VectorEncoding encoding;

  IVFFieldReader(SegmentIVFVectorsReader reader, FieldMetadata metadata, FieldInfo info) {
    this.reader = reader;
    similarity = info.getVectorSimilarityFunction();
    encoding = info.getVectorEncoding();
    fineTier = metadata.fineTier();
    coarseTier = metadata.coarseTier();
    dim = metadata.dim();
    nlist = metadata.nlist();
    count = metadata.count();
    nprobe = metadata.nprobe();
    spillBits = metadata.spillBits();
    sections = metadata.sections();
    cellOffsetsMeta = metadata.cellOffsets();
    rotation = HadamardRotation.forDimension(dim);
    fine = FineCodec.of(fineTier, dim);
    coarseCodec = CoarseCodec.of(coarseTier, dim);
    coarseBytes = coarseCodec.codeBytes;
  }

  /**
   * Checks this field's sections against the data file, which ends at {@code dataEnd} before its
   * footer, and against what the field's metadata implies of them.
   */
  void checkSections(long dataEnd, IndexInput data) throws CorruptIndexException {
    if (sections[SECTION_CELL_OFFSETS] > dataEnd) {
      throw new CorruptIndexException("sections past the end of the data", data);
    }
    long fine = (long) count * this.fine.recordBytes;
    if (sectionLength(SECTION_FINE) != fine
        || sectionLength(SECTION_ORD_TO_SLOT) != (long) count * Integer.BYTES
        || sectionLength(SECTION_CENTROIDS) < (long) nlist * dim * Float.BYTES) {
      throw new CorruptIndexException(
          "section lengths do not match " + count + " vectors in " + nlist + " cells", data);
    }
  }

  /** The bytes of section {@code s}. */
  long sectionLength(int s) {
    return sections[s + 1] - sections[s];
  }

  /** Where section {@code s} starts in the data file. */
  long sectionStart(int s) {
    return sections[s];
  }

  /**
   * A slice of section {@code s}. Searches take their own slices: positional reads through a shared
   * slice are not thread-safe on every directory implementation.
   */
  RandomAccessInput section(int s) throws IOException {
    return reader.data.randomAccessSlice(sections[s], sectionLength(s));
  }

  /** Opens the sections searches need, once. */
  synchronized IVFFieldReader open() throws IOException {
    if (opened) {
      return this;
    }
    if (section(SECTION_COARSE) instanceof MemorySegmentAccessInput in) {
      coarseAccess = in;
      coarseSegment = mappedOrNull(in, 0, in.length());
    }
    slotDocs = section(SECTION_SLOT_DOCS);
    slotDocsSegment = mappedOrNull(slotDocs);
    fineSegment = mappedOrNull(section(SECTION_FINE));
    cellStart = new int[nlist + 1];
    if (nlist > 0) {
      long start = sections[SECTION_CELL_OFFSETS];
      RandomAccessInput tail = reader.data.randomAccessSlice(start, reader.data.length() - start);
      DirectMonotonicReader offsets = DirectMonotonicReader.getInstance(cellOffsetsMeta, tail);
      for (int c = 0; c <= nlist; c++) {
        long offset = offsets.get(c);
        if (offset % Integer.BYTES != 0
            || offset > sectionLength(SECTION_SLOT_DOCS)
            || (c == 0 ? offset != 0 : offset < (long) cellStart[c - 1] * Integer.BYTES)) {
          throw new CorruptIndexException("cell " + c + " starts at " + offset, reader.data);
        }
        cellStart[c] = (int) (offset / Integer.BYTES);
      }
      long slots = cellStart[nlist];
      if (slots < count
          || slots > (long) count * (1 + spillBits)
          || sectionLength(SECTION_SLOT_DOCS) != slots * Integer.BYTES) {
        throw new CorruptIndexException(
            slots + " slots for " + count + " vectors, spillBits " + spillBits, reader.data);
      }
    }
    IndexInput in = reader.data.clone();
    centroids = new float[nlist][dim];
    in.seek(sections[SECTION_CENTROIDS]);
    for (float[] centroid : centroids) {
      in.readFloats(centroid, 0, dim);
    }
    // Each cell's codes are a region of whole blocks; regions follow one another.
    coarseRegions = new long[nlist + 1];
    for (int c = 0; c < nlist; c++) {
      coarseRegions[c + 1] =
          coarseRegions[c] + coarseCodec.regionBytes(cellStart[c + 1] - cellStart[c]);
    }
    if (nlist > 0
        && sectionLength(SECTION_COARSE)
            != coarseRegions[nlist] + (long) coarseCodec.parameters() * Float.BYTES) {
      throw new CorruptIndexException("coarse section length", reader.data);
    }
    coarseParameters = new float[coarseCodec.parameters()];
    if (coarseParameters.length > 0 && nlist > 0) {
      // The coarse tier's parameters follow its codes; an empty field writes neither.
      in.seek(sections[SECTION_COARSE] + coarseRegions[nlist]);
      in.readFloats(coarseParameters, 0, coarseParameters.length);
    }
    opened = true;
    return this;
  }

  private MemorySegment mappedOrNull(RandomAccessInput section) throws IOException {
    if (section instanceof MemorySegmentAccessInput in) {
      return mappedOrNull(in, 0, in.length());
    }
    return null;
  }

  /**
   * Returns a mapped slice, or null when no single mapping covers it. The slice is rebased as a
   * plain native segment scoped to the reader, so the SIMD kernels see one segment type whether
   * they read mapped codes or io_uring buffers: once they have seen two, every scan runs about 16%
   * slower.
   */
  @SuppressWarnings("restricted")
  private MemorySegment mappedOrNull(MemorySegmentAccessInput in, long offset, long length) {
    if (length == 0) {
      return null;
    }
    try {
      MemorySegment mapped = in.segmentSliceOrNull(offset, length);
      // Only a native segment's address is a pointer: a directory may hand back a heap segment,
      // whose address is an array offset.
      if (mapped == null || mapped.isNative() == false) {
        return null;
      }
      // Read-only, as the mapping is: a stray write then throws instead of faulting.
      return MemorySegment.ofAddress(mapped.address())
          .reinterpret(length, reader.scope, null)
          .asReadOnly();
    } catch (IOException _) {
      return null;
    }
  }

  /**
   * The memory holding the coarse codes of {@code cell}: the whole mapped section, or a mapped
   * slice of the cell's region, or null when neither is mapped. The region starts at {@link
   * #coarseRegionBase} in it.
   */
  MemorySegment coarseRegion(int cell) {
    if (coarseSegment != null || coarseAccess == null) {
      return coarseSegment;
    }
    long start = coarseRegions[cell];
    return mappedOrNull(coarseAccess, start, coarseRegions[cell + 1] - start);
  }

  /** The offset of {@code cell}'s region in the memory {@link #coarseRegion} returns. */
  long coarseRegionBase(int cell) {
    return coarseSegment == null ? 0 : coarseRegions[cell];
  }

  // ---- Ordinals, slots and documents.

  /**
   * The document in {@code slot}, read from the mapped section, or through {@code docs}, a slice of
   * it, when unmapped.
   */
  int docAt(RandomAccessInput docs, int slot) throws IOException {
    if (slotDocsSegment != null) {
      return slotDocsSegment.getAtIndex(INT_LE, slot);
    }
    return docs.readInt((long) slot * Integer.BYTES);
  }

  /** Each ordinal's primary slot. */
  synchronized int[] ordToSlot() throws IOException {
    if (ordToSlot == null) {
      open(); // for the slot count the slots are checked against
      int[] slots = new int[count];
      IndexInput in = reader.data.clone();
      in.seek(sections[SECTION_ORD_TO_SLOT]);
      in.readInts(slots, 0, count);
      int slotCount = cellStart[nlist];
      for (int slot : slots) {
        if (slot < 0 || slot >= slotCount) {
          throw new CorruptIndexException("slot " + slot + " of " + slotCount, reader.data);
        }
      }
      ordToSlot = slots;
    }
    return ordToSlot;
  }

  /** Each ordinal's document, read through its primary slot. */
  synchronized int[] ordToDoc() throws IOException {
    if (ordToDoc == null) {
      int[] slots = ordToSlot();
      int[] docs = new int[count];
      for (int ord = 0; ord < count; ord++) {
        docs[ord] = docAt(slotDocs, slots[ord]);
        // Searches binary-search this array, so it must be strictly increasing.
        if (docs[ord] >= reader.maxDoc || (ord == 0 ? docs[ord] < 0 : docs[ord] <= docs[ord - 1])) {
          throw new CorruptIndexException("ordinal " + ord + " has doc " + docs[ord], reader.data);
        }
      }
      ordToDoc = docs;
    }
    return ordToDoc;
  }

  /** Whether every document of the segment has a vector, so ordinals are doc ids. */
  boolean dense() {
    return count == reader.maxDoc;
  }

  /** {@link #ordToDoc} for a sparse field, or null for a dense one. */
  int[] ordToDocIfSparse() throws IOException {
    return dense() ? null : ordToDoc();
  }

  /**
   * Ordinal {@code ord}'s primary cell: the cell whose run of slots holds its primary slot. Empty
   * cells start where their successor does, so the last cell starting at or before the slot is
   * never empty.
   */
  int primaryCell(int ord) throws IOException {
    return cellOfSlot(ordToSlot()[ord]);
  }

  /** The cell holding {@code slot}. */
  int cellOfSlot(int slot) {
    return SegmentIVFVectorsWriter.cellOfSlot(cellStart, slot);
  }

  // ---- Centroids.

  /** The centroid codes and graph, loaded on first search. */
  CentroidCodes codes() throws IOException {
    CentroidCodes loaded = codes;
    if (loaded == null) {
      loaded = loadCodes();
    }
    return loaded;
  }

  private synchronized CentroidCodes loadCodes() throws IOException {
    if (codes == null) {
      allCells = new int[nlist];
      for (int c = 0; c < nlist; c++) {
        allCells[c] = c;
      }
      long graphLength = sectionLength(SECTION_GRAPH);
      if (graphLength > 0) {
        graph = CentroidGraph.read(section(SECTION_GRAPH), coarseCodec, graphLength, nlist);
      }
      codes = new CentroidCodes(centroids, coarseCodec);
    }
    return codes;
  }

  /** A new view of this field's vectors. */
  IVFVectorValues values() {
    return new IVFVectorValues(this);
  }
}
