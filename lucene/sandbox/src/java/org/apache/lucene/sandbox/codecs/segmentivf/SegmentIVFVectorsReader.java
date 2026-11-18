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

import static org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.DATA_CODEC_NAME;
import static org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.DATA_EXTENSION;
import static org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.META_CODEC_NAME;
import static org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.META_EXTENSION;
import static org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.VERSION_CURRENT;
import static org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.VERSION_START;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.Float16VectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.FieldMetadata;
import org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.SearchStrategy;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.FileDataHint;
import org.apache.lucene.store.FileTypeHint;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.StringHelper;

/**
 * Reads a segment's SegmentIVF fields: opens the files, and hands each search to a {@link
 * SegmentSearch} of the field.
 *
 * <p><b>Files.</b> A segment has two files, both written by {@link SegmentIVFVectorsWriter}:
 *
 * <ul>
 *   <li>The <b>metadata</b> file ({@code .ivfm}), read whole and checksummed when the segment
 *       opens. It holds one {@link FieldMetadata} entry per field, ending with field number -1.
 *   <li>The <b>data</b> file ({@code .ivfd}), kept open for the life of the reader. It holds each
 *       field's sections back to back, read in place by {@link IVFFieldReader}. Only its header and
 *       footer are checked at open; {@link #checkIntegrity} checksums it in full.
 * </ul>
 *
 * <p><b>Fields.</b> Each field becomes an {@link IVFFieldReader} at open, holding only its
 * metadata. {@link #field} opens its sections on first use, so a segment that is never searched
 * reads no data.
 *
 * <p><b>Two ways to search.</b>
 *
 * <ul>
 *   <li>{@link #search(String, float[], KnnCollector, AcceptDocs)}, Lucene's per-segment entry
 *       point, scans this segment's cells and fine-reranks its own shortlist, all in one {@link
 *       SegmentSearch}. Plain {@link org.apache.lucene.search.KnnFloatVectorQuery} searches come
 *       here.
 *   <li>{@link SegmentIVFKnnQuery} splits a search in two so it reranks only the index-wide best:
 *       it asks every segment for {@link #candidates} (a coarse scan, no rerank), keeps the nearest
 *       across segments, and hands each segment its share back to {@link #rerank}. The {@link
 *       Candidates} carry the segment's search between the two calls, so the rerank reuses its
 *       prepared query.
 * </ul>
 *
 * <p>Outside merges, the data file is opened for random access: searches read cells and fine codes
 * scattered across it, and prefetch the cells they scan.
 */
final class SegmentIVFVectorsReader extends KnnVectorsReader {
  /**
   * A segment's deduplicated coarse shortlist: ordinals and their coarse distances, and the search
   * that gathered them, whose prepared query the rerank reuses.
   */
  record Candidates(int[] ords, int[] distances, SegmentSearch search) {
    static final Candidates EMPTY = new Candidates(new int[0], new int[0], null);
  }

  /** Every field of the segment by name, opened or not; fields without vectors are included. */
  private final Map<String, IVFFieldReader> fields = new HashMap<>();

  /** The data file, shared by all fields; each search reads through its own slices of it. */
  final IndexInput data;

  final int maxDoc;

  /**
   * Scopes the native views {@link IVFFieldReader} makes of mapped sections; closed with the
   * reader, so no view outlives the mapping it points into.
   */
  final Arena scope = Arena.ofShared();

  /** How reranks read fine codes: io_uring off disk where available, else the mapping. */
  private final FineCodeIO fineCodeIO;

  private final String segment;
  private final String segmentId;
  private volatile boolean closed;

  // Core: Lucene99FlatVectorsReader#readMetadata/openDataInput do this open, but are private.
  SegmentIVFVectorsReader(SegmentReadState state) throws IOException {
    segment = state.segmentInfo.name;
    segmentId = StringHelper.idToString(state.segmentInfo.getId());
    maxDoc = state.segmentInfo.maxDoc();
    String suffix = state.segmentSuffix;
    byte[] id = state.segmentInfo.getId();
    // A reader that fails to construct is never closed, and a shared arena is not freed by GC.
    try {
      String metaName = IndexFileNames.segmentFileName(segment, suffix, META_EXTENSION);
      // The metadata is small, so it is read whole and checksummed now. checkFooter reports a
      // checksum mismatch alongside whatever went wrong while reading (prior), so corruption is not
      // hidden behind the exception it caused.
      try (ChecksumIndexInput meta = state.directory.openChecksumInput(metaName)) {
        Throwable prior = null;
        try {
          CodecUtil.checkIndexHeader(
              meta, META_CODEC_NAME, VERSION_START, VERSION_CURRENT, id, suffix);
          // One entry per field, each starting with its field number; -1 ends the file.
          for (int number = meta.readInt(); number != -1; number = meta.readInt()) {
            FieldInfo info = state.fieldInfos.fieldInfo(number);
            if (info == null) {
              throw new CorruptIndexException("invalid field number " + number, meta);
            }
            fields.put(
                info.name, new IVFFieldReader(this, FieldMetadata.read(meta, info, maxDoc), info));
          }
        } catch (Throwable t) {
          prior = t;
        } finally {
          CodecUtil.checkFooter(meta, prior);
        }
      }
      String dataName = IndexFileNames.segmentFileName(segment, suffix, DATA_EXTENSION);
      IOContext context = state.context;
      // A merge reads the data sequentially, so only a searching reader hints random access.
      if (context.context() != IOContext.Context.MERGE) {
        context =
            context.withHints(FileTypeHint.DATA, FileDataHint.KNN_VECTORS, DataAccessHint.RANDOM);
      }
      IndexInput in = null;
      try {
        in = state.directory.openInput(dataName, context);
        // The data file can be gigabytes, so opening checks only its header and that its footer is
        // well formed; checkIntegrity verifies the checksum over the whole file.
        CodecUtil.checkIndexHeader(in, DATA_CODEC_NAME, VERSION_START, VERSION_CURRENT, id, suffix);
        CodecUtil.retrieveChecksum(in);
        long dataEnd = in.length() - CodecUtil.footerLength();
        for (IVFFieldReader field : fields.values()) {
          field.checkSections(dataEnd, in);
        }
        fineCodeIO = new FineCodeIO(state, dataName, fields.values(), in);
      } catch (Throwable t) {
        IOUtils.closeWhileSuppressingExceptions(t, in);
        throw t;
      }
      data = in;
    } catch (Throwable t) {
      IOUtils.closeWhileSuppressingExceptions(t, scope::close);
      throw t;
    }
  }

  /**
   * Searches this segment alone: a coarse scan and a fine rerank of its own shortlist into {@code
   * collector}. The probe count and oversample come from the collector's {@link SearchStrategy}
   * when it carries one, else the field's default.
   */
  @Override
  public void search(String name, float[] target, KnnCollector collector, AcceptDocs acceptDocs)
      throws IOException {
    IVFFieldReader field = field(name);
    if (field != null) {
      new SegmentSearch(field, fineCodeIO, target, collector, collector.k(), null).run(acceptDocs);
    }
  }

  /**
   * Runs this segment's coarse search for {@code strategy}, filtered by {@code accept} as bits, and
   * returns its deduplicated shortlist for a cross-segment rerank.
   */
  Candidates candidates(
      String name, float[] target, int k, SearchStrategy strategy, AcceptDocs accept)
      throws IOException {
    IVFFieldReader field = field(name);
    if (field == null) {
      return Candidates.EMPTY;
    }
    SegmentSearch search = new SegmentSearch(field, fineCodeIO, target, null, k, strategy);
    search.run(accept);
    return search.gathered == null ? Candidates.EMPTY : search.gathered;
  }

  /**
   * Fine-reranks {@code ords}, the part of {@code from}'s shortlist that survived the index-wide
   * cut, into {@code collector}. The second half of a {@link SegmentIVFKnnQuery} search, after
   * {@link #candidates}.
   */
  void rerank(Candidates from, int[] ords, KnnCollector collector) throws IOException {
    ensureOpen();
    from.search().rerankInto(ords, collector);
  }

  /**
   * {@link #rerank} in two halves, for a cross-segment rerank that reads every segment's share in
   * one batch ({@link FineCodeIO#readAll}): this plans {@code ords}'s reads, or returns null when
   * they are read mapped.
   */
  FineCodeIO.Plan planRerank(Candidates from, int[] ords) {
    return from.search().planRerank(ords);
  }

  /** The second half of {@link #planRerank}: scores {@code ords} from the batch's buffer. */
  void rerank(
      Candidates from,
      int[] ords,
      FineCodeIO.Plan plan,
      MemorySegment source,
      KnnCollector collector)
      throws IOException {
    ensureOpen();
    from.search().rerankInto(ords, plan, source, collector);
  }

  /**
   * The SegmentIVF reader of {@code field} in {@code leaf}, or null when another format reads it.
   * Leaf wrappers are unwrapped; a {@link CodecReader} wrapper is not, but its vector reader is
   * asked instead, so a wrapper that remaps documents, like {@code SortingCodecReader}, exposes its
   * own vector reader and is searched as another format.
   */
  static SegmentIVFVectorsReader of(LeafReader leaf, String field) {
    LeafReader reader = FilterLeafReader.unwrap(leaf);
    if (reader instanceof CodecReader codecReader
        && codecReader.getVectorReader() instanceof KnnVectorsReader vectors
        && unwrapReaderForField(vectors, field) instanceof SegmentIVFVectorsReader sivf) {
      return sivf;
    }
    return null;
  }

  /** The reader behind a per-field wrapper, or {@code reader} itself. */
  static KnnVectorsReader unwrapReaderForField(KnnVectorsReader reader, String field) {
    return reader.unwrapReaderForField(field);
  }

  /**
   * The opened field, or null when it is absent or has no vectors. The first call opens its
   * sections ({@link IVFFieldReader#open}); later calls return it as it is.
   */
  IVFFieldReader field(String name) throws IOException {
    ensureOpen();
    IVFFieldReader field = fields.get(name);
    if (field == null || field.count == 0) {
      return null;
    }
    return field.open();
  }

  /** Whether reranks read fine codes off disk with io_uring rather than through the mapping. */
  boolean readsFineDirectly() {
    return fineCodeIO.readsDirectly();
  }

  /** This segment's fine-code reads, for tests. */
  FineCodeIO fineCodeIO() {
    return fineCodeIO;
  }

  /** The segment's name, which keys its remembered clustering ({@link Clustering.HotStart}). */
  String segment() {
    return segment;
  }

  /** The segment's id ({@link StringHelper#idToString}), which identifies it across indexes. */
  String segmentId() {
    return segmentId;
  }

  @Override
  public FloatVectorValues getFloatVectorValues(String name) throws IOException {
    ensureOpen();
    IVFFieldReader field = fields.get(name);
    if (field == null || field.encoding != VectorEncoding.FLOAT32) {
      return null;
    }
    return field.open().values();
  }

  /**
   * Empty values for a byte field, which SegmentIVF rejects (it is written without vectors), so a
   * caller that finds vectors in the field's {@link FieldInfo} gets values, as it expects; null for
   * any other field.
   */
  @Override
  public ByteVectorValues getByteVectorValues(String name) {
    IVFFieldReader field = fields.get(name);
    if (field == null || field.encoding != VectorEncoding.BYTE) {
      return null;
    }
    return ByteVectorValues.fromBytes(List.of(), field.dim);
  }

  /** Empty values for a float16 field, as {@link #getByteVectorValues} for a byte field. */
  @Override
  public Float16VectorValues getFloat16VectorValues(String name) {
    IVFFieldReader field = fields.get(name);
    if (field == null || field.encoding != VectorEncoding.FLOAT16) {
      return null;
    }
    return Float16VectorValues.fromFloats16(List.of(), field.dim);
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
    CodecUtil.checksumEntireFile(data, merge);
  }

  @Override
  public synchronized void close() throws IOException {
    // A second close must not close a reused file descriptor.
    if (closed) {
      return;
    }
    closed = true;
    // The scope first: it invalidates the rebased views before the mapping goes away. If it could
    // not close, a view may still be in use, so the mapping is left for GC rather than unmapped
    // under it.
    try {
      closeScope();
    } catch (Throwable t) {
      IOUtils.closeWhileSuppressingExceptions(t, fineCodeIO);
      throw t;
    }
    IOUtils.close(data, fineCodeIO);
  }

  /**
   * Closes the scope. A shared arena refuses to close while a thread holds it in a native call; as
   * core's MemorySegmentIndexInput#close does, the close is retried until that call returns. Views
   * are read by Java code only, so it does not happen today.
   */
  private void closeScope() {
    while (true) {
      try {
        scope.close();
        return;
      } catch (IllegalStateException _) {
        if (scope.scope().isAlive() == false) {
          return;
        }
        Thread.onSpinWait();
      }
    }
  }

  /** Throws AlreadyClosedException once closed, as Lucene's readers do. */
  private void ensureOpen() {
    if (closed) {
      throw new AlreadyClosedException("this SegmentIVF reader is closed");
    }
  }
}
