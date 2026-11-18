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

import static org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.SECTION_FINE;

import java.io.Closeable;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IOUtils;

/**
 * How a segment's fine codes are read for a rerank: off disk by default.
 *
 * <p>A rerank reads a few hundred fine codes at random ordinals, each once per query and rarely
 * again, so they are read from disk rather than kept in memory. There are three ways to read them:
 *
 * <ol>
 *   <li><b>One io_uring batch</b>, the default: a single submission issues every read at once, so
 *       their latencies overlap ({@link IoUring}). When every field's fine section starts on a page
 *       and its codes either fill whole pages or divide a page evenly (INT8 at 1024 or 4096
 *       dimensions), the batch is read uncached (O_DIRECT): caching a fine code would only evict
 *       the doc values and postings that every query rereads. Codes smaller than a page are read as
 *       their whole page, and codes that share a page read it once.
 *   <li><b>Mapped, in place</b>, where io_uring is unavailable: off Linux, on a directory that is
 *       not a file-system directory or a compound segment, or on a kernel that refuses its reads.
 *   <li><b>Copied</b>, on a directory that does not map the file: codes are copied through a slice.
 * </ol>
 *
 * <p>The exact scorer of {@link IVFVectorValues} always reads mapped: its hits recur across
 * queries, and the page cache serves them.
 */
final class FineCodeIO implements Closeable {
  /** Test hook: when non-null, forces io_uring reads on or off. */
  static volatile Boolean forceUring;

  /** Reads issued off disk, and the codes they held, process-wide: for tests and benchmarks. */
  static final LongAdder DISK_READS = new LongAdder();

  static final LongAdder DISK_CODES = new LongAdder();

  private final IoUring ioUring;

  /** Sets up fine-code reads for a segment's data file and fields. */
  FineCodeIO(
      SegmentReadState state, String dataName, Collection<IVFFieldReader> fields, IndexInput data) {
    IoUring opened = openIoUring(state, dataName, pageReadable(fields));
    if (opened != null && readsSameBytes(opened, data) == false) {
      IOUtils.closeWhileHandlingException(opened);
      opened = null;
    }
    ioUring = opened;
  }

  /**
   * Whether io_uring, which reads the file underneath every FilterDirectory, reads the bytes the
   * directory serves: a wrapper may transform them (encrypt, compress, shift them), and reranks
   * would then score whatever is on disk with no error. Compares the file's first page.
   */
  static boolean readsSameBytes(IoUring ioUring, IndexInput data) {
    try {
      int length = (int) Math.min(IoUring.PAGE_BYTES, data.length());
      int readLength = ioUring.direct ? IoUring.PAGE_BYTES : length;
      MemorySegment read =
          ioUring.readBatch(
              new long[] {0}, readLength, IoUring.buffer(MemorySegment.NULL, readLength));
      byte[] expected = new byte[length];
      IndexInput in = data.clone();
      in.seek(0);
      in.readBytes(expected, 0, length);
      return MemorySegment.mismatch(read, 0, length, MemorySegment.ofArray(expected), 0, length)
          == -1;
    } catch (IOException | RuntimeException _) {
      return false;
    }
  }

  /**
   * Whether every field's fine codes can be read uncached: each section starts on a page, and each
   * code fills whole pages or divides a page evenly, so no code straddles a page boundary.
   */
  private static boolean pageReadable(Collection<IVFFieldReader> fields) {
    if (fields.isEmpty()) {
      return false;
    }
    for (IVFFieldReader field : fields) {
      if (field.count == 0) {
        // No fine codes to read, and an empty field's sections are not page-aligned.
        continue;
      }
      int recordBytes = field.fine.recordBytes;
      boolean tiled =
          recordBytes % IoUring.PAGE_BYTES == 0 || IoUring.PAGE_BYTES % recordBytes == 0;
      if (tiled == false || field.sectionStart(SECTION_FINE) % IoUring.PAGE_BYTES != 0) {
        return false;
      }
    }
    return true;
  }

  /** Opens batched reads of a file-system directory's data file, or returns null. */
  private static IoUring openIoUring(SegmentReadState state, String name, boolean direct) {
    if (FilterDirectory.unwrap(state.directory) instanceof FSDirectory fs) {
      try {
        return IoUring.open(fs.getDirectory().resolve(name), direct);
      } catch (IOException | RuntimeException _) {
        // Fine codes are read mapped instead.
      }
    }
    return null;
  }

  /** Whether reranks of this segment can read with io_uring. */
  boolean readsDirectly() {
    return ioUring != null;
  }

  /** Supplies a slice of the fine section, opened on first use. */
  interface SliceSupplier {
    RandomAccessInput get() throws IOException;
  }

  /**
   * Returns memory holding the fine codes of {@code count} ordinals from {@code ords[from]}, with
   * each code's start in {@code offsets}. The copied fallback reads through {@code slice}, into
   * {@code scratch}'s buffer.
   */
  MemorySegment read(
      IVFFieldReader field,
      int[] ords,
      int from,
      int count,
      long[] offsets,
      SliceSupplier slice,
      SearchScratch scratch)
      throws IOException {
    if (ioUring != null && useUring()) {
      MemorySegment read = readBatch(field, ords, from, count, offsets, scratch);
      if (read != null) {
        return read;
      }
    }
    return readMapped(field, ords, from, count, offsets, slice, scratch);
  }

  /** {@link #read} through the mapping, or copied through {@code slice} when it is unmapped. */
  MemorySegment readMapped(
      IVFFieldReader field,
      int[] ords,
      int from,
      int count,
      long[] offsets,
      SliceSupplier slice,
      SearchScratch scratch)
      throws IOException {
    int recordBytes = field.fine.recordBytes;
    if (field.fineSegment != null) {
      for (int i = 0; i < count; i++) {
        offsets[i] = (long) ords[from + i] * recordBytes;
      }
      return field.fineSegment;
    }
    RandomAccessInput codes = slice.get();
    int codeBytes = field.fine.codeBytes;
    byte[] copied =
        scratch.bytes = ArrayUtil.growNoCopy(scratch.bytes, Math.multiplyExact(count, codeBytes));
    for (int i = 0; i < count; i++) {
      codes.readBytes((long) ords[from + i] * recordBytes, copied, i * codeBytes, codeBytes);
      offsets[i] = (long) i * codeBytes;
    }
    return MemorySegment.ofArray(copied);
  }

  /**
   * Reads the codes with one io_uring batch, or returns null if the batch failed. Uncached codes
   * smaller than a page are read as their pages, each page once.
   *
   * <p>On return, code {@code i} (ordinal {@code ords[from + i]}) starts at {@code offsets[i]} in
   * the returned buffer. The batch is laid out in one of two ways:
   *
   * <ul>
   *   <li><b>Whole pages</b>, for O_DIRECT reads of codes smaller than a page. O_DIRECT only reads
   *       whole, page-aligned pages, so each code's page is read instead, and the code is found at
   *       its offset within it. Codes divide a page evenly ({@link #pageReadable}), so no code
   *       straddles two. Shortlisted codes often share a page, so each page is read once: read
   *       {@code k} lands at {@code k * PAGE_BYTES}, and every code on that page points into it.
   *   <li><b>One read per code</b> otherwise: through the page cache, where any position and length
   *       will do, or for codes of whole pages, which are already aligned. Read {@code i} is code
   *       {@code i}, at {@code i * recordBytes}.
   * </ul>
   */
  private MemorySegment readBatch(
      IVFFieldReader field,
      int[] ords,
      int from,
      int count,
      long[] offsets,
      SearchScratch scratch) {
    // The file position of each read in the batch; reads may be fewer than codes.
    long[] positions = scratch.readPositions = ArrayUtil.growNoCopy(scratch.readPositions, count);
    int length = readLength(field);
    int reads = plan(field, ords, from, count, positions, offsets, scratch);
    // The thread's reused, page-aligned buffer, grown to hold every read of the batch.
    scratch.uringBuffer = IoUring.buffer(scratch.uringBuffer, (long) reads * length);
    try {
      // One submission for the whole batch: the device serves the reads concurrently, so the
      // batch costs about one read's latency rather than one per code.
      MemorySegment buffer = ioUring.readBatch(positions, reads, length, scratch.uringBuffer);
      // Reads issued and codes they served: fewer reads than codes is pages shared.
      DISK_READS.add(reads);
      DISK_CODES.add(count);
      return buffer;
    } catch (IOException _) {
      // The caller falls back to mapped. If reads may still land in the buffer, it is dropped
      // (IoUring keeps it alive); a batch that failed before submitting, or after every read came
      // back, leaves it reusable.
      if (IoUring.retired(scratch.uringBuffer)) {
        scratch.uringBuffer = MemorySegment.NULL;
      }
      return null;
    } catch (Throwable t) {
      // Likewise whatever else was thrown, which propagates.
      if (IoUring.retired(scratch.uringBuffer)) {
        scratch.uringBuffer = MemorySegment.NULL;
      }
      throw t;
    }
  }

  /** Whether reads are of whole pages: uncached reads of codes smaller than a page. */
  private boolean readsPages(IVFFieldReader field) {
    return ioUring.direct && field.fine.recordBytes < IoUring.PAGE_BYTES;
  }

  /** The length of every read of a batch: a page, or a code. */
  private int readLength(IVFFieldReader field) {
    return readsPages(field) ? IoUring.PAGE_BYTES : field.fine.recordBytes;
  }

  /**
   * Plans the reads of {@code count} codes from {@code ords[from]}, laid out as {@link #readBatch}
   * describes: fills {@code positions} with each read's file position and {@code offsets} with each
   * code's start in the batch's buffer, and returns how many reads there are.
   */
  private int plan(
      IVFFieldReader field,
      int[] ords,
      int from,
      int count,
      long[] positions,
      long[] offsets,
      SearchScratch scratch) {
    int recordBytes = field.fine.recordBytes;
    long fineStart = field.sectionStart(SECTION_FINE);
    if (readsPages(field) == false) {
      for (int i = 0; i < count; i++) {
        positions[i] = fineStart + (long) ords[from + i] * recordBytes;
        offsets[i] = (long) i * recordBytes;
      }
      return count;
    }
    // The page map gives each distinct page the next read index, and a page already seen the
    // index it got then.
    scratch.clearPages(count);
    int reads = 0;
    for (int i = 0; i < count; i++) {
      // Fine codes are stored by ordinal, each recordBytes long, from the section's start.
      long position = fineStart + (long) ords[from + i] * recordBytes;
      // Round down to the code's page: PAGE_BYTES is a power of two, so -PAGE_BYTES is a mask of
      // every bit above the in-page offset.
      long page = position & -IoUring.PAGE_BYTES;
      int read = scratch.pageRead(page, reads);
      if (read == reads) {
        // A page not seen before this batch: it becomes read number `reads`.
        positions[reads++] = page;
      }
      // The code sits at its offset within its page, in that page's place in the buffer.
      offsets[i] = (long) read * IoUring.PAGE_BYTES + (position - page);
    }
    return reads;
  }

  // ---- One batch for a cross-segment rerank.
  //
  // A cross-segment rerank reads each segment's share of the index-wide shortlist from that
  // segment's file. Read share by share, each would wait for its own reads, so a rerank spread over
  // many segments would pay one device round trip per segment, and hold a ring per segment. Instead
  // every share is planned first (plan) and all are read in one batch on one ring (readAll): one
  // submission and one wait, however many segments the shortlist spans.

  /** One segment's share of a cross-segment rerank's reads, planned by {@link #plan}. */
  static final class Plan {
    private final FineCodeIO reader;
    private final long[] positions;
    private final int reads;
    private final int length;

    /** Where each code starts in the buffer {@link #readAll} returns, once it has read the plan. */
    final long[] offsets;

    private Plan(FineCodeIO reader, long[] positions, int reads, int length, long[] offsets) {
      this.reader = reader;
      this.positions = positions;
      this.reads = reads;
      this.length = length;
      this.offsets = offsets;
    }
  }

  /**
   * Plans the reads of {@code count} codes from {@code ords[from]}, or returns null when they are
   * read mapped instead, io_uring being unavailable here.
   */
  Plan plan(IVFFieldReader field, int[] ords, int from, int count, SearchScratch scratch) {
    if (ioUring == null || useUring() == false) {
      return null;
    }
    long[] positions = new long[count];
    long[] offsets = new long[count];
    int reads = plan(field, ords, from, count, positions, offsets, scratch);
    return new Plan(this, positions, reads, readLength(field), offsets);
  }

  /**
   * A thread's buffer for {@link FineCodeIO#readAll}, reused by its next query rather than
   * allocated per query: a native allocation is freed only once GC finds it unreachable, which heap
   * pressure does not hasten, so one per query piles up under load. A query holds it from {@link
   * #acquire}, until its reranks have read the codes, to {@link #release}; a query nested in one
   * gets a fresh buffer, as {@link SearchScratch} does.
   */
  static final class BatchBuffer {
    private static final ThreadLocal<BatchBuffer> LOCAL = ThreadLocal.withInitial(BatchBuffer::new);

    private MemorySegment segment = MemorySegment.NULL;
    private boolean inUse;

    /** The thread's buffer, or a fresh one when a query on this thread already holds it. */
    static BatchBuffer acquire() {
      BatchBuffer buffer = LOCAL.get();
      if (buffer.inUse) {
        buffer = new BatchBuffer();
      }
      buffer.inUse = true;
      return buffer;
    }

    /** Hands the buffer back for the thread's next query, unless it outgrew what is kept. */
    void release() {
      inUse = false;
      if (segment.byteSize() > SearchScratch.MAX_RETAINED_BYTES) {
        segment = MemorySegment.NULL;
      }
    }
  }

  /**
   * Reads every plan in one batch into {@code into}, grown as needed, and returns the segment they
   * landed in, each plan's codes at its {@link Plan#offsets}; or null if the batch failed, the
   * codes then to be read mapped. Plans whose read lengths differ (segments read O_DIRECT with
   * others read through the page cache) are not batched: null is returned, and every plan is read
   * mapped.
   */
  static MemorySegment readAll(List<Plan> plans, BatchBuffer into) {
    if (plans.isEmpty()) {
      return MemorySegment.NULL;
    }
    int length = plans.get(0).length;
    int reads = 0;
    for (Plan plan : plans) {
      if (plan.length != length) {
        return null;
      }
      reads += plan.reads;
    }
    // Every read, its file, and its place in the buffer: plan after plan.
    IoUring[] files = new IoUring[reads];
    long[] positions = new long[reads];
    int read = 0;
    for (Plan plan : plans) {
      long shift = (long) read * length;
      for (int i = 0; i < plan.offsets.length; i++) {
        plan.offsets[i] += shift;
      }
      for (int r = 0; r < plan.reads; r++) {
        files[read] = plan.reader.ioUring;
        positions[read++] = plan.positions[r];
      }
    }
    MemorySegment buffer = into.segment = IoUring.buffer(into.segment, (long) reads * length);
    try {
      IoUring.readBatch(files, positions, reads, length, buffer);
    } catch (IOException _) {
      // The codes are read mapped. The buffer is dropped only if reads may still land in it, as
      // in readBatch.
      if (IoUring.retired(buffer)) {
        into.segment = MemorySegment.NULL;
      }
      return null;
    } catch (Throwable t) {
      if (IoUring.retired(buffer)) {
        into.segment = MemorySegment.NULL;
      }
      throw t;
    }
    DISK_READS.add(reads);
    for (Plan plan : plans) {
      DISK_CODES.add(plan.offsets.length);
    }
    return buffer;
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(ioUring);
  }

  /** Whether reranks read fine codes with io_uring where it is available: always, unless forced. */
  static boolean useUring() {
    if (IoUring.disabled()) {
      // Every attempt would fail before submitting: read mapped without trying.
      return false;
    }
    Boolean forced = forceUring;
    return forced == null || forced;
  }
}
