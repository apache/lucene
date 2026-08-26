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
package org.apache.lucene.index;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.hnsw.DefaultFlatVectorScorer;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsFormat;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.ThreadInterruptedException;

/**
 * Standalone benchmark for incremental (sparse) doc-values updates. This is <b>not</b> a unit test
 * and does not use the test framework. Run it as a normal Java application (via {@link #main}) so
 * the JVM is configured like production (real JIT, no forced assertions). It would be removed
 * before this feature merges; it lives here only so reviewers can reproduce the numbers.
 *
 * <p>It compares the three ways to change one field on a document that also carries a vector: a
 * full reindex of the document ({@code updateDocument}), the classic dense doc-values update
 * (rewrite the whole column), and the incremental sparse update ({@link
 * IndexWriterConfig#setMaxDocValuesOverlays}). The index is built and then updated by {@code
 * dvbench.threads} threads, with a background near-real-time refresh every {@code
 * dvbench.refreshMs} (like a search application), rather than periodic commits. The same updates
 * are applied each way and measured identically (update throughput, bytes written per update
 * against the raw value size, the live file/generation count, and an aggregation scan), and all
 * three assert the same final aggregate. With {@code dvbench.queryThreads > 0} background threads
 * run aggregation queries against the refreshing reader; with {@code dvbench.sweep=true} it also
 * runs the sparse arm across a range of {@code maxDocValuesOverlays}.
 *
 * <p>Example (production-like: assertions off, real JIT, mmap directory):
 *
 * <pre>
 * java -da -Xmx6g --enable-native-access=ALL-UNNAMED \
 *   -Ddvbench.docs=5000000 -Ddvbench.dims=512 -Ddvbench.flatVectors=true \
 *   -Ddvbench.updates=1000000 -Ddvbench.threads=4 -Ddvbench.maxGens=16 \
 *   -cp lucene-core.jar:. org.apache.lucene.index.IncrementalDocValuesUpdatesBenchmark
 * </pre>
 */
public class IncrementalDocValuesUpdatesBenchmark {

  private static final int NUM_DOCS = Integer.getInteger("dvbench.docs", 50_000);
  private static final int DIMS =
      Integer.getInteger("dvbench.dims", 128); // <= 0 disables the vector
  private static final int NUM_UPDATES = Integer.getInteger("dvbench.updates", 100_000);
  private static final int THREADS = Integer.getInteger("dvbench.threads", 1);
  private static final int REFRESH_MS = Integer.getInteger("dvbench.refreshMs", 1_000);
  private static final int MAX_GENS = Integer.getInteger("dvbench.maxGens", 16);
  private static final String TYPE =
      System.getProperty("dvbench.type", "numeric"); // numeric, binary, softdelete
  private static final boolean BINARY = "binary".equals(TYPE);
  private static final boolean SOFT_DELETES = "softdelete".equals(TYPE);
  private static final int BINLEN =
      Integer.getInteger("dvbench.binlen", 32); // binary value length, >= 8
  private static final boolean FLAT_VECTORS = Boolean.getBoolean("dvbench.flatVectors");
  private static final int QUERY_THREADS = Integer.getInteger("dvbench.queryThreads", 0);
  // Aggregate updates/sec across all threads; 0 = as fast as possible (batch). A fixed rate models
  // a steady stream,
  // where dense rewrites the whole column on every refresh regardless of how few docs actually
  // changed.
  private static final double RATE = Double.parseDouble(System.getProperty("dvbench.rate", "0"));
  // Random-access lookup samples every Nth doc (in ascending docid order, the sort/facet read
  // pattern); default 10 = ~10% of docs.
  private static final int LOOKUP_STRIDE = Integer.getInteger("dvbench.lookupStride", 10);
  private static final String SOFT_FIELD = "__soft_delete";

  /** Sink so the JIT cannot elide the random-access lookup whose result is otherwise unused. */
  @SuppressWarnings("unused")
  private static volatile long blackhole;

  /**
   * Raw size of one logical value: what a single update actually changes, the denominator for write
   * amplification.
   */
  private static final int RAW_VALUE_BYTES = BINARY ? BINLEN : Long.BYTES;

  private static final int VECTOR_BYTES = DIMS > 0 ? DIMS * Float.BYTES : 0;

  /**
   * No field is analyzed (the id is a StringField), so a no-op analyzer avoids pulling in an
   * analysis module.
   */
  private static Analyzer noopAnalyzer() {
    return new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        throw new UnsupportedOperationException("no analyzed fields in this benchmark");
      }
    };
  }

  /** Flat (graph-less) vector storage so million-doc runs at high dimensions stay tractable. */
  private static Codec flatVectorCodec() {
    return new FilterCodec("Lucene104", Codec.forName("Lucene104")) {
      @Override
      public KnnVectorsFormat knnVectorsFormat() {
        return new Lucene99FlatVectorsFormat(DefaultFlatVectorScorer.INSTANCE);
      }
    };
  }

  private enum Mode {
    FULL_REINDEX, // updateDocument: rewrite the whole document
    DENSE_DV, // update, feature off: rewrite the whole column
    SPARSE_DV // update, feature on: sparse delta generation
  }

  private static final class CountingDirectory extends FilterDirectory {
    final AtomicLong bytesWritten = new AtomicLong();

    CountingDirectory(Directory in) {
      super(in);
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
      return new CountingOutput(super.createOutput(name, context), bytesWritten);
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context)
        throws IOException {
      return new CountingOutput(super.createTempOutput(prefix, suffix, context), bytesWritten);
    }
  }

  private static final class CountingOutput extends IndexOutput {
    private final IndexOutput in;
    private final AtomicLong counter;

    CountingOutput(IndexOutput in, AtomicLong counter) {
      super("Counting(" + in + ")", in.getName());
      this.in = in;
      this.counter = counter;
    }

    @Override
    public void writeByte(byte b) throws IOException {
      counter.incrementAndGet();
      in.writeByte(b);
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
      counter.addAndGet(length);
      in.writeBytes(b, offset, length);
    }

    @Override
    public void close() throws IOException {
      in.close();
    }

    @Override
    public long getFilePointer() {
      return in.getFilePointer();
    }

    @Override
    public long getChecksum() throws IOException {
      return in.getChecksum();
    }
  }

  private static float[] vectorForId(int id) {
    if (DIMS <= 0) {
      return null;
    }
    Random rnd = new Random(id);
    float[] v = new float[DIMS];
    double norm = 0;
    for (int i = 0; i < DIMS; i++) {
      v[i] = rnd.nextFloat();
      norm += v[i] * v[i];
    }
    norm = Math.sqrt(norm);
    for (int i = 0; i < DIMS; i++) {
      v[i] /= (float) norm;
    }
    return v;
  }

  /**
   * Encodes the logical long value into a {@code BINLEN}-byte payload so the binary arm aggregates
   * the same sum.
   */
  private static BytesRef toBytes(long v) {
    byte[] b = new byte[BINLEN];
    for (int i = 0; i < BINLEN; i++) {
      b[i] = (byte) (v >>> (8 * (i % 8)));
    }
    return new BytesRef(b);
  }

  private static long fromBytes(BytesRef ref) {
    long v = 0;
    for (int i = 7; i >= 0; i--) {
      v = (v << 8) | (ref.bytes[ref.offset + i] & 0xFFL);
    }
    return v;
  }

  /**
   * Rebuilds the full document for {@code id} with the given value (used by the full-reindex arm).
   */
  private static Document docFor(int id, long val) {
    Document d = new Document();
    d.add(new StringField("id", Integer.toString(id), StringField.Store.NO));
    if (BINARY) {
      d.add(new BinaryDocValuesField("val", toBytes(val)));
    } else {
      d.add(new NumericDocValuesField("val", val));
    }
    // Soft-delete docs start live with no soft-delete field: a doc is soft-deleted once the field
    // is
    // present on it, so the mark is added later as a doc-values update, never at build time.
    if (DIMS > 0) {
      d.add(new KnnFloatVectorField("vec", vectorForId(id), VectorSimilarityFunction.DOT_PRODUCT));
    }
    return d;
  }

  private static void applyUpdate(IndexWriter w, Mode mode, int id, long val) throws IOException {
    Term term = new Term("id", Integer.toString(id));
    if (SOFT_DELETES) {
      // The soft-delete mark is itself a numeric doc-values update, so the dense/sparse arms
      // measure exactly what this
      // feature changes about soft deletes; the full-reindex slot is a hard delete (liveDocs, no
      // doc-values write).
      if (mode == Mode.FULL_REINDEX) {
        w.deleteDocuments(term);
      } else {
        // updateDocValues creates the soft-delete column on first use; marking a doc sets it to 1.
        w.updateDocValues(term, new NumericDocValuesField(SOFT_FIELD, 1L));
      }
    } else if (mode == Mode.FULL_REINDEX) {
      w.updateDocument(term, docFor(id, val));
    } else if (BINARY) {
      w.updateBinaryDocValue(term, "val", toBytes(val));
    } else {
      w.updateNumericDocValue(term, "val", val);
    }
  }

  private record ArmResult(
      long updateBytes,
      long updateMs,
      long scanMs,
      long lookupMs,
      long sum,
      int peakDvd,
      int endFiles,
      long queries,
      double avgQueryMs) {}

  private interface ShardTask {
    void run(int shard) throws Exception;
  }

  /**
   * Runs {@code task} on {@code threads} threads (shards 0..threads-1) and waits for all of them.
   */
  private static void parallel(int threads, AtomicReference<Throwable> error, ShardTask task) {
    Thread[] workers = new Thread[threads];
    for (int i = 0; i < threads; i++) {
      final int shard = i;
      workers[i] =
          new Thread(
              () -> {
                try {
                  task.run(shard);
                } catch (Throwable t) {
                  error.compareAndSet(null, t);
                }
              });
      workers[i].start();
    }
    for (Thread t : workers) {
      try {
        t.join();
      } catch (InterruptedException ie) {
        throw new ThreadInterruptedException(ie);
      }
    }
  }

  /**
   * Counts {@code .dvd} data files in the directory, one per live doc-values generation (base +
   * deltas).
   */
  private static int countDvd(Directory dir) throws IOException {
    int n = 0;
    for (String f : dir.listAll()) {
      if (f.endsWith(".dvd")) {
        n++;
      }
    }
    return n;
  }

  /** Live-doc aggregation over the updated field, the read path the overlay affects. */
  private static long aggScan(DirectoryReader reader) throws IOException {
    long sum = 0;
    for (LeafReaderContext ctx : reader.leaves()) {
      Bits live = ctx.reader().getLiveDocs();
      if (BINARY) {
        BinaryDocValues dv = ctx.reader().getBinaryDocValues("val");
        for (int doc = dv.nextDoc(); doc != BinaryDocValues.NO_MORE_DOCS; doc = dv.nextDoc()) {
          if (live == null || live.get(doc)) {
            sum += fromBytes(dv.binaryValue());
          }
        }
      } else {
        NumericDocValues dv = ctx.reader().getNumericDocValues("val");
        // In soft-delete mode a doc is excluded when its soft-delete mark is set (reading that mark
        // exercises the
        // overlay, which is the point). The mark field is absent on the hard-delete arm, so soft
        // stays null there.
        NumericDocValues soft = SOFT_DELETES ? ctx.reader().getNumericDocValues(SOFT_FIELD) : null;
        for (int doc = dv.nextDoc(); doc != NumericDocValues.NO_MORE_DOCS; doc = dv.nextDoc()) {
          if (live != null && live.get(doc) == false) {
            continue;
          }
          if (soft != null && soft.advanceExact(doc) && soft.longValue() != 0) {
            continue;
          }
          sum += dv.longValue();
        }
      }
    }
    return sum;
  }

  /**
   * Random-access doc-values lookup over an ascending sample of docs, the sort/facet read pattern
   * (forward-only {@code advanceExact}, as the doc-values contract requires). This is where the
   * overlay merge is felt most, since each lookup resolves the doc across all layers rather than
   * streaming.
   */
  private static long randomLookup(DirectoryReader reader) throws IOException {
    long checksum = 0;
    for (LeafReaderContext ctx : reader.leaves()) {
      int maxDoc = ctx.reader().maxDoc();
      if (BINARY) {
        BinaryDocValues dv = ctx.reader().getBinaryDocValues("val");
        for (int doc = 0; doc < maxDoc; doc += LOOKUP_STRIDE) {
          if (dv.advanceExact(doc)) {
            checksum += fromBytes(dv.binaryValue());
          }
        }
      } else {
        NumericDocValues dv = ctx.reader().getNumericDocValues("val");
        for (int doc = 0; doc < maxDoc; doc += LOOKUP_STRIDE) {
          if (dv.advanceExact(doc)) {
            checksum += dv.longValue();
          }
        }
      }
    }
    return checksum;
  }

  /** Runs aggregation queries against the refreshing reader until stopped. */
  private static final class Querier extends Thread {
    private final ReaderManager readers;
    private final AtomicBoolean stop;
    private final AtomicReference<Throwable> error;
    long queries;
    long totalNanos;

    Querier(ReaderManager readers, AtomicBoolean stop, AtomicReference<Throwable> error) {
      this.readers = readers;
      this.stop = stop;
      this.error = error;
    }

    @Override
    public void run() {
      try {
        while (stop.get() == false) {
          DirectoryReader reader = readers.acquire();
          try {
            long t0 = System.nanoTime();
            aggScan(reader);
            totalNanos += System.nanoTime() - t0;
            queries++;
          } finally {
            readers.release(reader);
          }
        }
      } catch (Throwable t) {
        error.compareAndSet(null, t);
      }
    }
  }

  private static ArmResult runArm(Mode mode, int maxGens) throws IOException {
    Path tmp = Files.createTempDirectory("dvbench");
    CountingDirectory dir = new CountingDirectory(FSDirectory.open(tmp));
    IndexWriterConfig conf =
        new IndexWriterConfig(noopAnalyzer())
            .setMaxDocValuesOverlays(mode == Mode.SPARSE_DV ? maxGens : 0);
    if (FLAT_VECTORS && DIMS > 0) {
      conf.setCodec(flatVectorCodec());
    }
    if (SOFT_DELETES) {
      conf.setSoftDeletesField(SOFT_FIELD);
    }
    IndexWriter w = new IndexWriter(dir, conf);
    AtomicReference<Throwable> error = new AtomicReference<>();

    parallel(
        THREADS,
        error,
        shard -> {
          for (int i = shard; i < NUM_DOCS; i += THREADS) {
            w.addDocument(docFor(i, i));
          }
        });
    rethrow(error);
    w.commit();
    // Let the natural background merges finish; updates run against the many-segment index they
    // leave.
    w.waitForMerges();

    // Pre-generate the updates. The ids are uniformly random, so updates hit scattered docs in
    // random order (not the
    // ingestion order); each delta generation then holds non-contiguous docids, which stresses the
    // sparse encoding
    // and the overlay. Pre-generating also lets every arm and thread count reach the same final
    // values: each doc's
    // updates all run on the same thread (partitioned by id), so the last-writer-wins result is
    // deterministic.
    int[] ids = new int[NUM_UPDATES];
    long[] vals = new long[NUM_UPDATES];
    Random up = new Random(7);
    for (int i = 0; i < NUM_UPDATES; i++) {
      ids[i] = up.nextInt(NUM_DOCS);
      vals[i] = up.nextLong();
    }

    ReaderManager readers = new ReaderManager(w);
    AtomicBoolean stop = new AtomicBoolean();
    AtomicInteger peakDvd = new AtomicInteger();

    // Near-real-time refresh: reopen every REFRESH_MS, which applies the buffered doc-values
    // updates (writing their
    // generations), the way a search application refreshes rather than committing on a fixed update
    // count.
    Thread refresher =
        new Thread(
            () -> {
              try {
                while (stop.get() == false) {
                  LockSupport.parkNanos(REFRESH_MS * 1_000_000L);
                  readers.maybeRefresh();
                  peakDvd.accumulateAndGet(countDvd(dir), Math::max);
                }
              } catch (Throwable t) {
                error.compareAndSet(null, t);
              }
            });
    refresher.start();

    Querier[] queriers = new Querier[QUERY_THREADS];
    for (int i = 0; i < QUERY_THREADS; i++) {
      queriers[i] = new Querier(readers, stop, error);
      queriers[i].start();
    }

    long startWrite = dir.bytesWritten.get();
    final long updateStart = System.nanoTime();
    // With a target rate, each thread paces itself to RATE/THREADS updates/sec so the aggregate
    // matches RATE.
    final long perThreadIntervalNanos = RATE > 0 ? (long) (1e9 * THREADS / RATE) : 0;
    parallel(
        THREADS,
        error,
        shard -> {
          int applied = 0;
          for (int i = 0; i < NUM_UPDATES; i++) {
            if (ids[i] % THREADS != shard) {
              continue;
            }
            if (perThreadIntervalNanos > 0) {
              long wait = (updateStart + applied * perThreadIntervalNanos) - System.nanoTime();
              if (wait > 0) {
                LockSupport.parkNanos(wait);
              }
            }
            applyUpdate(w, mode, ids[i], vals[i]);
            applied++;
          }
        });
    long updateMs = (System.nanoTime() - updateStart) / 1_000_000;

    stop.set(
        true); // the refresher finishes its current sleep + refresh, then exits (no interrupt: it
    // may be mid-IO)
    join(refresher);
    long queries = 0;
    long queryNanos = 0;
    for (Querier q : queriers) {
      join(q);
      queries += q.queries;
      queryNanos += q.totalNanos;
    }
    rethrow(error);

    readers.maybeRefresh(); // apply any updates buffered since the last refresh
    peakDvd.accumulateAndGet(countDvd(dir), Math::max);
    long updateBytes = dir.bytesWritten.get() - startWrite;
    int endFiles = dir.listAll().length;

    DirectoryReader reader = readers.acquire();
    long sum;
    long scanMs;
    long lookupMs;
    try {
      long scanStart = System.nanoTime();
      sum = aggScan(reader);
      scanMs = (System.nanoTime() - scanStart) / 1_000_000;
      long lookupStart = System.nanoTime();
      blackhole += randomLookup(reader);
      lookupMs = (System.nanoTime() - lookupStart) / 1_000_000;
    } finally {
      readers.release(reader);
    }

    readers.close();
    w.close();
    dir.close();
    IOUtils.rm(tmp);
    double avgQueryMs = queries == 0 ? 0 : queryNanos / (double) queries / 1e6;
    return new ArmResult(
        updateBytes, updateMs, scanMs, lookupMs, sum, peakDvd.get(), endFiles, queries, avgQueryMs);
  }

  private static void join(Thread t) {
    try {
      t.join();
    } catch (InterruptedException ie) {
      throw new ThreadInterruptedException(ie);
    }
  }

  private static void rethrow(AtomicReference<Throwable> error) throws IOException {
    Throwable t = error.get();
    if (t != null) {
      throw new RuntimeException("benchmark worker failed", t);
    }
  }

  private static void report(String label, ArmResult r) {
    double perUpdate = (double) r.updateBytes / NUM_UPDATES;
    double updatesPerSec = r.updateMs == 0 ? 0 : NUM_UPDATES / (r.updateMs / 1000.0);
    String concurrent =
        r.queries == 0
            ? ""
            : String.format(Locale.ROOT, "  concurrent_q=%d@%,.1fms", r.queries, r.avgQueryMs);
    System.out.println(
        String.format(
            Locale.ROOT,
            "[%-12s] %,.0f upd/s  per_update=%,.0fB  amp=%,.1fx(raw %dB)  total=%,.1fMB  peak_dv_gens=%d  files=%d  scan=%dms  lookup=%dms%s",
            label,
            updatesPerSec,
            perUpdate,
            perUpdate / RAW_VALUE_BYTES,
            RAW_VALUE_BYTES,
            r.updateBytes / 1e6,
            r.peakDvd,
            r.endFiles,
            r.scanMs,
            r.lookupMs,
            concurrent));
  }

  private static void checkSame(long a, long b) {
    if (a != b) {
      throw new AssertionError("arms disagree on the final aggregate: " + a + " != " + b);
    }
  }

  private static String rateStr() {
    return RATE > 0 ? String.format(Locale.ROOT, "%,.0f/s", RATE) : "unlimited";
  }

  private static void runArms() throws IOException {
    System.out.println(
        String.format(
            Locale.ROOT,
            "=== %s val=%dB, %d docs, vector=%dd/%dB (%s), doc~%dB, %d updates, %d threads, rate=%s, refresh/%dms, maxGens=%d, queryThreads=%d ===",
            TYPE.toUpperCase(Locale.ROOT),
            RAW_VALUE_BYTES,
            NUM_DOCS,
            DIMS,
            VECTOR_BYTES,
            DIMS <= 0 ? "none" : (FLAT_VECTORS ? "flat" : "hnsw"),
            RAW_VALUE_BYTES + VECTOR_BYTES,
            NUM_UPDATES,
            THREADS,
            rateStr(),
            REFRESH_MS,
            MAX_GENS,
            QUERY_THREADS));
    ArmResult a = runArm(Mode.FULL_REINDEX, MAX_GENS);
    report(SOFT_DELETES ? "hard_delete" : "full_reindex", a);
    ArmResult b = runArm(Mode.DENSE_DV, MAX_GENS);
    report(SOFT_DELETES ? "dense_soft" : "dense_dv", b);
    ArmResult c = runArm(Mode.SPARSE_DV, MAX_GENS);
    report(SOFT_DELETES ? "sparse_soft" : "sparse_dv", c);
    checkSame(a.sum, b.sum);
    checkSame(b.sum, c.sum);
  }

  private static void runSweep() throws IOException {
    System.out.println(
        String.format(
            Locale.ROOT,
            "=== SWEEP %s: %d docs, %d updates, %d threads, refresh/%dms ===",
            BINARY ? "BINARY" : "NUMERIC",
            NUM_DOCS,
            NUM_UPDATES,
            THREADS,
            REFRESH_MS));
    long expect = Long.MIN_VALUE;
    for (int k : new int[] {2, 4, 8, 16, 32, 64, 128}) {
      ArmResult r = runArm(Mode.SPARSE_DV, k);
      report("sparse maxGens=" + k, r);
      if (expect == Long.MIN_VALUE) {
        expect = r.sum;
      } else {
        checkSame(expect, r.sum);
      }
    }
  }

  public static void main(String[] args) throws IOException {
    boolean assertionsOn = false;
    assert (assertionsOn = true) == true; // Intentional side-effect
    if (assertionsOn) {
      System.out.println(
          "WARNING: assertions are enabled (-ea); re-run with -da for production-like timing.");
    }
    runArms();
    if (Boolean.getBoolean("dvbench.sweep")) {
      runSweep();
    }
  }
}
