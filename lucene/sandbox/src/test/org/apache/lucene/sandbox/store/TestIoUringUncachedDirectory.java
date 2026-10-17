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
package org.apache.lucene.sandbox.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.SuppressForbidden;

/**
 * Reads served through io_uring have to agree byte for byte with ordinary reads, whether or not the
 * caller prefetched them, and a prefetch has to be consumable through a different clone of the
 * input than the one that started it, since that is how rescoring uses it.
 *
 * <p>io_uring needs a recent kernel and {@code liburing-ffi}, so these tests only run when {@code
 * -Dtests.iouring.run=true} is set and the engine reports itself usable.
 */
public class TestIoUringUncachedDirectory extends LuceneTestCase {

  private static final String FILE = "_0.vec";

  /** The plain context: files are selected by extension, not by hint. */
  private static final IOContext VECTORS = IOContext.DEFAULT;

  private static final int FILE_BYTES = 1 << 20;

  private static boolean enabled() {
    return Boolean.getBoolean("tests.iouring.run") && IoUring.isAvailable();
  }

  @SuppressForbidden(reason = "temp dir for a native-fd test")
  private Path tmpDir() throws IOException {
    return createTempDir("iouring-uncached");
  }

  private byte[] writeFile(Path dir) throws IOException {
    byte[] expected = new byte[FILE_BYTES];
    new Random(42).nextBytes(expected);
    try (Directory plain = new MMapDirectory(dir);
        IndexOutput out = plain.createOutput(FILE, IOContext.DEFAULT)) {
      out.writeBytes(expected, expected.length);
    }
    return expected;
  }

  public void testPrefetchedReadsMatchPlainReads() throws Exception {
    assumeTrue("io_uring not available", enabled());
    Path dir = tmpDir();
    byte[] expected = writeFile(dir);
    try (IoUringUncachedDirectory d =
        new IoUringUncachedDirectory(new MMapDirectory(dir), 64, true)) {
      assertTrue("expected io_uring to be enabled", d.isEnabled());
      try (IndexInput in = d.openInput(FILE, VECTORS)) {
        byte[] got = new byte[4096];
        // Prefetch well ahead of the reads, then consume them in order.
        for (int i = 0; i < 16; i++) {
          in.prefetch((long) i * 4096, 4096);
        }
        for (int i = 0; i < 16; i++) {
          in.seek((long) i * 4096);
          in.readBytes(got, 0, got.length);
          for (int j = 0; j < got.length; j++) {
            assertEquals("byte " + (i * 4096 + j), expected[i * 4096 + j], got[j]);
          }
        }
      }
    }
  }

  public void testReadWithoutPrefetchStillCorrect() throws Exception {
    assumeTrue("io_uring not available", enabled());
    Path dir = tmpDir();
    byte[] expected = writeFile(dir);
    try (IoUringUncachedDirectory d =
            new IoUringUncachedDirectory(new MMapDirectory(dir), 64, true);
        IndexInput in = d.openInput(FILE, VECTORS)) {
      byte[] got = new byte[1024];
      in.seek(12345); // deliberately unaligned, which RWF_DONTCACHE permits
      in.readBytes(got, 0, got.length);
      for (int j = 0; j < got.length; j++) {
        assertEquals("byte " + (12345 + j), expected[12345 + j], got[j]);
      }
    }
  }

  public void testPrefetchOnOneCloneReadOnAnother() throws Exception {
    assumeTrue("io_uring not available", enabled());
    Path dir = tmpDir();
    byte[] expected = writeFile(dir);
    try (IoUringUncachedDirectory d =
            new IoUringUncachedDirectory(new MMapDirectory(dir), 64, true);
        IndexInput in = d.openInput(FILE, VECTORS)) {
      IndexInput prefetcher = in.clone();
      IndexInput reader = in.clone();
      for (int i = 0; i < 8; i++) {
        prefetcher.prefetch((long) i * 4096, 4096);
      }
      byte[] got = new byte[4096];
      for (int i = 0; i < 8; i++) {
        reader.seek((long) i * 4096);
        reader.readBytes(got, 0, got.length);
        for (int j = 0; j < got.length; j++) {
          assertEquals("byte " + (i * 4096 + j), expected[i * 4096 + j], got[j]);
        }
      }
    }
  }

  public void testSliceReadsAreOffsetCorrectly() throws Exception {
    assumeTrue("io_uring not available", enabled());
    Path dir = tmpDir();
    byte[] expected = writeFile(dir);
    try (IoUringUncachedDirectory d =
            new IoUringUncachedDirectory(new MMapDirectory(dir), 64, true);
        IndexInput in = d.openInput(FILE, VECTORS)) {
      long sliceStart = 8192;
      IndexInput slice = in.slice("s", sliceStart, 8192);
      slice.prefetch(4096, 4096);
      byte[] got = new byte[4096];
      slice.seek(4096);
      slice.readBytes(got, 0, got.length);
      for (int j = 0; j < got.length; j++) {
        assertEquals(
            "byte " + (sliceStart + 4096 + j), expected[(int) sliceStart + 4096 + j], got[j]);
      }
    }
  }

  /** The vector path reads floats; a missing override there silently falls back to the delegate. */
  public void testPrefetchedFloatReadsMatchPlainReads() throws Exception {
    assumeTrue("io_uring not available", enabled());
    Path dir = tmpDir();
    byte[] expected = writeFile(dir);
    int dim = 1024;
    int bytes = dim * Float.BYTES;
    try (IoUringUncachedDirectory d =
            new IoUringUncachedDirectory(new MMapDirectory(dir), 64, true);
        IndexInput in = d.openInput(FILE, VECTORS)) {
      float[] got = new float[dim];
      for (int i = 0; i < 4; i++) {
        in.prefetch((long) i * bytes, bytes);
      }
      for (int i = 0; i < 4; i++) {
        in.seek((long) i * bytes);
        in.readFloats(got, 0, dim);
        ByteBuffer expectedBytes =
            ByteBuffer.wrap(expected, i * bytes, bytes).order(ByteOrder.LITTLE_ENDIAN);
        for (int j = 0; j < dim; j++) {
          assertEquals("float " + (i * dim + j), expectedBytes.getFloat(), got[j], 0f);
        }
      }
    }
  }

  /**
   * A ring is taken for a batch and handed back once the batch is consumed. With a single ring,
   * many batches in a row must therefore all still be served by io_uring; if rings were not handed
   * back, everything after the first batch would quietly fall through to the delegate.
   */
  public void testRingIsReusedAcrossBatches() throws Exception {
    assumeTrue("io_uring not available", enabled());
    Path dir = tmpDir();
    byte[] expected = writeFile(dir);
    try (IoUringUncachedDirectory d =
            new IoUringUncachedDirectory(new NIOFSDirectory(dir), 8, true, 1);
        IndexInput in = d.openInput(FILE, VECTORS)) {
      byte[] got = new byte[4096];
      for (int batch = 0; batch < 20; batch++) {
        long start = (long) batch * 4 * 4096;
        for (int i = 0; i < 4; i++) {
          assertTrue(
              "batch " + batch + " read " + i + " was not tracked, so the ring was never returned",
              in.prefetch(start + (long) i * 4096, 4096));
        }
        for (int i = 0; i < 4; i++) {
          in.seek(start + (long) i * 4096);
          in.readBytes(got, 0, got.length);
          int base = (int) start + i * 4096;
          for (int j = 0; j < got.length; j++) {
            assertEquals("byte " + (base + j), expected[base + j], got[j]);
          }
        }
      }
    }
  }

  /**
   * RWF_DONTCACHE was added in Linux 6.14 and an older kernel fails every read that carries it, so
   * the capability has to be settled when a file is opened rather than discovered per read.
   */
  public void testUncachedCapabilityIsResolvedOnOpen() throws Exception {
    assumeTrue("io_uring not available", enabled());
    Path dir = tmpDir();
    writeFile(dir);
    try (IoUringUncachedDirectory d =
        new IoUringUncachedDirectory(new MMapDirectory(dir), 64, true)) {
      assertFalse("nothing opened yet, so nothing is known", d.isUncached());
      try (IndexInput in = d.openInput(FILE, VECTORS)) {
        // Whatever the kernel says, reads must work; only the caching behaviour differs.
        byte[] got = new byte[4096];
        in.prefetch(0, 4096);
        in.readBytes(got, 0, got.length);
        if (d.isUncached() == false) {
          assumeTrue("kernel predates RWF_DONTCACHE; reads still served", true);
        }
      }
    }
  }

  /** With every ring on loan a read reports untracked, and the caller's own read still works. */
  public void testPoolExhaustionFallsBackInsteadOfBlocking() throws Exception {
    assumeTrue("io_uring not available", enabled());
    Path dir = tmpDir();
    byte[] expected = writeFile(dir);
    // One ring of depth 2: a third prefetch in the same batch has nowhere to go. NIOFS inputs
    // report every prefetch as not issued, so the return value counts the io_uring reads.
    try (IoUringUncachedDirectory d =
            new IoUringUncachedDirectory(new NIOFSDirectory(dir), 2, true, 1);
        IndexInput in = d.openInput(FILE, VECTORS)) {
      int tracked = 0;
      for (int i = 0; i < 8; i++) {
        if (in.prefetch((long) i * 4096, 4096)) {
          tracked++;
        }
      }
      assertTrue("some reads should have been tracked", tracked > 0);
      assertTrue("depth 2 cannot track 8 reads at once", tracked < 8);
      // Every range still reads correctly, tracked or not.
      byte[] got = new byte[4096];
      for (int i = 0; i < 8; i++) {
        in.seek((long) i * 4096);
        in.readBytes(got, 0, got.length);
        for (int j = 0; j < got.length; j++) {
          assertEquals("byte " + (i * 4096 + j), expected[i * 4096 + j], got[j]);
        }
      }
    }
  }

  /**
   * Rings are handed between threads through the pool, so concurrent batches must not interfere.
   */
  public void testConcurrentBatchesAreIndependent() throws Exception {
    assumeTrue("io_uring not available", enabled());
    Path dir = tmpDir();
    byte[] expected = writeFile(dir);
    try (IoUringUncachedDirectory d =
            new IoUringUncachedDirectory(new MMapDirectory(dir), 32, true, 4);
        IndexInput shared = d.openInput(FILE, VECTORS)) {
      int threads = 8;
      Thread[] workers = new Thread[threads];
      AtomicReference<Throwable> failure = new AtomicReference<>();
      for (int t = 0; t < threads; t++) {
        final int id = t;
        workers[t] =
            new Thread(
                () -> {
                  try (IndexInput in = shared.clone()) {
                    byte[] got = new byte[4096];
                    for (int round = 0; round < 10; round++) {
                      int block = (id * 10 + round) % 200;
                      in.prefetch((long) block * 4096, 4096);
                      in.seek((long) block * 4096);
                      in.readBytes(got, 0, got.length);
                      for (int j = 0; j < got.length; j++) {
                        if (got[j] != expected[block * 4096 + j]) {
                          throw new AssertionError("mismatch at block " + block + " byte " + j);
                        }
                      }
                    }
                  } catch (Throwable e) {
                    failure.compareAndSet(null, e);
                  }
                });
        workers[t].start();
      }
      for (Thread w : workers) {
        w.join();
      }
      if (failure.get() != null) {
        throw new AssertionError("concurrent reads disagreed with the file", failure.get());
      }
    }
  }

  /** A prefetch that the ring cannot take is passed to the delegate rather than dropped. */
  public void testRefusedPrefetchGoesToDelegate() throws Exception {
    assumeTrue("io_uring not available", enabled());
    Path dir = tmpDir();
    byte[] expected = writeFile(dir);
    AtomicInteger delegated = new AtomicInteger();
    NIOFSDirectory counting =
        new NIOFSDirectory(dir) {
          @Override
          public IndexInput openInput(String name, IOContext context) throws IOException {
            return new FilterIndexInput("counting", super.openInput(name, context)) {
              @Override
              public boolean prefetch(long offset, long length) {
                delegated.incrementAndGet();
                return false;
              }
            };
          }
        };
    try (IoUringUncachedDirectory d = new IoUringUncachedDirectory(counting, 2, true, 1);
        IndexInput in = d.openInput(FILE, VECTORS)) {
      int tracked = 0;
      for (int i = 0; i < 8; i++) {
        if (in.prefetch((long) i * 4096, 4096)) {
          tracked++;
        }
      }
      assertTrue("depth 2 cannot track 8 reads at once", tracked < 8);
      assertEquals("every refused prefetch reaches the delegate", 8 - tracked, delegated.get());
      byte[] got = new byte[4096];
      for (int i = 0; i < 8; i++) {
        in.seek((long) i * 4096);
        in.readBytes(got, 0, got.length);
        for (int j = 0; j < got.length; j++) {
          assertEquals("byte " + (i * 4096 + j), expected[i * 4096 + j], got[j]);
        }
      }
    }
  }

  /**
   * A batch longer than the submit batch is handed to the kernel in parts while it is queued, and
   * reads admitted to the page cache are served like the rest.
   */
  public void testLongBatchWithAdmission() throws Exception {
    assumeTrue("io_uring not available", enabled());
    Path dir = tmpDir();
    byte[] expected = writeFile(dir);
    int admitPercent = random().nextInt(101);
    int reads = 100 + random().nextInt(100);
    try (IoUringUncachedDirectory d =
            new IoUringUncachedDirectory(new NIOFSDirectory(dir), 256, true, 1, admitPercent);
        IndexInput in = d.openInput(FILE, VECTORS)) {
      for (int i = 0; i < reads; i++) {
        assertTrue("read " + i + " was not tracked", in.prefetch((long) i * 4096, 4096));
      }
      byte[] got = new byte[4096];
      for (int i = 0; i < reads; i++) {
        in.seek((long) i * 4096);
        in.readBytes(got, 0, got.length);
        for (int j = 0; j < got.length; j++) {
          assertEquals("byte " + (i * 4096 + j), expected[i * 4096 + j], got[j]);
        }
      }
    }
  }

  public void testAdmitPercentIsChecked() throws Exception {
    Path dir = tmpDir();
    expectThrows(
        IllegalArgumentException.class,
        () -> new IoUringUncachedDirectory(new MMapDirectory(dir), 64, false, 1, 101));
  }

  public void testDisabledIsPlainPassthrough() throws Exception {
    Path dir = tmpDir();
    byte[] expected = writeFile(dir);
    try (IoUringUncachedDirectory d =
            new IoUringUncachedDirectory(new MMapDirectory(dir), 64, false);
        IndexInput in = d.openInput(FILE, VECTORS)) {
      assertFalse(d.isEnabled());
      byte[] got = new byte[256];
      in.readBytes(got, 0, got.length);
      for (int j = 0; j < got.length; j++) {
        assertEquals(expected[j], got[j]);
      }
    }
  }
}
