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

import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MergeInfo;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.IOUtils;

/** Exercises {@link IOUringDirectory} against a real ring. Skipped without io_uring. */
public class TestIOUringDirectory extends LuceneTestCase {

  private static final int QUEUE_DEPTH = 8;
  private static final int MAX_RINGS = 4;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    assumeTrue("io_uring unavailable on this platform", LibUring.isAvailable());
  }

  private IOUringDirectory open(Path path) throws IOException {
    return open(path, QUEUE_DEPTH);
  }

  private IOUringDirectory open(Path path, int queueDepth) throws IOException {
    IOUringDirectory dir =
        new IOUringDirectory(
            FSDirectory.open(path), Set.of("vec"), queueDepth, MAX_RINGS, /* enable= */ true);
    assumeTrue("io_uring not usable in " + path, dir.isEnabled());
    return dir;
  }

  private static byte[] writeFile(IOUringDirectory dir, String name, int length)
      throws IOException {
    byte[] content = new byte[length];
    for (int i = 0; i < length; i++) {
      content[i] = (byte) (i * 31 + 7);
    }
    try (IndexOutput out = dir.createOutput(name, IOContext.DEFAULT)) {
      out.writeBytes(content, content.length);
    }
    return content;
  }

  /** A handle comes back, is positioned at zero, and yields the right bytes. */
  public void testPrefetchRangeReadsCorrectBytes() throws Exception {
    Path path = createTempDir("uring-dir");
    try (IOUringDirectory dir = open(path)) {
      byte[] content = writeFile(dir, "test.vec", 8192);
      try (IndexInput in = dir.openInput("test.vec", IOContext.DEFAULT)) {
        for (int iter = 0; iter < 20; iter++) {
          int offset = random().nextInt(content.length - 1);
          int length = 1 + random().nextInt(Math.min(1024, content.length - offset));
          IndexInput handle = in.prefetchRange("range", offset, length);
          assertNotNull("an enabled directory must hand out a handle", handle);
          try (handle) {
            assertEquals(length, handle.length());
            assertEquals(0, handle.getFilePointer());
            byte[] actual = new byte[length];
            handle.readBytes(actual, 0, length);
            for (int i = 0; i < length; i++) {
              assertEquals("byte " + i + " of range at " + offset, content[offset + i], actual[i]);
            }
          }
        }
      }
    }
  }

  /**
   * Issues every range before reading any. There are more ranges than the ring has slots, so {@code
   * prefetchRange} has to crank the ring to free one.
   */
  public void testBatchDeeperThanQueueDepth() throws Exception {
    Path path = createTempDir("uring-batch");
    try (IOUringDirectory dir = open(path)) {
      final int dim = 16;
      final int vectorBytes = dim * Float.BYTES;
      final int count = QUEUE_DEPTH * 5;
      byte[] content = writeFile(dir, "vectors.vec", vectorBytes * count);

      try (IndexInput in = dir.openInput("vectors.vec", IOContext.DEFAULT)) {
        List<IndexInput> handles = new ArrayList<>();
        try {
          for (int i = 0; i < count; i++) {
            IndexInput handle = in.prefetchRange("vector", (long) i * vectorBytes, vectorBytes);
            assertNotNull(handle);
            handles.add(handle);
          }
          for (int i = 0; i < count; i++) {
            float[] vector = new float[dim];
            handles.get(i).readFloats(vector, 0, dim);
            float[] expected = expectedFloats(content, i * vectorBytes, dim);
            assertArrayEquals("vector " + i, expected, vector, 0f);
          }
          IOUringDirectory.Stats stats = dir.stats();
          assertEquals("every range submitted exactly once", count, stats.sqesSubmitted());
          assertEquals("every submitted read completed and was reaped", count, stats.cqesReaped());
          assertTrue(
              "reads are not being batched: " + stats, stats.syscalls() < stats.sqesSubmitted());
          long bucketed = 0;
          for (long n : stats.sqesPerSyscall()) {
            bucketed += n;
          }
          assertEquals("histogram accounts for every syscall", stats.syscalls(), bucketed);
          long reapedBucketed = 0;
          for (long n : stats.cqesPerSyscall()) {
            reapedBucketed += n;
          }
          assertEquals(
              "reaped histogram accounts for every syscall", stats.syscalls(), reapedBucketed);
        } finally {
          IOUtils.close(handles);
        }
      }
    }
  }

  /** A burst smaller than the queue depth is not submitted until a result is needed. */
  public void testBurstSubmitsAsOneSyscall() throws Exception {
    Path path = createTempDir("uring-burst");
    try (IOUringDirectory dir = open(path, 64)) {
      final int count = 20;
      writeFile(dir, "burst.vec", count * 64);
      try (IndexInput in = dir.openInput("burst.vec", IOContext.DEFAULT)) {
        List<IndexInput> handles = new ArrayList<>();
        try {
          for (int i = 0; i < count; i++) {
            handles.add(in.prefetchRange("r", (long) i * 64, 64));
          }
          assertEquals("recording a range must not enter the kernel", 0, dir.stats().syscalls());

          handles.get(0).readByte();
          IOUringDirectory.Stats stats = dir.stats();
          assertEquals(count, stats.sqesSubmitted());
          assertEquals("20 reads fall in the 16-31 bucket", 1, stats.sqesPerSyscall()[5]);
          assertEquals("no read went in alone", 0, stats.sqesPerSyscall()[1]);
          assertEquals("all of it triggered by the read", stats.syscalls(), stats.fromRead());
        } finally {
          IOUtils.close(handles);
        }
      }
    }
  }

  /** Prefetching through a slice of a clone works, with offsets relative to the slice. */
  public void testCapabilitySurvivesSliceAndClone() throws Exception {
    Path path = createTempDir("uring-slice");
    try (IOUringDirectory dir = open(path)) {
      byte[] content = writeFile(dir, "test.vec", 4096);
      try (IndexInput in = dir.openInput("test.vec", IOContext.DEFAULT)) {
        IndexInput slice = in.slice("vector-data", 1024, 2048);
        IndexInput clone = slice.clone();

        IndexInput handle = clone.prefetchRange("range", 256, 128);
        assertNotNull("slice of a clone lost the capability", handle);
        try (handle) {
          byte[] actual = new byte[128];
          handle.readBytes(actual, 0, 128);
          for (int i = 0; i < 128; i++) {
            assertEquals(content[1024 + 256 + i], actual[i]);
          }
        }
        expectThrows(EOFException.class, () -> slice.prefetchRange("past-slice-end", 2040, 16));
      }
    }
  }

  /** Cloning the returned handle awaits the read and shares the buffer with its own position. */
  public void testCloneReturnedHandle() throws Exception {
    Path path = createTempDir("uring-handle-clone");
    try (IOUringDirectory dir = open(path)) {
      byte[] content = writeFile(dir, "test.vec", 4096);
      try (IndexInput in = dir.openInput("test.vec", IOContext.DEFAULT)) {
        IndexInput handle = in.prefetchRange("range", 512, 256);
        assertNotNull(handle);
        IndexInput clone = handle.clone();

        assertEquals(0, clone.getFilePointer());
        byte[] fromClone = new byte[256];
        clone.readBytes(fromClone, 0, 256);
        assertEquals(256, clone.getFilePointer());
        assertEquals(0, handle.getFilePointer());

        byte[] fromHandle = new byte[256];
        handle.readBytes(fromHandle, 0, 256);
        assertArrayEquals(fromHandle, fromClone);
        for (int i = 0; i < 256; i++) {
          assertEquals(content[512 + i], fromClone[i]);
        }

        // Closing the clone leaves the handle usable.
        clone.close();
        handle.seek(0);
        byte[] again = new byte[256];
        handle.readBytes(again, 0, 256);
        assertArrayEquals(fromHandle, again);

        // A clone outlives the handle it came from.
        IndexInput survivor = handle.clone();
        assertEquals(
            "a clone inherits the position it was taken at", 256, survivor.getFilePointer());
        handle.close();
        survivor.seek(0);
        byte[] afterClose = new byte[256];
        survivor.readBytes(afterClose, 0, 256);
        assertArrayEquals(fromHandle, afterClose);
        survivor.close();
      }
    }
  }

  /** A clone carries the position it was taken at. */
  public void testCloneCarriesPosition() throws Exception {
    Path path = createTempDir("uring-clone-pos");
    try (IOUringDirectory dir = open(path)) {
      byte[] content = writeFile(dir, "test.vec", 1024);
      try (IndexInput in = dir.openInput("test.vec", IOContext.DEFAULT)) {
        IndexInput handle = in.prefetchRange("range", 0, 64);
        try (handle) {
          handle.readBytes(new byte[16], 0, 16);
          IndexInput clone = handle.clone();
          assertEquals(16, clone.getFilePointer());
          assertEquals(content[16], clone.readByte());
          clone.close();
        }
      }
    }
  }

  /** Slicing a handle gives a view of part of the same buffer. */
  public void testSliceReturnedHandle() throws Exception {
    Path path = createTempDir("uring-handle-slice");
    try (IOUringDirectory dir = open(path)) {
      byte[] content = writeFile(dir, "test.vec", 2048);
      try (IndexInput in = dir.openInput("test.vec", IOContext.DEFAULT)) {
        IndexInput handle = in.prefetchRange("range", 256, 512);
        try (handle) {
          IndexInput slice = handle.slice("inner", 128, 64);
          assertEquals(64, slice.length());
          byte[] actual = new byte[64];
          slice.readBytes(actual, 0, 64);
          for (int i = 0; i < 64; i++) {
            assertEquals(content[256 + 128 + i], actual[i]);
          }
          expectThrows(EOFException.class, () -> handle.slice("too-long", 128, 512));
          slice.close();
        }
      }
    }
  }

  /** Reading a closed handle fails. */
  public void testReadAfterCloseThrows() throws Exception {
    Path path = createTempDir("uring-closed");
    try (IOUringDirectory dir = open(path)) {
      writeFile(dir, "test.vec", 1024);
      try (IndexInput in = dir.openInput("test.vec", IOContext.DEFAULT)) {
        IndexInput handle = in.prefetchRange("range", 0, 64);
        handle.readBytes(new byte[8], 0, 8);
        handle.close();
        expectThrows(AlreadyClosedException.class, () -> handle.readByte());
      }
    }
  }

  /** Handles closed without being read must not leak rings. */
  public void testAbandonedBatchDoesNotLeakRings() throws Exception {
    Path path = createTempDir("uring-abandon");
    try (IOUringDirectory dir = open(path)) {
      writeFile(dir, "test.vec", 8192);
      try (IndexInput in = dir.openInput("test.vec", IOContext.DEFAULT)) {
        for (int batch = 0; batch < 10; batch++) {
          List<IndexInput> handles = new ArrayList<>();
          for (int i = 0; i < QUEUE_DEPTH * 2; i++) {
            handles.add(in.prefetchRange("abandoned", (long) i * 64, 64));
          }
          IOUtils.close(handles);
        }
        assertTrue(
            "leaked rings across abandoned batches", dir.getEngine().ringCount() <= MAX_RINGS);

        IndexInput handle = in.prefetchRange("after", 0, 64);
        assertNotNull(handle);
        try (handle) {
          byte[] actual = new byte[64];
          handle.readBytes(actual, 0, 64);
        }
      }
    }
  }

  /**
   * Closing handles that were never read leaves their reads in flight, and the ring goes back to
   * the pool dirty. Closing the directory then has to wait for those reads before releasing the
   * ring. Whether any read is still in flight at that point depends on timing, so this checks that
   * teardown completes cleanly rather than the ordering itself.
   */
  public void testCloseDirectoryWithReadsInFlight() throws Exception {
    Path path = createTempDir("uring-close-inflight");
    IOUringDirectory dir = open(path, 64);
    writeFile(dir, "test.vec", 1 << 20);
    IndexInput in = dir.openInput("test.vec", IOContext.DEFAULT);
    List<IndexInput> handles = new ArrayList<>();
    for (int i = 0; i < 64; i++) {
      handles.add(in.prefetchRange("abandoned", (long) i * 4096, 4096));
    }
    // Reading one submits all of them without waiting for the rest.
    handles.get(0).readByte();
    IOUtils.close(handles);
    in.close();
    dir.close();
    assertEquals("rings left after close", 0, dir.getEngine().ringCount());
  }

  /** A thread that ends without closing its handles does not leak its ring. */
  public void testRingFreedWhenThreadEndsWithoutClosing() throws Exception {
    Path path = createTempDir("uring-gc");
    try (IOUringDirectory dir = open(path)) {
      writeFile(dir, "test.vec", 4096);
      try (IndexInput in = dir.openInput("test.vec", IOContext.DEFAULT)) {
        Thread t =
            new Thread(
                () -> {
                  try {
                    for (int i = 0; i < 4; i++) {
                      in.prefetchRange("leaked", (long) i * 64, 64);
                    }
                  } catch (IOException e) {
                    throw new AssertionError(e);
                  }
                });
        t.start();
        t.join();
        assertEquals(1, dir.getEngine().ringCount());

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (dir.getEngine().ringCount() != 0 && System.nanoTime() < deadline) {
          System.gc();
          LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(50));
        }
        assertEquals("ring of the finished thread was not freed", 0, dir.getEngine().ringCount());
      }
    }
  }

  /** Closing the input releases its descriptor. */
  public void testDescriptorLifecycle() throws Exception {
    Path path = createTempDir("uring-fds");
    try (IOUringDirectory dir = open(path)) {
      writeFile(dir, "test.vec", 1024);
      assertEquals(0, dir.getEngine().openFdCount());
      IndexInput in = dir.openInput("test.vec", IOContext.DEFAULT);
      assertEquals(1, dir.getEngine().openFdCount());
      // Clones and slices share the descriptor and must not close it.
      in.clone().close();
      in.slice("s", 0, 512).close();
      assertEquals(1, dir.getEngine().openFdCount());
      in.close();
      assertEquals(0, dir.getEngine().openFdCount());
    }
  }

  /** Only the configured extensions are served; others use the default. */
  public void testOnlyConfiguredExtensionsAreIntercepted() throws Exception {
    Path path = createTempDir("uring-ext");
    try (IOUringDirectory dir = open(path)) {
      writeFile(dir, "test.dat", 1024);
      try (IndexInput in = dir.openInput("test.dat", IOContext.DEFAULT)) {
        assertNull(
            "an uninteresting extension should not be served by the ring",
            in.prefetchRange("range", 0, 64));
      }
      assertEquals("no descriptor should have been opened", 0, dir.getEngine().openFdCount());
    }
  }

  /** Merge contexts use the default. */
  public void testMergeContextIsNotIntercepted() throws Exception {
    Path path = createTempDir("uring-merge");
    try (IOUringDirectory dir = open(path)) {
      writeFile(dir, "test.vec", 1024);
      IOContext merge = IOContext.merge(new MergeInfo(1, 1024, false, 1));
      try (IndexInput in = dir.openInput("test.vec", merge)) {
        assertNull(in.prefetchRange("range", 0, 64));
      }
    }
  }

  /** A disabled directory is a pass-through. */
  public void testDisabledIsPassThrough() throws Exception {
    Path path = createTempDir("uring-off");
    try (IOUringDirectory dir =
        new IOUringDirectory(
            FSDirectory.open(path), Set.of("vec"), QUEUE_DEPTH, MAX_RINGS, /* enable= */ false)) {
      assertFalse(dir.isEnabled());
      writeFile(dir, "test.vec", 1024);
      try (IndexInput in = dir.openInput("test.vec", IOContext.DEFAULT)) {
        assertNull(in.prefetchRange("range", 0, 64));
        byte[] actual = new byte[64];
        in.readBytes(actual, 0, 64);
      }
    }
  }

  /** Out-of-range ranges are rejected. */
  public void testRangePastEOFThrows() throws Exception {
    Path path = createTempDir("uring-eof");
    try (IOUringDirectory dir = open(path)) {
      writeFile(dir, "test.vec", 1024);
      try (IndexInput in = dir.openInput("test.vec", IOContext.DEFAULT)) {
        expectThrows(EOFException.class, () -> in.prefetchRange("past-eof", 1000, 100));
        expectThrows(EOFException.class, () -> in.prefetchRange("past-eof", 1024, 1));
        expectThrows(EOFException.class, () -> in.prefetchRange("negative", -1, 8));
      }
    }
  }

  /** Reading past the end of a handle is an EOF. */
  public void testReadPastEndOfHandleThrows() throws Exception {
    Path path = createTempDir("uring-handle-eof");
    try (IOUringDirectory dir = open(path)) {
      writeFile(dir, "test.vec", 1024);
      try (IndexInput in = dir.openInput("test.vec", IOContext.DEFAULT)) {
        IndexInput handle = in.prefetchRange("range", 0, 32);
        try (handle) {
          byte[] tooMuch = new byte[33];
          expectThrows(EOFException.class, () -> handle.readBytes(tooMuch, 0, 33));
        }
      }
    }
  }

  private static float[] expectedFloats(byte[] content, int offset, int count) {
    float[] out = new float[count];
    for (int i = 0; i < count; i++) {
      int bits =
          (content[offset + i * 4] & 0xFF)
              | ((content[offset + i * 4 + 1] & 0xFF) << 8)
              | ((content[offset + i * 4 + 2] & 0xFF) << 16)
              | ((content[offset + i * 4 + 3] & 0xFF) << 24);
      out[i] = Float.intBitsToFloat(bits);
    }
    return out;
  }
}
