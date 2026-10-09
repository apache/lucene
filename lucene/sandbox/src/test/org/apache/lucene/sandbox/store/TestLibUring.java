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

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import org.apache.lucene.tests.util.LuceneTestCase;

/** Exercises the {@link LibUring} bindings against a real ring. Skipped without io_uring. */
public class TestLibUring extends LuceneTestCase {

  @Override
  public void setUp() throws Exception {
    super.setUp();
    assumeTrue("io_uring unavailable on this platform", LibUring.isAvailable());
  }

  /** A ring initialises and tears down. */
  public void testQueueInitAndExit() {
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment ring = arena.allocate(LibUring.RING_BYTES, 8);
      assertEquals("io_uring_queue_init", 0, LibUring.queueInit(8, ring, 0));
      LibUring.queueExit(ring);
    }
  }

  /** The full probe: bytes read back and compared, {@code user_data} round-tripped. */
  public void testProbeReadsBytesBack() throws Exception {
    Path dir = createTempDir("uring-probe");
    LibUring.Support support = LibUring.probe(dir);
    assertTrue("probe failed although a ring initialises", support.available());
    // DONTCACHE support is not asserted: it needs Linux 6.14+ and filesystem support.
    if (VERBOSE) {
      System.out.println("RWF_DONTCACHE supported: " + support.dontcache());
    }
  }

  /**
   * Asserts that {@code RWF_DONTCACHE} is honoured on a filesystem chosen by the person running the
   * test. Skipped unless {@code LUCENE_URING_SCRATCH_DIR} names a writable directory on a
   * filesystem that supports it, such as ext4 or xfs.
   */
  public void testDontcacheOnChosenFilesystem() throws Exception {
    String scratch = System.getenv("LUCENE_URING_SCRATCH_DIR");
    assumeTrue(
        "set LUCENE_URING_SCRATCH_DIR to a directory on ext4 or xfs to run this", scratch != null);
    Path dir = Path.of(scratch);
    assumeTrue(scratch + " is not a writable directory", Files.isWritable(dir));

    LibUring.Support support = LibUring.probe(dir);
    assertTrue("io_uring unusable in " + scratch, support.available());
    assertTrue(
        "RWF_DONTCACHE not honoured in " + scratch + ", check the filesystem type",
        support.dontcache());
  }

  /** Submits more reads than the ring has submission slots, freeing each slot as it is reaped. */
  public void testMoreReadsThanQueueDepth() throws Exception {
    final int qd = 8;
    final int reads = 64;
    final int chunk = 512;

    Path tmp = createTempDir("uring-depth");
    // Whatever flags this filesystem supports.
    final int rwFlags = LibUring.probe(tmp).readFlags();

    Path file = tmp.resolve("data");
    byte[] content = new byte[reads * chunk];
    for (int i = 0; i < content.length; i++) {
      content[i] = (byte) (i * 31 + 7);
    }
    Files.write(file, content);

    try (Arena arena = Arena.ofConfined()) {
      MemorySegment ring = arena.allocate(LibUring.RING_BYTES, 8);
      assertEquals(0, LibUring.queueInit(qd, ring, 0));
      int fd = LibUring.open(arena.allocateFrom(file.toString()), LibUring.O_RDONLY);
      assertTrue("open failed", fd >= 0);
      try {
        // One buffer per read.
        MemorySegment[] bufs = new MemorySegment[reads];
        for (int i = 0; i < reads; i++) {
          bufs[i] = arena.allocate(chunk);
        }
        MemorySegment cqePtrs = arena.allocate((long) qd * ADDRESS.byteSize());
        boolean[] done = new boolean[reads];

        int inFlight = 0;
        int completed = 0;
        for (int i = 0; i < reads; i++) {
          // Ring full: reap a completion to free a slot.
          while (inFlight == qd) {
            int rc = LibUring.submitAndWait(ring, 1);
            assertTrue("submit_and_wait=" + rc, rc >= 0 || rc == LibUring.NEG_EINTR);
            int n = reap(ring, cqePtrs, qd, chunk, done);
            inFlight -= n;
            completed += n;
          }
          MemorySegment sqe = LibUring.getSqe(ring);
          if (MemorySegment.NULL.equals(sqe)) {
            // Submission queue full: submit to free every SQE.
            assertTrue(LibUring.submit(ring) >= 0);
            sqe = LibUring.getSqe(ring);
            assertFalse("no SQE even after submit", MemorySegment.NULL.equals(sqe));
          }
          LibUring.prepRead(sqe, fd, bufs[i], chunk, (long) i * chunk);
          LibUring.setRwFlags(sqe, rwFlags);
          LibUring.setData64(sqe, i);
          inFlight++;
        }
        while (completed < reads) {
          int rc = LibUring.submitAndWait(ring, 1);
          assertTrue("submit_and_wait=" + rc, rc >= 0 || rc == LibUring.NEG_EINTR);
          completed += reap(ring, cqePtrs, qd, chunk, done);
        }

        for (int i = 0; i < reads; i++) {
          assertTrue("read " + i + " never completed", done[i]);
          byte[] actual = new byte[chunk];
          MemorySegment.copy(bufs[i], JAVA_BYTE, 0, actual, 0, chunk);
          assertArrayEquals(
              "chunk " + i, Arrays.copyOfRange(content, i * chunk, (i + 1) * chunk), actual);
        }
      } finally {
        LibUring.close(fd);
        LibUring.queueExit(ring);
      }
    }
  }

  /** Reaps the completions that have landed, marking each read done. Returns how many. */
  private static int reap(
      MemorySegment ring, MemorySegment cqePtrs, int max, int expectedBytes, boolean[] done) {
    int n = LibUring.peekBatchCqe(ring, cqePtrs, max);
    for (int i = 0; i < n; i++) {
      MemorySegment cqe = LibUring.cqeAt(cqePtrs, i);
      int index = (int) LibUring.cqeData(cqe);
      int res = LibUring.cqeResult(cqe);
      assertTrue("read " + index + " failed errno=" + (-res), res >= 0);
      assertEquals("short read at " + index, expectedBytes, res);
      assertFalse("duplicate completion for " + index, done[index]);
      done[index] = true;
    }
    if (n > 0) {
      LibUring.cqAdvance(ring, n);
    }
    return n;
  }
}
