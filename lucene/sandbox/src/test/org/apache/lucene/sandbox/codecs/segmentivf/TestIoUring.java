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

import static org.apache.lucene.sandbox.codecs.segmentivf.IoUring.PAGE_BYTES;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.ArrayUtil;

/** Tests of {@link IoUring} itself. io_uring is Linux-only, so elsewhere these are skipped. */
public class TestIoUring extends LuceneTestCase {

  private static final int EBADF = 9;

  /** Writes {@code bytes} to a new file and opens it, or skips the test without io_uring. */
  private Path writeFile(byte[] bytes) throws IOException {
    Path file = createTempDir().resolve("data");
    Files.write(file, bytes);
    return file;
  }

  private static IoUring openOrSkip(Path file) {
    try {
      return IoUring.open(file, false);
    } catch (IOException e) {
      assumeTrue("io_uring is unavailable here: " + e.getMessage(), false);
      throw new AssertionError("unreachable");
    }
  }

  private static byte[] page(byte[] bytes, long position) {
    return ArrayUtil.copyOfSubArray(bytes, (int) position, (int) position + PAGE_BYTES);
  }

  private static IndexInput input(byte[] bytes) {
    return new ByteBuffersIndexInput(
        new ByteBuffersDataInput(List.of(ByteBuffer.wrap(bytes))), "served");
  }

  /**
   * io_uring reads the file underneath every FilterDirectory, so FineCodeIO uses it only when it
   * reads the bytes the directory serves; a directory that transforms them (here, served bytes that
   * differ from the file's) is read through the directory instead.
   */
  public void testReadsSameBytesComparesWithTheDirectory() throws IOException {
    byte[] bytes = new byte[PAGE_BYTES + 100];
    random().nextBytes(bytes);
    try (IoUring ioUring = openOrSkip(writeFile(bytes))) {
      byte[] transformed = bytes.clone();
      transformed[7] ^= 0x5A;
      assertTrue(FineCodeIO.readsSameBytes(ioUring, input(bytes)));
      assertFalse(FineCodeIO.readsSameBytes(ioUring, input(transformed)));
    }
  }

  /**
   * libc's syscall returns -1 and sets errno; the wrapper must hand back -errno, as the EINTR
   * checks and error messages expect. Before the fix it returned -1, which reads as errno 1.
   */
  public void testSyscallReturnsNegatedErrno() throws Throwable {
    IoUring ioUring = openOrSkip(writeFile(new byte[PAGE_BYTES]));
    ioUring.close();
    long ret = IoUring.syscall(IoUring.SYS_IO_URING_ENTER, -1, 0, 0, 0);
    assertEquals(-EBADF, ret);
  }

  /**
   * A second close does nothing. It used to close the descriptor number again, which by then may
   * belong to another file (here, most likely the next instance's), and to miscount open instances.
   */
  public void testCloseIsIdempotent() throws Exception {
    byte[] first = new byte[PAGE_BYTES];
    byte[] second = new byte[2 * PAGE_BYTES];
    random().nextBytes(first);
    random().nextBytes(second);
    IoUring a = openOrSkip(writeFile(first));
    a.close();
    // The lowest free descriptor is reused, so b most likely gets a's old number.
    IoUring b = openOrSkip(writeFile(second));
    try {
      a.close();
      MemorySegment read =
          b.readBatch(
              new long[] {PAGE_BYTES}, PAGE_BYTES, IoUring.buffer(MemorySegment.NULL, PAGE_BYTES));
      assertArrayEquals(
          page(second, PAGE_BYTES), read.asSlice(0, PAGE_BYTES).toArray(ValueLayout.JAVA_BYTE));
      // b is still open, so the rings it used stay pooled; a's second close must not free them.
      assertTrue(IoUring.idleRings() > 0);
    } finally {
      b.close();
      b.close();
    }
    assertEquals(0, IoUring.idleRings());
  }

  /** Opening sets up the ring pool, so searches take rings already made. */
  public void testOpenFillsTheRingPool() throws Exception {
    IoUring ioUring = openOrSkip(writeFile(new byte[PAGE_BYTES]));
    try {
      assertTrue(
          IoUring.idleRings() + " idle rings, want " + IoUring.MAX_IDLE,
          IoUring.idleRings() >= IoUring.MAX_IDLE);
    } finally {
      ioUring.close();
    }
    assertEquals(0, IoUring.idleRings());
  }

  /**
   * One batch reads several files, each read naming its own, as a cross-segment rerank reads every
   * segment's share: reads alternate between two files, across more than one ring, and each lands
   * the page of its own file.
   */
  public void testOneBatchReadsSeveralFiles() throws Exception {
    int pages = 8;
    byte[] a = new byte[pages * PAGE_BYTES];
    byte[] b = new byte[pages * PAGE_BYTES];
    random().nextBytes(a);
    random().nextBytes(b);
    IoUring first = openOrSkip(writeFile(a));
    IoUring second = openOrSkip(writeFile(b));
    try {
      int n = IoUring.RING_ENTRIES + 10;
      IoUring[] files = new IoUring[n];
      long[] positions = new long[n];
      for (int k = 0; k < n; k++) {
        files[k] = k % 2 == 0 ? first : second;
        positions[k] = (long) random().nextInt(pages) * PAGE_BYTES;
      }
      MemorySegment buffer = IoUring.buffer(MemorySegment.NULL, (long) n * PAGE_BYTES);
      IoUring.readBatch(files, positions, n, PAGE_BYTES, buffer);
      for (int k = 0; k < n; k++) {
        byte[] expected = page(k % 2 == 0 ? a : b, positions[k]);
        byte[] read =
            buffer.asSlice((long) k * PAGE_BYTES, PAGE_BYTES).toArray(ValueLayout.JAVA_BYTE);
        assertArrayEquals("read " + k, expected, read);
      }
    } finally {
      first.close();
      second.close();
    }
  }

  /**
   * A read that ends the file returns only the bytes up to its end, and succeeds: the last page of
   * a file shorter than whole pages, as a small segment's last fine page is. It used to fail the
   * batch on every query, which then read that page through the mapping. A read starting at or past
   * the end still fails.
   */
  public void testReadEndingTheFileReturnsItsBytes() throws Exception {
    byte[] bytes = new byte[PAGE_BYTES + 100];
    random().nextBytes(bytes);
    IoUring ioUring = openOrSkip(writeFile(bytes));
    try {
      MemorySegment buffer = IoUring.buffer(MemorySegment.NULL, 2 * PAGE_BYTES);
      ioUring.readBatch(new long[] {0, PAGE_BYTES}, PAGE_BYTES, buffer);
      assertArrayEquals(
          page(bytes, 0), buffer.asSlice(0, PAGE_BYTES).toArray(ValueLayout.JAVA_BYTE));
      assertArrayEquals(
          ArrayUtil.copyOfSubArray(bytes, PAGE_BYTES, bytes.length),
          buffer.asSlice(PAGE_BYTES, 100).toArray(ValueLayout.JAVA_BYTE));
      expectThrows(
          IoUring.ReadRefusedException.class,
          () ->
              ioUring.readBatch(
                  new long[] {2 * PAGE_BYTES},
                  PAGE_BYTES,
                  IoUring.buffer(MemorySegment.NULL, PAGE_BYTES)));
    } finally {
      ioUring.close();
    }
  }

  /**
   * A read after close fails instead of submitting a descriptor number that may by then be another
   * file's, and it is not taken for a refusal: io_uring stays usable for later opens.
   */
  public void testReadAfterCloseFailsWithoutRefusingLaterOpens() throws Exception {
    byte[] bytes = new byte[PAGE_BYTES];
    random().nextBytes(bytes);
    IoUring closed = openOrSkip(writeFile(bytes));
    closed.close();
    IOException e =
        expectThrows(
            IOException.class,
            () ->
                closed.readBatch(
                    new long[] {0}, PAGE_BYTES, IoUring.buffer(MemorySegment.NULL, PAGE_BYTES)));
    assertFalse(e instanceof IoUring.ReadRefusedException);
    IoUring reopened = openOrSkip(writeFile(bytes));
    try {
      MemorySegment read =
          reopened.readBatch(
              new long[] {0}, PAGE_BYTES, IoUring.buffer(MemorySegment.NULL, PAGE_BYTES));
      assertArrayEquals(bytes, read.asSlice(0, PAGE_BYTES).toArray(ValueLayout.JAVA_BYTE));
    } finally {
      reopened.close();
    }
  }

  /**
   * Many threads read random pages, in batches that span more than one ring, and check every page.
   * This exercises the completion ring's ordering (acquire of the tail before reading entries) on
   * weakly ordered CPUs: a stale entry would be skipped or misread and fail the batch or the check.
   */
  public void testConcurrentBatchesReadCorrectPages() throws Exception {
    int pages = 64;
    byte[] bytes = new byte[pages * PAGE_BYTES];
    random().nextBytes(bytes);
    IoUring ioUring = openOrSkip(writeFile(bytes));
    try {
      Thread[] threads = new Thread[4];
      long[] seeds = new long[threads.length];
      for (int t = 0; t < threads.length; t++) {
        seeds[t] = random().nextLong();
      }
      Throwable[] failures = new Throwable[threads.length];
      for (int t = 0; t < threads.length; t++) {
        int thread = t;
        threads[t] =
            new Thread(() -> {
              try {
                readAndCheck(ioUring, bytes, pages, new Random(seeds[thread]));
              } catch (Throwable e) {
                failures[thread] = e;
              }
            });
        threads[t].start();
      }
      for (Thread thread : threads) {
        thread.join();
      }
      for (Throwable failure : failures) {
        if (failure != null) {
          throw new AssertionError(failure);
        }
      }
    } finally {
      ioUring.close();
    }
  }

  private static void readAndCheck(IoUring ioUring, byte[] bytes, int pages, Random random)
      throws IOException {
    MemorySegment buffer = MemorySegment.NULL;
    for (int round = 0; round < 50; round++) {
      int n = 1 + random.nextInt(IoUring.RING_ENTRIES + 64);
      long[] positions = new long[n];
      for (int k = 0; k < n; k++) {
        positions[k] = (long) random.nextInt(pages) * PAGE_BYTES;
      }
      buffer = IoUring.buffer(buffer, (long) n * PAGE_BYTES);
      ioUring.readBatch(positions, PAGE_BYTES, buffer);
      for (int k = 0; k < n; k++) {
        MemorySegment slice = buffer.asSlice((long) k * PAGE_BYTES, PAGE_BYTES);
        byte[] read = slice.toArray(ValueLayout.JAVA_BYTE);
        assertArrayEquals("read " + k, page(bytes, positions[k]), read);
      }
    }
  }
}
