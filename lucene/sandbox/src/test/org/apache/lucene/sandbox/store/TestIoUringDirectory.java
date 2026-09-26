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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.MergeInfo;
import org.apache.lucene.store.VectorBatch;
import org.apache.lucene.store.VectorBatchCapable;
import org.apache.lucene.tests.store.BaseDirectoryTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IOUtils;

/**
 * Tests {@link IoUringDirectory}, including the whole {@link Directory} contract inherited from
 * {@link BaseDirectoryTestCase}. The io_uring cases run only where io_uring, {@code liburing-ffi}
 * and O_DIRECT are all available; everything else falls back to the delegate and is still
 * exercised.
 */
public class TestIoUringDirectory extends BaseDirectoryTestCase {

  private static final int DIM = 1024;
  private static final int BLK = DIM * Float.BYTES; // 4096: one aligned O_DIRECT block per vector

  @Override
  protected Directory getDirectory(Path path) throws IOException {
    return new IoUringDirectory(FSDirectory.open(path), 256, IoUring.isAvailable());
  }

  /** Opens a directory whose {@code .vec} input must expose the batch capability, or skips. */
  private IoUringDirectory ioUringDir(Path path) throws IOException {
    assumeTrue("io_uring and liburing-ffi available", IoUring.isAvailable());
    IoUringDirectory dir = new IoUringDirectory(FSDirectory.open(path), 256, true);
    assumeTrue("io_uring enabled on this platform", dir.isIoUringEnabled());
    return dir;
  }

  /** Writes {@code count} random vectors back to back, returning them. */
  private float[][] writeVectors(Directory dir, String name, int count) throws IOException {
    float[][] vectors = new float[count][DIM];
    try (IndexOutput out = dir.createOutput(name, IOContext.DEFAULT)) {
      for (int i = 0; i < count; i++) {
        ByteBuffer bb = ByteBuffer.allocate(BLK).order(ByteOrder.LITTLE_ENDIAN);
        for (int d = 0; d < DIM; d++) {
          vectors[i][d] = random().nextFloat();
          bb.putFloat(vectors[i][d]);
        }
        out.writeBytes(bb.array(), 0, BLK);
      }
    }
    return vectors;
  }

  /** Reads a shortlist from one input the only way the API offers: queue it, then execute. */
  private static void readVia(IndexInput in, long[] positions, int dim, int count, float[] out)
      throws IOException {
    VectorBatch batch = ((VectorBatchCapable) in).newBatch();
    assertNotNull(batch);
    assertTrue(batch.add(in, positions, dim, count, out));
    batch.execute();
  }

  private static void assertVector(float[] expected, float[] actual, int i, int dim) {
    assertArrayEquals(
        "vector " + i, expected, ArrayUtil.copyOfSubArray(actual, i * dim, i * dim + dim), 0f);
  }

  /**
   * Batch reads must return exactly what was written, for randomized shortlist sizes and orders,
   * including shortlists longer than the queue depth (multi-batch) and unaligned positions.
   */
  public void testBatchReadsMatchWritten() throws Exception {
    Path path = createTempDir("ioUringBatch");
    try (IoUringDirectory dir = ioUringDir(path)) {
      int count = TEST_NIGHTLY ? atLeast(600) : atLeast(50);
      float[][] expected = writeVectors(dir, "data.vec", count);
      try (IndexInput in = dir.openInput("data.vec", IOContext.DEFAULT)) {
        assumeTrue("O_DIRECT engaged", in instanceof VectorBatchCapable);

        // Empty batch is a no-op.
        readVia(in, new long[0], DIM, 0, new float[0]);

        // Random shortlist: random size, random (possibly repeated, unordered) vectors.
        int n = TestUtil.nextInt(random(), 1, count);
        long[] positions = new long[n];
        int[] pick = new int[n];
        for (int i = 0; i < n; i++) {
          pick[i] = random().nextInt(count);
          positions[i] = (long) pick[i] * BLK;
        }
        float[] actual = new float[n * DIM];
        readVia(in, positions, DIM, n, actual);
        for (int i = 0; i < n; i++) {
          assertVector(expected[pick[i]], actual, i, DIM);
        }
      }
    }
  }

  /** A position that is not a multiple of the block size must still return the right bytes. */
  public void testUnalignedPositions() throws Exception {
    Path path = createTempDir("ioUringUnaligned");
    try (IoUringDirectory dir = ioUringDir(path)) {
      // Prepend a few bytes so every vector afterwards starts unaligned.
      int skew = TestUtil.nextInt(random(), 1, BLK - 1);
      float[] vector = new float[DIM];
      try (IndexOutput out = dir.createOutput("data.vec", IOContext.DEFAULT)) {
        out.writeBytes(new byte[skew], skew);
        ByteBuffer bb = ByteBuffer.allocate(BLK).order(ByteOrder.LITTLE_ENDIAN);
        for (int d = 0; d < DIM; d++) {
          vector[d] = random().nextFloat();
          bb.putFloat(vector[d]);
        }
        out.writeBytes(bb.array(), 0, BLK);
      }
      try (IndexInput in = dir.openInput("data.vec", IOContext.DEFAULT)) {
        assumeTrue("O_DIRECT engaged", in instanceof VectorBatchCapable);
        float[] actual = new float[DIM];
        readVia(in, new long[] {skew}, DIM, 1, actual);
        assertArrayEquals(vector, actual, 0f);
      }
    }
  }

  /** io_uring and the plain mmap path must agree, so the fallback is not silently different. */
  public void testAgreesWithMMap() throws Exception {
    Path uringPath = createTempDir("ioUringAgree");
    Path mmapPath = createTempDir("mmapAgree");
    int count = atLeast(20);
    try (IoUringDirectory uring = ioUringDir(uringPath);
        Directory mmap = new MMapDirectory(mmapPath)) {
      float[][] expected = writeVectors(uring, "data.vec", count);
      try (IndexOutput out = mmap.createOutput("data.vec", IOContext.DEFAULT)) {
        ByteBuffer bb = ByteBuffer.allocate(count * BLK).order(ByteOrder.LITTLE_ENDIAN);
        for (float[] v : expected) {
          for (float f : v) {
            bb.putFloat(f);
          }
        }
        out.writeBytes(bb.array(), 0, bb.capacity());
      }
      long[] positions = new long[count];
      for (int i = 0; i < count; i++) {
        positions[i] = (long) i * BLK;
      }
      float[] viaUring = new float[count * DIM];
      try (IndexInput in = uring.openInput("data.vec", IOContext.DEFAULT)) {
        assumeTrue("O_DIRECT engaged", in instanceof VectorBatchCapable);
        readVia(in, positions, DIM, count, viaUring);
      }
      float[] viaMMap = new float[count * DIM];
      try (IndexInput in = mmap.openInput("data.vec", IOContext.DEFAULT)) {
        for (int i = 0; i < count; i++) {
          in.seek(positions[i]);
          in.readFloats(viaMMap, i * DIM, DIM);
        }
      }
      assertArrayEquals(viaMMap, viaUring, 0f);
    }
  }

  /** Concurrent readers share the bounded ring pool; every one must still read correct data. */
  public void testConcurrentBatchReads() throws Exception {
    Path path = createTempDir("ioUringConcurrent");
    int threads = TestUtil.nextInt(random(), 2, 8);
    // Fewer rings than threads, so borrowers have to wait for and reuse rings.
    try (FSDirectory fs = FSDirectory.open(path);
        IoUringDirectory dir = new IoUringDirectory(fs, 64, IoUring.isAvailable(), 2)) {
      assumeTrue("io_uring enabled", dir.isIoUringEnabled());
      int count = atLeast(30);
      float[][] expected = writeVectors(dir, "data.vec", count);
      try (IndexInput in = dir.openInput("data.vec", IOContext.DEFAULT)) {
        assumeTrue("O_DIRECT engaged", in instanceof VectorBatchCapable);
        CountDownLatch start = new CountDownLatch(1);
        List<Thread> workers = new ArrayList<>();
        List<Throwable> failures = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
          Thread worker =
              new Thread(
                  () -> {
                    try {
                      start.await();
                      for (int iter = 0; iter < 10; iter++) {
                        long[] positions = new long[count];
                        for (int i = 0; i < count; i++) {
                          positions[i] = (long) i * BLK;
                        }
                        float[] actual = new float[count * DIM];
                        readVia(in, positions, DIM, count, actual);
                        for (int i = 0; i < count; i++) {
                          assertVector(expected[i], actual, i, DIM);
                        }
                      }
                    } catch (Throwable e) {
                      synchronized (failures) {
                        failures.add(e);
                      }
                    }
                  });
          worker.start();
          workers.add(worker);
        }
        start.countDown();
        for (Thread worker : workers) {
          worker.join();
        }
        synchronized (failures) {
          if (failures.isEmpty() == false) {
            throw new AssertionError("concurrent batch reads failed", failures.get(0));
          }
        }
      }
    }
  }

  /** Closing the directory releases its rings, and a fresh directory over the same files works. */
  public void testReopenAfterClose() throws Exception {
    Path path = createTempDir("ioUringReopen");
    int count = atLeast(10);
    float[][] expected;
    try (IoUringDirectory dir = ioUringDir(path)) {
      expected = writeVectors(dir, "data.vec", count);
    }
    try (IoUringDirectory dir = ioUringDir(path);
        IndexInput in = dir.openInput("data.vec", IOContext.DEFAULT)) {
      assumeTrue("O_DIRECT engaged", in instanceof VectorBatchCapable);
      long[] positions = new long[count];
      for (int i = 0; i < count; i++) {
        positions[i] = (long) i * BLK;
      }
      float[] actual = new float[count * DIM];
      readVia(in, positions, DIM, count, actual);
      for (int i = 0; i < count; i++) {
        assertVector(expected[i], actual, i, DIM);
      }
    }
  }

  /**
   * A failed batch must not poison later ones. Reading past EOF makes the kernel return a short
   * read, which aborts mid-batch and leaves submissions unreaped; if that ring were handed back to
   * the pool its late completions would be decoded against the next caller's request array. So the
   * next read on the same directory must still return exactly the right vectors.
   */
  public void testFailedBatchDoesNotCorruptLaterReads() throws Exception {
    Path path = createTempDir("ioUringAfterError");
    int count = atLeast(8);
    // maxRings=1: if the failed ring were recycled, the next read is guaranteed to get it.
    try (FSDirectory fs = FSDirectory.open(path);
        IoUringDirectory dir = new IoUringDirectory(fs, 64, IoUring.isAvailable(), 1)) {
      assumeTrue("io_uring enabled", dir.isIoUringEnabled());
      float[][] expected = writeVectors(dir, "data.vec", count);
      try (IndexInput in = dir.openInput("data.vec", IOContext.DEFAULT)) {
        assumeTrue("O_DIRECT engaged", in instanceof VectorBatchCapable);
        long fileLength = in.length();

        // Ask for a vector that starts at EOF: nothing to read, so the batch must fail.
        expectThrows(
            IOException.class, () -> readVia(in, new long[] {fileLength}, DIM, 1, new float[DIM]));

        // The ring that served the failed batch must have been destroyed, not returned to the pool:
        // this is the assertion that pins the fix, since a recycled ring only misbehaves when
        // submissions were left unreaped, which a black-box read cannot force deterministically.
        assertEquals("failed read must not leave its ring in the pool", 0, dir.ringCount());

        // And a good request afterwards still has to be served correctly.
        long[] positions = new long[count];
        for (int i = 0; i < count; i++) {
          positions[i] = (long) i * BLK;
        }
        float[] actual = new float[count * DIM];
        readVia(in, positions, DIM, count, actual);
        for (int i = 0; i < count; i++) {
          assertVector(expected[i], actual, i, DIM);
        }
      }
    }
  }

  /**
   * Merges stream {@code .vec} front to back, where batching earns nothing and O_DIRECT costs
   * read-ahead, so a merge-context open must be left to the delegate.
   */
  public void testMergeContextIsNotServedByIoUring() throws Exception {
    Path path = createTempDir("ioUringMerge");
    try (IoUringDirectory dir = ioUringDir(path)) {
      writeVectors(dir, "data.vec", 4);
      IOContext mergeContext = IOContext.merge(new MergeInfo(1, 1024L * 1024, false, 1));
      try (IndexInput in = dir.openInput("data.vec", mergeContext)) {
        assertFalse("merge reads must not go through io_uring", in instanceof VectorBatchCapable);
      }
      // ... while a search-context open on the very same file still does.
      try (IndexInput in = dir.openInput("data.vec", IOContext.DEFAULT)) {
        assumeTrue("O_DIRECT engaged", in instanceof VectorBatchCapable);
      }
    }
  }

  /**
   * Vectors whose length is not one block exercise the multi-block span arithmetic (and the reduced
   * number of slots per submission that follows from it), which a fixed dim=1024 test never
   * touches.
   */
  public void testDimensionsSpanningSeveralBlocks() throws Exception {
    for (int dim : new int[] {1, 512, 1025, 2048, 3000}) {
      Path path = createTempDir("ioUringDim" + dim);
      int count = TestUtil.nextInt(random(), 2, 12);
      try (IoUringDirectory dir = ioUringDir(path)) {
        float[][] expected = new float[count][dim];
        long[] positions = new long[count];
        try (IndexOutput out = dir.createOutput("data.vec", IOContext.DEFAULT)) {
          for (int i = 0; i < count; i++) {
            positions[i] = out.getFilePointer();
            ByteBuffer bb = ByteBuffer.allocate(dim * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
            for (int d = 0; d < dim; d++) {
              expected[i][d] = random().nextFloat();
              bb.putFloat(expected[i][d]);
            }
            out.writeBytes(bb.array(), 0, bb.capacity());
          }
        }
        try (IndexInput in = dir.openInput("data.vec", IOContext.DEFAULT)) {
          assumeTrue("O_DIRECT engaged", in instanceof VectorBatchCapable);
          float[] actual = new float[count * dim];
          readVia(in, positions, dim, count, actual);
          for (int i = 0; i < count; i++) {
            assertVector(expected[i], actual, i, dim);
          }
        }
      }
    }
  }

  /**
   * The O_DIRECT fd belongs to the {@link IndexInput}, so repeatedly opening and closing inputs —
   * as happens when segments are merged away and readers are reopened — must not accumulate fds.
   */
  public void testInputsDoNotLeakFileDescriptors() throws Exception {
    Path path = createTempDir("ioUringFds");
    try (IoUringDirectory dir = ioUringDir(path)) {
      float[][] expected = writeVectors(dir, "data.vec", 2);
      assertEquals("no input open yet", 0, dir.openFdCount());

      // Counting descriptors directly, rather than looping until the process limit is hit: with a
      // high ulimit a leak would simply go unnoticed, so the test would pass either way.
      IndexInput first = dir.openInput("data.vec", IOContext.DEFAULT);
      assumeTrue("O_DIRECT engaged", first instanceof VectorBatchCapable);
      assertEquals("an open input holds exactly one descriptor", 1, dir.openFdCount());
      try (IndexInput second = dir.openInput("data.vec", IOContext.DEFAULT)) {
        assertEquals("descriptors are per input, not per file", 2, dir.openFdCount());
        // A clone shares the original's descriptor and must not open or close one of its own.
        second.clone();
        assertEquals("clone must not open a descriptor", 2, dir.openFdCount());
      }
      assertEquals("closing an input releases its descriptor", 1, dir.openFdCount());
      first.close();
      assertEquals(0, dir.openFdCount());

      // Repeated open/read/close, as happens when segments are merged away and readers reopen,
      // must leave nothing behind.
      for (int i = 0; i < 50; i++) {
        try (IndexInput in = dir.openInput("data.vec", IOContext.DEFAULT)) {
          float[] actual = new float[DIM];
          readVia(in, new long[] {0L}, DIM, 1, actual);
          assertVector(expected[0], actual, 0, DIM);
        }
      }
      assertEquals("descriptors leaked across open/close cycles", 0, dir.openFdCount());
    }
  }

  /**
   * The point of the batch: reads for <em>several</em> files go out together. Each file gets its
   * own descriptor, so a batch that mixed up which descriptor served which request would return
   * another file's bytes — the failure that would silently corrupt a rerank across segments.
   */
  public void testBatchSpansMultipleFiles() throws Exception {
    Path path = createTempDir("ioUringMultiFile");
    int files = TestUtil.nextInt(random(), 2, 6);
    try (IoUringDirectory dir = ioUringDir(path)) {
      float[][][] expected = new float[files][][];
      List<IndexInput> inputs = new ArrayList<>();
      try {
        for (int f = 0; f < files; f++) {
          expected[f] = writeVectors(dir, "seg" + f + ".vec", 3 + f);
          IndexInput in = dir.openInput("seg" + f + ".vec", IOContext.DEFAULT);
          inputs.add(in);
          assumeTrue("O_DIRECT engaged", in instanceof VectorBatchCapable);
        }
        VectorBatch batch = ((VectorBatchCapable) inputs.get(0)).newBatch();
        assertNotNull("io_uring must support cross-input batching", batch);

        // Queue every file's vectors, in an interleaved order, before anything is read.
        float[][] out = new float[files][];
        for (int f = files - 1; f >= 0; f--) {
          int count = expected[f].length;
          long[] positions = new long[count];
          for (int i = 0; i < count; i++) {
            positions[i] = (long) i * BLK;
          }
          out[f] = new float[count * DIM];
          assertTrue(
              "every input of this directory belongs in the batch",
              batch.add(inputs.get(f), positions, DIM, count, out[f]));
        }
        batch.execute();
        for (int f = 0; f < files; f++) {
          for (int i = 0; i < expected[f].length; i++) {
            assertVector(expected[f][i], out[f], i, DIM);
          }
        }

        // A batch is reusable after executing.
        long[] one = new long[] {0L};
        float[] again = new float[DIM];
        assertTrue(batch.add(inputs.get(0), one, DIM, 1, again));
        batch.execute();
        assertVector(expected[0][0], again, 0, DIM);
      } finally {
        IOUtils.close(inputs);
      }
    }
  }

  /**
   * A shortlist arrives as many small per-segment chunks, so the batch grows repeatedly. Its
   * parallel arrays hold different element widths and therefore reach different lengths when grown,
   * so each has to be sized on its own.
   */
  public void testBatchGrowsAcrossManySmallAdds() throws Exception {
    Path path = createTempDir("ioUringGrow");
    int count = 400; // well past the batch's initial capacity, over several growths
    try (IoUringDirectory dir = ioUringDir(path)) {
      float[][] expected = writeVectors(dir, "data.vec", count);
      try (IndexInput in = dir.openInput("data.vec", IOContext.DEFAULT)) {
        assumeTrue("O_DIRECT engaged", in instanceof VectorBatchCapable);
        VectorBatch batch = ((VectorBatchCapable) in).newBatch();
        assertNotNull(batch);
        // Chunks of 3 so the growth path runs many times rather than once.
        List<float[]> chunks = new ArrayList<>();
        for (int start = 0; start < count; start += 3) {
          int len = Math.min(3, count - start);
          long[] positions = new long[len];
          float[] chunk = new float[len * DIM];
          for (int i = 0; i < len; i++) {
            positions[i] = (long) (start + i) * BLK;
          }
          assertTrue(batch.add(in, positions, DIM, len, chunk));
          chunks.add(chunk);
        }
        batch.execute(); // nothing was read until here; the chunk arrays are filled now
        int vec = 0;
        for (float[] chunk : chunks) {
          for (int i = 0; i < chunk.length / DIM; i++) {
            assertVector(expected[vec++], chunk, i, DIM);
          }
        }
        assertEquals(count, vec);
      }
    }
  }

  /** An input from another directory has a different engine, so the batch must refuse it. */
  public void testBatchRefusesForeignInput() throws Exception {
    Path a = createTempDir("ioUringA");
    Path b = createTempDir("ioUringB");
    try (IoUringDirectory dirA = ioUringDir(a);
        IoUringDirectory dirB = ioUringDir(b)) {
      writeVectors(dirA, "a.vec", 2);
      writeVectors(dirB, "b.vec", 2);
      try (IndexInput inA = dirA.openInput("a.vec", IOContext.DEFAULT);
          IndexInput inB = dirB.openInput("b.vec", IOContext.DEFAULT)) {
        assumeTrue("O_DIRECT engaged", inA instanceof VectorBatchCapable);
        VectorBatch batch = ((VectorBatchCapable) inA).newBatch();
        assertFalse(
            "an input from another directory must be refused, not silently mis-read",
            batch.add(inB, new long[] {0L}, DIM, 1, new float[DIM]));
      }
    }
  }

  /** With io_uring off the directory is a pass-through and exposes no batch capability. */
  public void testDisabledIsPassThrough() throws Exception {
    Path path = createTempDir("ioUringOff");
    try (FSDirectory fs = FSDirectory.open(path);
        IoUringDirectory dir = new IoUringDirectory(fs, 256, false)) {
      assertFalse(dir.isIoUringEnabled());
      try (IndexOutput out = dir.createOutput("data.vec", IOContext.DEFAULT)) {
        out.writeBytes(new byte[BLK], BLK);
      }
      try (IndexInput in = dir.openInput("data.vec", IOContext.DEFAULT)) {
        assertFalse(
            "disabled directory must not expose io_uring", in instanceof VectorBatchCapable);
      }
    }
  }
}
