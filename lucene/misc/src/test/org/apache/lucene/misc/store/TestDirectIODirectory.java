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
package org.apache.lucene.misc.store;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.OptionalLong;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FlushInfo;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MergeInfo;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.store.BaseDirectoryTestCase;
import org.apache.lucene.util.ArrayUtil;
import org.junit.BeforeClass;

public class TestDirectIODirectory extends BaseDirectoryTestCase {

  @BeforeClass
  public static void checkSupported() throws IOException {
    assumeTrue(
        "This test required a JDK version that has support for ExtendedOpenOption.DIRECT",
        DirectIODirectory.ExtendedOpenOption_DIRECT != null);
    // jdk supports it, let's check that the filesystem does too
    Path path = createTempDir("directIOProbe");
    try (Directory dir = open(path);
        IndexOutput out = dir.createOutput("out", IOContext.DEFAULT)) {
      out.writeString("test");
    } catch (IOException e) {
      assumeNoException("test requires filesystem that supports Direct IO", e);
    }
  }

  private static DirectIODirectory open(Path path) throws IOException {
    return new DirectIODirectory(FSDirectory.open(path)) {
      @Override
      protected boolean useDirectIO(String name, IOContext context, OptionalLong fileLength) {
        return true;
      }
    };
  }

  @Override
  protected DirectIODirectory getDirectory(Path path) throws IOException {
    return open(path);
  }

  public void testIndexWriteRead() throws IOException {
    try (Directory dir = getDirectory(createTempDir("testDirectIODirectory"))) {
      try (RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
        Document doc = new Document();
        Field field = newField("field", "foo bar", TextField.TYPE_STORED);
        doc.add(field);

        iw.addDocument(doc);
        iw.commit();
      }

      try (IndexReader ir = DirectoryReader.open(dir)) {
        IndexSearcher s = newSearcher(ir);
        assertEquals(1, s.count(new PhraseQuery("field", "foo", "bar")));
      }
    }
  }

  public void testIllegalEOFWithFileSizeMultipleOfBlockSize() throws Exception {
    Path path = createTempDir("testIllegalEOF");
    final int fileSize = Math.toIntExact(Files.getFileStore(path).getBlockSize()) * 2;

    try (Directory dir = getDirectory(path)) {
      IndexOutput o = dir.createOutput("out", newIOContext(random()));
      byte[] b = new byte[fileSize];
      o.writeBytes(b, 0, fileSize);
      o.close();
      IndexInput i = dir.openInput("out", newIOContext(random()));
      i.seek(fileSize);

      // Seeking past EOF should always throw EOFException
      expectThrows(
          EOFException.class, () -> i.seek(fileSize + RandomizedTest.randomIntBetween(1, 2048)));

      // Reading immediately after seeking past EOF should throw EOFException
      expectThrows(EOFException.class, () -> i.readByte());
      i.close();
    }
  }

  public void testReadPastEOFShouldThrowEOFExceptionWithEmptyFile() throws Exception {
    // fileSize needs to be 0 to test this condition. Do not randomize.
    final int fileSize = 0;
    try (Directory dir = getDirectory(createTempDir("testReadPastEOF"))) {
      try (IndexOutput o = dir.createOutput("out", newIOContext(random()))) {
        o.writeBytes(new byte[fileSize], 0, fileSize);
      }

      try (IndexInput i = dir.openInput("out", newIOContext(random()))) {
        i.seek(fileSize);
        expectThrows(EOFException.class, () -> i.readByte());
        expectThrows(EOFException.class, () -> i.readBytes(new byte[1], 0, 1));
      }

      try (IndexInput i = dir.openInput("out", newIOContext(random()))) {
        expectThrows(
            EOFException.class, () -> i.seek(fileSize + RandomizedTest.randomIntBetween(1, 2048)));
        expectThrows(EOFException.class, () -> i.readByte());
        expectThrows(EOFException.class, () -> i.readBytes(new byte[1], 0, 1));
      }

      try (IndexInput i = dir.openInput("out", newIOContext(random()))) {
        expectThrows(EOFException.class, () -> i.readByte());
      }

      try (IndexInput i = dir.openInput("out", newIOContext(random()))) {
        expectThrows(EOFException.class, () -> i.readBytes(new byte[1], 0, 1));
      }
    }
  }

  public void testSeekPastEOFAndRead() throws Exception {
    try (Directory dir = getDirectory(createTempDir("testSeekPastEOF"))) {
      final int len = random().nextInt(2048);

      try (IndexOutput o = dir.createOutput("out", newIOContext(random()))) {
        byte[] b = new byte[len];
        o.writeBytes(b, 0, len);
      }

      try (IndexInput i = dir.openInput("out", newIOContext(random()))) {
        // Seeking past EOF should always throw EOFException
        expectThrows(
            EOFException.class, () -> i.seek(len + RandomizedTest.randomIntBetween(1, 2048)));

        // Reading immediately after seeking past EOF should throw EOFException
        expectThrows(EOFException.class, () -> i.readByte());
      }
    }
  }

  public void testUseDirectIODefaults() throws Exception {
    Path path = createTempDir("testUseDirectIODefaults");
    try (DirectIODirectory dir = new DirectIODirectory(FSDirectory.open(path))) {
      long largeSize = DirectIODirectory.DEFAULT_MIN_BYTES_DIRECT + random().nextInt(10_000);
      long smallSize =
          random().nextInt(Math.toIntExact(DirectIODirectory.DEFAULT_MIN_BYTES_DIRECT));
      int numDocs = random().nextInt(1000);

      assertFalse(dir.useDirectIO("dummy", IOContext.DEFAULT, OptionalLong.empty()));

      assertTrue(
          dir.useDirectIO(
              "dummy",
              IOContext.merge(new MergeInfo(numDocs, largeSize, true, -1)),
              OptionalLong.empty()));
      assertFalse(
          dir.useDirectIO(
              "dummy",
              IOContext.merge(new MergeInfo(numDocs, smallSize, true, -1)),
              OptionalLong.empty()));

      assertTrue(
          dir.useDirectIO(
              "dummy",
              IOContext.merge(new MergeInfo(numDocs, largeSize, true, -1)),
              OptionalLong.of(largeSize)));
      assertFalse(
          dir.useDirectIO(
              "dummy",
              IOContext.merge(new MergeInfo(numDocs, smallSize, true, -1)),
              OptionalLong.of(smallSize)));
      assertFalse(
          dir.useDirectIO(
              "dummy",
              IOContext.merge(new MergeInfo(numDocs, smallSize, true, -1)),
              OptionalLong.of(largeSize)));
      assertFalse(
          dir.useDirectIO(
              "dummy",
              IOContext.merge(new MergeInfo(numDocs, largeSize, true, -1)),
              OptionalLong.of(smallSize)));

      assertFalse(
          dir.useDirectIO(
              "dummy", IOContext.flush(new FlushInfo(numDocs, largeSize)), OptionalLong.empty()));
      assertFalse(
          dir.useDirectIO(
              "dummy", IOContext.flush(new FlushInfo(numDocs, smallSize)), OptionalLong.empty()));
      assertFalse(
          dir.useDirectIO(
              "dummy",
              IOContext.flush(new FlushInfo(numDocs, largeSize)),
              OptionalLong.of(largeSize)));
    }
  }

  private static byte[] writeRandomFile(Directory dir, String name, int size) throws IOException {
    byte[] bytes = new byte[size];
    random().nextBytes(bytes);
    try (IndexOutput o = dir.createOutput(name, IOContext.DEFAULT)) {
      o.writeBytes(bytes, 0, size);
    }
    return bytes;
  }

  /**
   * Slice offsets to exercise: the start of the file, a few unaligned offsets inside the first
   * block (the shape of a codec header), two block starts and an unaligned offset after one, the
   * last four bytes of the file, plus random offsets. Offsets stop at {@code fileSize - 2} so every
   * slice is at least two bytes long, which the correctness matrix relies on for its inner-slice
   * and clone cases.
   */
  private static long[] sliceOffsets(int blockSize, int fileSize) {
    assert fileSize >= 4 * blockSize;
    long[] fixed = {0, 1, 7, 96, blockSize, blockSize + 188, 2L * blockSize, fileSize - 4};
    long[] offsets = ArrayUtil.growExact(fixed, fixed.length + atLeast(3));
    for (int i = fixed.length; i < offsets.length; i++) {
      offsets[i] = RandomizedTest.randomLongBetween(0, fileSize - 2);
    }
    return offsets;
  }

  public void testSliceDefersIOAtEveryOffset() throws Exception {
    Path path = createTempDir("testSliceDefersIOAtEveryOffset");
    final int blockSize = Math.toIntExact(Files.getFileStore(path).getBlockSize());
    final int fileSize = 4 * blockSize;
    try (Directory dir = getDirectory(path)) {
      byte[] bytes = writeRandomFile(dir, "out", fileSize);
      final long[] offsets = sliceOffsets(blockSize, fileSize);
      try (IndexInput in = dir.openInput("out", IOContext.DEFAULT)) {
        for (long offset : offsets) {
          IndexInput slice = in.slice("slice@" + offset, offset, fileSize - offset);
          DirectIODirectory.DirectIOIndexInput directSlice =
              (DirectIODirectory.DirectIOIndexInput) slice;
          assertTrue(
              "slice at offset " + offset + " did not defer its first fill",
              directSlice.isDeferred());
          assertEquals(0L, slice.getFilePointer());
          assertEquals(
              "first byte of slice at offset " + offset, bytes[(int) offset], slice.readByte());
          assertFalse(directSlice.isDeferred());
        }
      }
    }
  }

  public void testSliceCorrectnessMatrix() throws Exception {
    Path path = createTempDir("testSliceCorrectnessMatrix");
    final int blockSize = Math.toIntExact(Files.getFileStore(path).getBlockSize());
    final int fileSize = 4 * blockSize;
    try (Directory dir = getDirectory(path)) {
      byte[] bytes = writeRandomFile(dir, "out", fileSize);
      final long[] offsets = sliceOffsets(blockSize, fileSize);
      try (IndexInput in = dir.openInput("out", IOContext.DEFAULT)) {
        for (long offset : offsets) {
          final long[] lengths = {1, 4, blockSize - 7, blockSize, blockSize + 3, fileSize - offset};
          for (long length : lengths) {
            if (length <= 0 || length > fileSize - offset) {
              continue;
            }
            final int len = (int) length;
            // full read-through of the slice equals the same range of the parent
            IndexInput slice = in.slice("s", offset, length);
            byte[] actual = new byte[len];
            slice.readBytes(actual, 0, len);
            assertArrayEquals(
                "offset " + offset + " length " + length,
                ArrayUtil.copyOfSubArray(bytes, (int) offset, (int) offset + len),
                actual);
            if (len >= 2) {
              // slice-of-slice at a non-zero inner offset
              IndexInput inner = in.slice("s", offset, length).slice("ss", 1, length - 1);
              byte[] innerBytes = new byte[len - 1];
              inner.readBytes(innerBytes, 0, len - 1);
              assertArrayEquals(
                  "inner slice at offset " + offset + " length " + length,
                  ArrayUtil.copyOfSubArray(bytes, (int) offset + 1, (int) offset + len),
                  innerBytes);
              // clone-after-partial-read starts at the parent's position
              IndexInput partial = in.slice("s", offset, length);
              partial.readBytes(new byte[len / 2], 0, len / 2);
              IndexInput clone = partial.clone();
              assertEquals(len / 2, clone.getFilePointer());
              byte[] rest = new byte[len - len / 2];
              clone.readBytes(rest, 0, rest.length);
              assertArrayEquals(
                  "clone at offset " + offset + " length " + length,
                  ArrayUtil.copyOfSubArray(bytes, (int) offset + len / 2, (int) offset + len),
                  rest);
            }
          }
        }
      }
    }
  }

  public void testSeekResolvesFreshParkedSlice() throws Exception {
    Path path = createTempDir("testSeekResolvesFreshParkedSlice");
    final int blockSize = Math.toIntExact(Files.getFileStore(path).getBlockSize());
    final int fileSize = DirectIODirectory.DEFAULT_MERGE_BUFFER_SIZE + 3 * blockSize;
    try (Directory dir = getDirectory(path)) {
      byte[] bytes = writeRandomFile(dir, "out", fileSize);
      final long[] offsets = {1, 96, blockSize + 188};
      try (IndexInput in = dir.openInput("out", IOContext.DEFAULT)) {
        for (long offset : offsets) {
          final long length =
              Math.min(DirectIODirectory.DEFAULT_MERGE_BUFFER_SIZE, fileSize - offset);
          IndexInput slice = in.slice("s", offset, length);
          // seek() on a slice that has not been read yet must position it correctly: the byte
          // read is the slice's own last byte, not one relative to the wrong base.
          slice.seek(length - 1);
          assertEquals("offset " + offset, bytes[(int) (offset + length - 1)], slice.readByte());
          assertEquals(length, slice.getFilePointer());
        }
      }
    }
  }

  public void testCloneOfPartiallyReadUnalignedSliceResolvesToItsPosition() throws Exception {
    // clone() positions a fresh clone at the parent's file pointer through seekInternal, and is
    // the only caller that reaches it with a pending start. If seekInternal did not drop that
    // pending start first, a clone taken after a partial read of a short unaligned slice would
    // read the wrong bytes or throw EOFException.
    Path path = createTempDir("testCloneOfPartiallyReadSlice");
    final int blockSize = Math.toIntExact(Files.getFileStore(path).getBlockSize());
    final int fileSize = 4 * blockSize;
    try (Directory dir = getDirectory(path)) {
      byte[] bytes = writeRandomFile(dir, "out", fileSize);
      final long[] offsets = {1, 96, blockSize + 188, 3L * blockSize - 4};
      try (IndexInput in = dir.openInput("out", IOContext.DEFAULT)) {
        for (long offset : offsets) {
          final long length = Math.min(blockSize - 7, fileSize - offset);
          IndexInput partial = in.slice("s", offset, length);
          final int half = (int) length / 2;
          partial.readBytes(new byte[half], 0, half);
          IndexInput clone = partial.clone();
          assertEquals("offset " + offset, half, clone.getFilePointer());
          byte[] rest = new byte[(int) length - half];
          clone.readBytes(rest, 0, rest.length);
          assertArrayEquals(
              "offset " + offset,
              ArrayUtil.copyOfSubArray(bytes, (int) offset + half, (int) offset + (int) length),
              rest);
        }
      }
    }
  }

  /**
   * A characterization test, not an endorsement. Today a read past a slice's own length is served
   * from the buffered block as long as it stays inside the window, and only a read that runs off
   * the end of the window throws {@link EOFException}. That quirk predates this change; the lazy
   * first fill must reproduce it exactly rather than "fix" it in passing. If the quirk is ever
   * removed on purpose, this test should change with it.
   */
  public void testFirstFillEofSemanticsMatchEagerFill() throws Exception {
    Path path = createTempDir("testFirstFillEofSemantics");
    final int blockSize = Math.toIntExact(Files.getFileStore(path).getBlockSize());
    final int fileSize = 4 * blockSize;
    // the oracle below relies on the whole file fitting in one buffer window
    assumeTrue(
        "file must fit in one buffer window",
        fileSize <= DirectIODirectory.DEFAULT_MERGE_BUFFER_SIZE);
    try (Directory dir = getDirectory(path)) {
      byte[] bytes = writeRandomFile(dir, "out", fileSize);
      final long[] offsets = {0, 96, blockSize, fileSize - 5};
      final long[] lengths = {0, 1, 4, blockSize};
      try (IndexInput in = dir.openInput("out", IOContext.DEFAULT)) {
        for (long offset : offsets) {
          for (long l : lengths) {
            final long length = Math.min(l, fileSize - offset);
            final int[] readLens = {1, 4, (int) length + 2, blockSize + 32, fileSize};
            for (int readLen : readLens) {
              IndexInput slice = in.slice("s", offset, length);
              final String cell = "offset=" + offset + " length=" + length + " readLen=" + readLen;
              // if the first fill ever checked EOF against the caller's read length instead of
              // the pending offset, the in-window cells below would fail
              if (readLen <= fileSize - offset) {
                byte[] dst = new byte[readLen];
                slice.readBytes(dst, 0, readLen);
                assertArrayEquals(
                    cell,
                    ArrayUtil.copyOfSubArray(bytes, (int) offset, (int) offset + readLen),
                    dst);
              } else {
                expectThrows(
                    EOFException.class, cell, () -> slice.readBytes(new byte[readLen], 0, readLen));
              }
            }
          }
        }
      }
    }
  }

  public void testZeroLengthSliceAtFileEndThrowsEOF() throws Exception {
    Path path = createTempDir("testZeroLengthSliceAtFileEnd");
    final int blockSize = Math.toIntExact(Files.getFileStore(path).getBlockSize());
    // one file whose length is a multiple of the block size (pending offset 0) and one whose
    // length is not
    final int[] fileSizes = {2 * blockSize, 2 * blockSize + 300};
    try (Directory dir = getDirectory(path)) {
      for (int fileSize : fileSizes) {
        String name = "out" + fileSize;
        writeRandomFile(dir, name, fileSize);
        try (IndexInput in = dir.openInput(name, IOContext.DEFAULT)) {
          IndexInput slice = in.slice("end", in.length(), 0);
          // the exception type matters: a first fill that finds nothing to read must fall
          // through to the caller's own read and throw EOFException, not a
          // BufferUnderflowException from an empty buffer
          expectThrows(EOFException.class, "file size " + fileSize, slice::readByte);
        }
      }
    }
  }

  public void testSeekBeyondSliceLengthMatchesEagerBehaviour() throws Exception {
    Path path = createTempDir("testSeekBeyondSliceLength");
    final int blockSize = Math.toIntExact(Files.getFileStore(path).getBlockSize());
    final int fileSize = 4 * blockSize;
    assumeTrue(
        "file must fit in one buffer window",
        fileSize <= DirectIODirectory.DEFAULT_MERGE_BUFFER_SIZE);
    try (Directory dir = getDirectory(path)) {
      byte[] bytes = writeRandomFile(dir, "out", fileSize);
      try (IndexInput in = dir.openInput("out", IOContext.DEFAULT)) {
        // seek targets inside the block the eager code buffered at construction: today the
        // seek silently repositions and the read serves the byte beyond the slice's end. A
        // fresh lazy slice must do the same rather than throw EOFException, and
        // getFilePointer() must return normally afterwards (assertions are enabled)
        final long[] offsets = {blockSize, blockSize + 188};
        final long[] seekTargets = {10, blockSize + 50L};
        for (long offset : offsets) {
          for (long target : seekTargets) {
            IndexInput slice = in.slice("s", offset, 4);
            slice.seek(target);
            assertEquals(
                "offset " + offset + " target " + target,
                bytes[(int) (offset + target)],
                slice.readByte());
            assertEquals(target + 1, slice.getFilePointer());
          }
        }
        // a target outside the buffered window: the refill checks EOF against the slice's own
        // end and throws, as today; getFilePointer() must still return normally afterwards
        IndexInput far = in.slice("s", blockSize + 188, 4);
        expectThrows(
            EOFException.class, () -> far.seek(DirectIODirectory.DEFAULT_MERGE_BUFFER_SIZE + 100L));
        assertThat(far.getFilePointer(), greaterThanOrEqualTo(0L));
      }
    }
  }

  // Ping-pong seeks should be really fast, since the position should be within buffer.
  // The test should complete within sub-second times, not minutes.
  public void testSeekSmall() throws IOException {
    Path tmpDir = createTempDir("testSeekSmall");
    try (Directory dir = getDirectory(tmpDir)) {
      int len = atLeast(100);
      try (IndexOutput o = dir.createOutput("out", newIOContext(random()))) {
        byte[] b = new byte[len];
        for (int i = 0; i < len; i++) {
          b[i] = (byte) i;
        }
        o.writeBytes(b, 0, len);
      }
      try (IndexInput in = dir.openInput("out", newIOContext(random()))) {
        for (int i = 0; i < 100_000; i++) {
          in.seek(2);
          assertEquals(2, in.readByte());
          in.seek(1);
          assertEquals(1, in.readByte());
          in.seek(0);
          assertEquals(0, in.readByte());
        }
      }
    }
  }
}
