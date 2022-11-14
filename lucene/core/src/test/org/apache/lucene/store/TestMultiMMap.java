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
package org.apache.lucene.store;

import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import org.apache.lucene.tests.store.BaseChunkedDirectoryTestCase;
import org.apache.lucene.util.BytesRef;
import org.junit.BeforeClass;

/**
 * Tests MMapDirectory's MultiMMapIndexInput
 *
 * <p>Because Java's ByteBuffer uses an int to address the values, it's necessary to access a file
 * &gt; Integer.MAX_VALUE in size using multiple byte buffers.
 */
public class TestMultiMMap extends BaseChunkedDirectoryTestCase {

  @Override
  protected Directory getDirectory(Path path, int maxChunkSize) throws IOException {
    return new MMapDirectory(path, maxChunkSize);
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    assertTrue(MMapDirectory.UNMAP_NOT_SUPPORTED_REASON, MMapDirectory.UNMAP_SUPPORTED);
  }

  public void testSeekingExceptions() throws IOException {
    final int sliceSize = 128;
    try (Directory dir = getDirectory(createTempDir(), sliceSize)) {
      final int size = 128 + 63;
      try (IndexOutput out = dir.createOutput("a", IOContext.DEFAULT)) {
        for (int i = 0; i < size; ++i) {
          out.writeByte((byte) 0);
        }
      }
      try (IndexInput in = dir.openInput("a", IOContext.DEFAULT)) {
        final long negativePos = -1234;
        var e =
            expectThrowsAnyOf(
                List.of(IllegalArgumentException.class, AssertionError.class),
                () -> in.seek(negativePos));
        assertTrue(
            "does not mention negative position", e.getMessage().contains("negative position"));

        final long posAfterEOF = size + 123;
        var eof = expectThrows(EOFException.class, () -> in.seek(posAfterEOF));
        assertTrue(
            "wrong position in error message: " + eof,
            eof.getMessage().contains(String.format(Locale.ROOT, "(pos=%d)", posAfterEOF)));

        // this test verifies that the invalid position is transformed back to original one for
        // exception by slicing:
        IndexInput slice = in.slice("slice", 33, sliceSize + 15);
        // ensure that the slice uses multi-mmap:
        assertCorrectImpl(false, slice);
        eof = expectThrows(EOFException.class, () -> slice.seek(posAfterEOF));
        assertTrue(
            "wrong position in error message: " + eof,
            eof.getMessage().contains(String.format(Locale.ROOT, "(pos=%d)", posAfterEOF)));
      }
    }
  }

  // TODO: can we improve ByteBuffersDirectory (without overhead) and move these clone safety tests
  // to the base test case?

  public void testCloneSafety() throws Exception {
    Directory mmapDir = getDirectory(createTempDir("testCloneSafety"));
    IndexOutput io = mmapDir.createOutput("bytes", newIOContext(random()));
    io.writeVInt(5);
    io.close();
    IndexInput one = mmapDir.openInput("bytes", IOContext.DEFAULT);
    IndexInput two = one.clone();
    IndexInput three = two.clone(); // clone of clone
    one.close();
    expectThrows(
        AlreadyClosedException.class,
        () -> {
          one.readVInt();
        });
    expectThrows(
        AlreadyClosedException.class,
        () -> {
          two.readVInt();
        });
    expectThrows(
        AlreadyClosedException.class,
        () -> {
          three.readVInt();
        });

    two.close();
    three.close();
    // test double close of master:
    one.close();
    mmapDir.close();
  }

  public void testCloneSliceSafety() throws Exception {
    Directory mmapDir = getDirectory(createTempDir("testCloneSliceSafety"));
    IndexOutput io = mmapDir.createOutput("bytes", newIOContext(random()));
    io.writeInt(1);
    io.writeInt(2);
    io.close();
    IndexInput slicer = mmapDir.openInput("bytes", newIOContext(random()));
    IndexInput one = slicer.slice("first int", 0, 4);
    IndexInput two = slicer.slice("second int", 4, 4);
    IndexInput three = one.clone(); // clone of clone
    IndexInput four = two.clone(); // clone of clone
    slicer.close();
    expectThrows(
        AlreadyClosedException.class,
        () -> {
          one.readInt();
        });
    expectThrows(
        AlreadyClosedException.class,
        () -> {
          two.readInt();
        });
    expectThrows(
        AlreadyClosedException.class,
        () -> {
          three.readInt();
        });
    expectThrows(
        AlreadyClosedException.class,
        () -> {
          four.readInt();
        });

    one.close();
    two.close();
    three.close();
    four.close();
    // test double-close of slicer:
    slicer.close();
    mmapDir.close();
  }

  // test has asserts specific to mmap impl...
  public void testImplementations() throws Exception {
    for (int i = 2; i < 12; i++) {
      final int chunkSize = 1 << i;
      Directory mmapDir = getDirectory(createTempDir("testImplementations"), chunkSize);
      IndexOutput io = mmapDir.createOutput("bytes", newIOContext(random()));
      int size = random().nextInt(chunkSize * 2) + 3; // add some buffer of 3 for slice tests
      byte[] bytes = new byte[size];
      random().nextBytes(bytes);
      io.writeBytes(bytes, bytes.length);
      io.close();
      IndexInput ii = mmapDir.openInput("bytes", newIOContext(random()));
      byte[] actual = new byte[size]; // first read all bytes
      ii.readBytes(actual, 0, actual.length);
      assertEquals(new BytesRef(bytes), new BytesRef(actual));
      // reinit:
      ii.seek(0L);

      // check impl (we must check size < chunksize: currently, if size==chunkSize, we get 2
      // buffers, the second one empty:
      assertCorrectImpl(size < chunkSize, ii);

      // clone tests:
      assertSame(ii.getClass(), ii.clone().getClass());

      // slice test (offset 0)
      int sliceSize = random().nextInt(size);
      IndexInput slice = ii.slice("slice", 0, sliceSize);
      assertCorrectImpl(sliceSize < chunkSize, slice);

      // slice test (offset > 0 )
      int offset = random().nextInt(size - 1) + 1;
      sliceSize = random().nextInt(size - offset + 1);
      slice = ii.slice("slice", offset, sliceSize);
      // System.out.println(offset + "/" + sliceSize + " chunkSize=" + chunkSize + " " +
      // slice.getClass());
      assertCorrectImpl(offset % chunkSize + sliceSize < chunkSize, slice);

      ii.close();
      mmapDir.close();
    }
  }

  private static void assertCorrectImpl(boolean isSingle, IndexInput ii) {
    var clazz = ii.getClass();
    if (isSingle) {
      assertTrue(
          "Require a single impl, got " + clazz, clazz.getSimpleName().matches("Single\\w+Impl"));
    } else {
      assertTrue(
          "Require a multi impl, got " + clazz, clazz.getSimpleName().matches("Multi\\w+Impl"));
    }
  }
}
