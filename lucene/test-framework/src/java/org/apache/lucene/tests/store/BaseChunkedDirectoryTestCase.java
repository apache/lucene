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
package org.apache.lucene.tests.store;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Random;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;

/**
 * Base class for Directories that "chunk" the input into blocks.
 *
 * <p>It tries to explicitly chunk with different sizes and test boundary conditions around the
 * chunks.
 */
public abstract class BaseChunkedDirectoryTestCase extends BaseDirectoryTestCase {

  @Override
  protected Directory getDirectory(Path path) throws IOException {
    return getDirectory(path, 1 << TestUtil.nextInt(random(), 10, 20));
  }

  /** Creates a new directory with the specified max chunk size */
  protected abstract Directory getDirectory(Path path, int maxChunkSize) throws IOException;

  public void testCloneClose() throws Exception {
    Directory dir = getDirectory(createTempDir("testCloneClose"));
    IndexOutput io = dir.createOutput("bytes", newIOContext(random()));
    io.writeVInt(5);
    io.close();
    IndexInput one = dir.openInput("bytes", IOContext.DEFAULT);
    IndexInput two = one.clone();
    IndexInput three = two.clone(); // clone of clone
    two.close();
    assertEquals(5, one.readVInt());
    expectThrows(
        AlreadyClosedException.class,
        () -> {
          two.readVInt();
        });
    assertEquals(5, three.readVInt());
    one.close();
    three.close();
    dir.close();
  }

  public void testCloneSliceClose() throws Exception {
    Directory dir = getDirectory(createTempDir("testCloneSliceClose"));
    IndexOutput io = dir.createOutput("bytes", newIOContext(random()));
    io.writeInt(1);
    io.writeInt(2);
    io.close();
    IndexInput slicer = dir.openInput("bytes", newIOContext(random()));
    IndexInput one = slicer.slice("first int", 0, 4);
    IndexInput two = slicer.slice("second int", 4, 4);
    one.close();
    expectThrows(
        AlreadyClosedException.class,
        () -> {
          one.readInt();
        });
    assertEquals(2, two.readInt());
    // reopen a new slice "another":
    IndexInput another = slicer.slice("first int", 0, 4);
    assertEquals(1, another.readInt());
    another.close();
    two.close();
    slicer.close();
    dir.close();
  }

  public void testSeekZero() throws Exception {
    int upto = TEST_NIGHTLY ? 31 : 3;
    for (int i = 0; i < upto; i++) {
      Directory dir = getDirectory(createTempDir("testSeekZero"), 1 << i);
      IndexOutput io = dir.createOutput("zeroBytes", newIOContext(random()));
      io.close();
      IndexInput ii = dir.openInput("zeroBytes", newIOContext(random()));
      ii.seek(0L);
      ii.close();
      dir.close();
    }
  }

  public void testSeekSliceZero() throws Exception {
    int upto = TEST_NIGHTLY ? 31 : 3;
    for (int i = 0; i < upto; i++) {
      Directory dir = getDirectory(createTempDir("testSeekSliceZero"), 1 << i);
      IndexOutput io = dir.createOutput("zeroBytes", newIOContext(random()));
      io.close();
      IndexInput slicer = dir.openInput("zeroBytes", newIOContext(random()));
      IndexInput ii = slicer.slice("zero-length slice", 0, 0);
      ii.seek(0L);
      ii.close();
      slicer.close();
      dir.close();
    }
  }

  public void testSeekEnd() throws Exception {
    for (int i = 0; i < 17; i++) {
      Directory dir = getDirectory(createTempDir("testSeekEnd"), 1 << i);
      IndexOutput io = dir.createOutput("bytes", newIOContext(random()));
      byte[] bytes = new byte[1 << i];
      random().nextBytes(bytes);
      io.writeBytes(bytes, bytes.length);
      io.close();
      IndexInput ii = dir.openInput("bytes", newIOContext(random()));
      byte[] actual = new byte[1 << i];
      ii.readBytes(actual, 0, actual.length);
      assertEquals(new BytesRef(bytes), new BytesRef(actual));
      ii.seek(1 << i);
      ii.close();
      dir.close();
    }
  }

  public void testSeekSliceEnd() throws Exception {
    for (int i = 0; i < 17; i++) {
      Directory dir = getDirectory(createTempDir("testSeekSliceEnd"), 1 << i);
      IndexOutput io = dir.createOutput("bytes", newIOContext(random()));
      byte[] bytes = new byte[1 << i];
      random().nextBytes(bytes);
      io.writeBytes(bytes, bytes.length);
      io.close();
      IndexInput slicer = dir.openInput("bytes", newIOContext(random()));
      IndexInput ii = slicer.slice("full slice", 0, bytes.length);
      byte[] actual = new byte[1 << i];
      ii.readBytes(actual, 0, actual.length);
      assertEquals(new BytesRef(bytes), new BytesRef(actual));
      ii.seek(1 << i);
      ii.close();
      slicer.close();
      dir.close();
    }
  }

  public void testSeeking() throws Exception {
    int numIters = TEST_NIGHTLY ? 10 : 1;
    for (int i = 0; i < numIters; i++) {
      Directory dir = getDirectory(createTempDir("testSeeking"), 1 << i);
      IndexOutput io = dir.createOutput("bytes", newIOContext(random()));
      byte[] bytes = new byte[1 << (i + 1)]; // make sure we switch buffers
      random().nextBytes(bytes);
      io.writeBytes(bytes, bytes.length);
      io.close();
      IndexInput ii = dir.openInput("bytes", newIOContext(random()));
      byte[] actual = new byte[1 << (i + 1)]; // first read all bytes
      ii.readBytes(actual, 0, actual.length);
      assertEquals(new BytesRef(bytes), new BytesRef(actual));
      for (int sliceStart = 0; sliceStart < bytes.length; sliceStart++) {
        for (int sliceLength = 0; sliceLength < bytes.length - sliceStart; sliceLength++) {
          byte[] slice = new byte[sliceLength];
          ii.seek(sliceStart);
          ii.readBytes(slice, 0, slice.length);
          assertEquals(new BytesRef(bytes, sliceStart, sliceLength), new BytesRef(slice));
        }
      }
      ii.close();
      dir.close();
    }
  }

  // note instead of seeking to offset and reading length, this opens slices at the
  // the various offset+length and just does readBytes.
  public void testSlicedSeeking() throws Exception {
    int numIters = TEST_NIGHTLY ? 10 : 1;
    for (int i = 0; i < numIters; i++) {
      Directory dir = getDirectory(createTempDir("testSlicedSeeking"), 1 << i);
      IndexOutput io = dir.createOutput("bytes", newIOContext(random()));
      byte[] bytes = new byte[1 << (i + 1)]; // make sure we switch buffers
      random().nextBytes(bytes);
      io.writeBytes(bytes, bytes.length);
      io.close();
      IndexInput ii = dir.openInput("bytes", newIOContext(random()));
      byte[] actual = new byte[1 << (i + 1)]; // first read all bytes
      ii.readBytes(actual, 0, actual.length);
      ii.close();
      assertEquals(new BytesRef(bytes), new BytesRef(actual));
      IndexInput slicer = dir.openInput("bytes", newIOContext(random()));
      for (int sliceStart = 0; sliceStart < bytes.length; sliceStart++) {
        for (int sliceLength = 0; sliceLength < bytes.length - sliceStart; sliceLength++) {
          assertSlice(bytes, slicer, 0, sliceStart, sliceLength);
        }
      }
      slicer.close();
      dir.close();
    }
  }

  @Override
  public void testSliceOfSlice() throws Exception {
    int upto = TEST_NIGHTLY ? 10 : 8;
    for (int i = 0; i < upto; i++) {
      Directory dir = getDirectory(createTempDir("testSliceOfSlice"), 1 << i);
      IndexOutput io = dir.createOutput("bytes", newIOContext(random()));
      byte[] bytes = new byte[1 << (i + 1)]; // make sure we switch buffers
      random().nextBytes(bytes);
      io.writeBytes(bytes, bytes.length);
      io.close();
      IndexInput ii = dir.openInput("bytes", newIOContext(random()));
      byte[] actual = new byte[1 << (i + 1)]; // first read all bytes
      ii.readBytes(actual, 0, actual.length);
      ii.close();
      assertEquals(new BytesRef(bytes), new BytesRef(actual));
      IndexInput outerSlicer = dir.openInput("bytes", newIOContext(random()));
      final int outerSliceStart = random().nextInt(bytes.length / 2);
      final int outerSliceLength = random().nextInt(bytes.length - outerSliceStart);
      IndexInput innerSlicer =
          outerSlicer.slice("parentBytesSlice", outerSliceStart, outerSliceLength);
      for (int sliceStart = 0; sliceStart < outerSliceLength; sliceStart++) {
        for (int sliceLength = 0; sliceLength < outerSliceLength - sliceStart; sliceLength++) {
          assertSlice(bytes, innerSlicer, outerSliceStart, sliceStart, sliceLength);
        }
      }
      innerSlicer.close();
      outerSlicer.close();
      dir.close();
    }
  }

  private void assertSlice(
      byte[] bytes, IndexInput slicer, int outerSliceStart, int sliceStart, int sliceLength)
      throws IOException {
    byte[] slice = new byte[sliceLength];
    IndexInput input = slicer.slice("bytesSlice", sliceStart, slice.length);
    input.readBytes(slice, 0, slice.length);
    input.close();
    assertEquals(
        new BytesRef(bytes, outerSliceStart + sliceStart, sliceLength), new BytesRef(slice));
  }

  public void testRandomChunkSizes() throws Exception {
    int num = TEST_NIGHTLY ? atLeast(10) : 3;
    for (int i = 0; i < num; i++) {
      assertChunking(random(), TestUtil.nextInt(random(), 20, 100));
    }
  }

  private void assertChunking(Random random, int chunkSize) throws Exception {
    Path path = createTempDir("mmap" + chunkSize);
    Directory chunkedDir = getDirectory(path, chunkSize);
    // we will map a lot, try to turn on the unmap hack
    if (chunkedDir instanceof MMapDirectory && MMapDirectory.UNMAP_SUPPORTED) {
      ((MMapDirectory) chunkedDir).setUseUnmap(true);
    }
    MockDirectoryWrapper dir = new MockDirectoryWrapper(random, chunkedDir);
    RandomIndexWriter writer =
        new RandomIndexWriter(
            random,
            dir,
            newIndexWriterConfig(new MockAnalyzer(random)).setMergePolicy(newLogMergePolicy()));
    Document doc = new Document();
    Field docid = newStringField("docid", "0", Field.Store.YES);
    Field junk = newStringField("junk", "", Field.Store.YES);
    doc.add(docid);
    doc.add(junk);

    int numDocs = 100;
    for (int i = 0; i < numDocs; i++) {
      docid.setStringValue("" + i);
      junk.setStringValue(TestUtil.randomUnicodeString(random));
      writer.addDocument(doc);
    }
    IndexReader reader = writer.getReader();
    writer.close();

    int numAsserts = atLeast(100);
    for (int i = 0; i < numAsserts; i++) {
      int docID = random.nextInt(numDocs);
      assertEquals("" + docID, reader.document(docID).get("docid"));
    }
    reader.close();
    dir.close();
  }

  public void testLittleEndianLongsCrossBoundary() throws Exception {
    try (Directory dir = getDirectory(createTempDir("testLittleEndianLongsCrossBoundary"), 16)) {
      try (IndexOutput out = dir.createOutput("littleEndianLongs", newIOContext(random()))) {
        out.writeByte((byte) 2);
        out.writeLong(3L);
        out.writeLong(Long.MAX_VALUE);
        out.writeLong(-3L);
      }
      try (IndexInput input = dir.openInput("littleEndianLongs", newIOContext(random()))) {
        assertEquals(25, input.length());
        assertEquals(2, input.readByte());
        long[] l = new long[4];
        input.readLongs(l, 1, 3);
        assertArrayEquals(new long[] {0L, 3L, Long.MAX_VALUE, -3L}, l);
        assertEquals(25, input.getFilePointer());
      }
    }
  }

  public void testLittleEndianFloatsCrossBoundary() throws Exception {
    try (Directory dir = getDirectory(createTempDir("testFloatsCrossBoundary"), 8)) {
      try (IndexOutput out = dir.createOutput("Floats", newIOContext(random()))) {
        out.writeByte((byte) 2);
        out.writeInt(Float.floatToIntBits(3f));
        out.writeInt(Float.floatToIntBits(Float.MAX_VALUE));
        out.writeInt(Float.floatToIntBits(-3f));
      }
      try (IndexInput input = dir.openInput("Floats", newIOContext(random()))) {
        assertEquals(13, input.length());
        assertEquals(2, input.readByte());
        float[] ff = new float[4];
        input.readFloats(ff, 1, 3);
        assertArrayEquals(new float[] {0, 3f, Float.MAX_VALUE, -3f}, ff, 0);
        assertEquals(13, input.getFilePointer());
      }
    }
  }
}
