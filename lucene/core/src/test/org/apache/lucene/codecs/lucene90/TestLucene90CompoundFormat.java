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
package org.apache.lucene.codecs.lucene90;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.tests.index.BaseCompoundFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;

public class TestLucene90CompoundFormat extends BaseCompoundFormatTestCase {
  private final Codec codec = TestUtil.getDefaultCodec();

  @Override
  protected Codec getCodec() {
    return codec;
  }

  public void testFileLengthOrdering() throws IOException {
    Directory dir = newDirectory();
    // Setup the test segment
    String segment = "_123";
    int chunk = 1024; // internal buffer size used by the stream
    SegmentInfo si = newSegmentInfo(dir, segment);
    byte[] segId = si.getId();
    List<String> orderedFiles = new ArrayList<>();
    int randomFileSize = random().nextInt(0, chunk);
    for (int i = 0; i < 10; i++) {
      String filename = segment + "." + i;
      createRandomFile(dir, filename, randomFileSize, segId);
      // increase the next files size by a random amount
      randomFileSize += random().nextInt(1, 100);
      orderedFiles.add(filename);
    }
    List<String> shuffledFiles = new ArrayList<>(orderedFiles);
    Collections.shuffle(shuffledFiles, random());
    si.setFiles(shuffledFiles);
    si.getCodec().compoundFormat().write(dir, si, IOContext.DEFAULT);

    // entries file should contain files ordered by their size
    String entriesFileName =
        IndexFileNames.segmentFileName(si.name, "", Lucene90CompoundFormat.ENTRIES_EXTENSION);
    try (ChecksumIndexInput entriesStream = dir.openChecksumInput(entriesFileName)) {
      Throwable priorE = null;
      try {
        CodecUtil.checkIndexHeader(
            entriesStream,
            Lucene90CompoundFormat.ENTRY_CODEC,
            Lucene90CompoundFormat.VERSION_START,
            Lucene90CompoundFormat.VERSION_CURRENT,
            si.getId(),
            "");
        final int numEntries = entriesStream.readVInt();
        long lastOffset = 0;
        long lastLength = 0;
        for (int i = 0; i < numEntries; i++) {
          final String id = entriesStream.readString();
          assertEquals(orderedFiles.get(i), segment + id);
          long offset = entriesStream.readLong();
          assertTrue(offset > lastOffset);
          lastOffset = offset;
          long length = entriesStream.readLong();
          assertTrue(length >= lastLength);
          lastLength = length;
        }
      } catch (Throwable exception) {
        priorE = exception;
      } finally {
        CodecUtil.checkFooter(entriesStream, priorE);
      }
    }
    dir.close();
  }

  /** An inner file opened with a context it does not read as gets a new mapping. */
  public void testAnOpenReadingDifferentlyReadsAMappingOfItsOwn() throws IOException {
    try (Directory base = newDirectory()) {
      SegmentInfo si = writeCompound(base);
      DataOpens dir = new DataOpens(base);
      Directory cfs = si.getCodec().compoundFormat().getCompoundReader(dir, si);
      try {
        assertEquals(1, dir.opens.size());
        String first = "_123.0";
        String second = "_123.1";
        try (IndexInput search = cfs.openInput(first, RANDOM)) {
          assertEquals("searches read the mapping opened with the reader", 1, dir.opens.size());

          IndexInput sequential = cfs.openInput(first, SEQUENTIAL);
          assertEquals("reading it differently maps the file again", 2, dir.opens.size());
          assertEquals(
              "opened without advice, like the reader's mapping",
              IOContext.DEFAULT,
              dir.opens.get(1));
          IndexInput otherSequential = cfs.openInput(first, SEQUENTIAL);
          IndexInput otherFile = cfs.openInput(second, SEQUENTIAL);
          assertEquals("reading the same way, or another file, maps nothing", 2, dir.opens.size());
          try (IndexInput expected = base.openInput(first, IOContext.DEFAULT)) {
            assertSameStreams(first, expected, sequential);
          }
          try (IndexInput expected = base.openInput(first, IOContext.DEFAULT)) {
            assertSameStreams(first, expected, otherSequential);
          }
          try (IndexInput expected = base.openInput(first, IOContext.DEFAULT)) {
            assertSameStreams(first, expected, search);
          }
          try (IndexInput expected = base.openInput(second, IOContext.DEFAULT)) {
            assertSameStreams(second, expected, otherFile);
          }
          sequential.close();
          otherSequential.close();
          otherFile.close();
          assertEquals("the other mapping stays open with the reader", 0, dir.closes);
        }
      } finally {
        cfs.close();
      }
      assertEquals("closing the reader closes both mappings", 2, dir.closes);
    }
  }

  /** No new mapping when every context reads the same. */
  public void testNoOtherMappingWhenInputsReadTheSame() throws IOException {
    try (Directory base = newDirectory()) {
      SegmentInfo si = writeCompound(base);
      DataOpens dir = new DataOpens(base, false);
      try (Directory cfs = si.getCodec().compoundFormat().getCompoundReader(dir, si);
          IndexInput search = cfs.openInput("_123.0", RANDOM);
          IndexInput sequential = cfs.openInput("_123.0", SEQUENTIAL)) {
        assertEquals(1, dir.opens.size());
        try (IndexInput expected = base.openInput("_123.0", IOContext.DEFAULT)) {
          assertSameStreams("_123.0", expected, sequential);
        }
        try (IndexInput expected = base.openInput("_123.0", IOContext.DEFAULT)) {
          assertSameStreams("_123.0", expected, search);
        }
      }
      assertEquals(1, dir.closes);
    }
  }

  public void testFallBackToTheReaderMappingWhenTheFileIsGone() throws IOException {
    try (Directory base = newDirectory()) {
      SegmentInfo si = writeCompound(base);
      DataOpens dir = new DataOpens(base);
      try (Directory cfs = si.getCodec().compoundFormat().getCompoundReader(dir, si)) {
        String file = "_123.0";
        try (IndexInput search = cfs.openInput(file, RANDOM)) {
          dir.gone = true;
          try (IndexInput sequential = cfs.openInput(file, SEQUENTIAL);
              IndexInput expected = base.openInput(file, IOContext.DEFAULT)) {
            assertSameStreams(file, expected, sequential);
          }
          try (IndexInput expected = base.openInput(file, IOContext.DEFAULT)) {
            assertSameStreams(file, expected, search);
          }
        }
        assertEquals(1, dir.opens.size());
      }
      assertEquals(1, dir.closes);
    }
  }

  /** On MMapDirectory, inner files opened with different advice use different mappings. */
  public void testEachAdviceReadsItsOwnMapping() throws IOException {
    try (MMapDirectory dir = new MMapDirectory(createTempDir("cfsAdvice"))) {
      dir.setReadAdvice(MMapDirectory.ADVISE_BY_CONTEXT);
      // larger than a page, so that inner files get advised
      SegmentInfo si = writeCompound(dir, 1 << 17);
      try (Directory cfs = si.getCodec().compoundFormat().getCompoundReader(dir, si);
          IndexInput search = cfs.openInput("_123.0", RANDOM);
          IndexInput otherSearch = cfs.openInput("_123.0", RANDOM);
          IndexInput sequential = cfs.openInput("_123.0", SEQUENTIAL)) {
        assertEquals(address(search), address(otherSearch));
        assertEquals(
            "only advice that applies needs a mapping of its own",
            MMapDirectory.supportsMadvise(),
            address(search) != address(sequential));
        try (IndexInput expected = dir.openInput("_123.0", IOContext.DEFAULT)) {
          assertSameStreams("_123.0", expected, sequential);
        }
      }
    }
  }

  private static long address(IndexInput in) throws IOException {
    return ((MemorySegmentAccessInput) in).segmentSliceOrNull(0, 1).address();
  }

  public void testNoOpenOnceClosed() throws IOException {
    try (Directory base = newDirectory()) {
      SegmentInfo si = writeCompound(base);
      DataOpens dir = new DataOpens(base);
      Directory cfs = si.getCodec().compoundFormat().getCompoundReader(dir, si);
      cfs.close();
      expectThrows(AlreadyClosedException.class, () -> cfs.openInput("_123.0", SEQUENTIAL));
      assertEquals(1, dir.opens.size());
      assertEquals(1, dir.closes);
    }
  }

  public void testAFailedOpenKeepsNothingOpen() throws IOException {
    try (Directory base = newDirectory()) {
      SegmentInfo si = writeCompound(base);
      DataOpens dir = new DataOpens(base);
      try (Directory cfs = si.getCodec().compoundFormat().getCompoundReader(dir, si);
          IndexInput search = cfs.openInput("_123.0", RANDOM)) {
        dir.failNextOpen = true;
        expectThrows(IOException.class, () -> cfs.openInput("_123.0", SEQUENTIAL));
        assertEquals(1, dir.opens.size());
        try (IndexInput sequential = cfs.openInput("_123.0", SEQUENTIAL);
            IndexInput expected = base.openInput("_123.0", IOContext.DEFAULT)) {
          assertSameStreams("_123.0", expected, sequential);
        }
        assertEquals("the next open maps the file", 2, dir.opens.size());
        try (IndexInput expected = base.openInput("_123.0", IOContext.DEFAULT)) {
          assertSameStreams("_123.0", expected, search);
        }
      }
      assertEquals(2, dir.closes);
    }
  }

  public void testConcurrentOpensMapOnce() throws Exception {
    try (Directory base = newDirectory()) {
      SegmentInfo si = writeCompound(base);
      DataOpens dir = new DataOpens(base);
      try (Directory cfs = si.getCodec().compoundFormat().getCompoundReader(dir, si);
          IndexInput first = cfs.openInput("_123.0", RANDOM);
          IndexInput second = cfs.openInput("_123.1", RANDOM)) {
        int threadCount = 2 + random().nextInt(4);
        CountDownLatch start = new CountDownLatch(1);
        List<Thread> threads = new ArrayList<>();
        List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());
        for (int t = 0; t < threadCount; t++) {
          String file = "_123." + (t % 2);
          Thread thread =
              new Thread(
                  () -> {
                    try {
                      start.await();
                      try (IndexInput in = cfs.openInput(file, SEQUENTIAL)) {
                        in.seek(in.length());
                      }
                    } catch (Throwable e) {
                      failures.add(e);
                    }
                  });
          thread.start();
          threads.add(thread);
        }
        start.countDown();
        for (Thread thread : threads) {
          thread.join();
        }
        assertEquals(List.of(), failures);
        assertEquals(2, dir.opens.size());
        for (IndexInput search : List.of(first, second)) {
          search.seek(search.length());
        }
      }
      assertEquals(2, dir.closes);
    }
  }

  private static final IOContext RANDOM = IOContext.DEFAULT.withHints(DataAccessHint.RANDOM);
  private static final IOContext SEQUENTIAL =
      IOContext.DEFAULT.withHints(DataAccessHint.SEQUENTIAL);

  private static SegmentInfo writeCompound(Directory dir) throws IOException {
    return writeCompound(dir, random().nextInt(1, 4096));
  }

  private static SegmentInfo writeCompound(Directory dir, int length) throws IOException {
    SegmentInfo si = newSegmentInfo(dir, "_123");
    List<String> files = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      String name = "_123." + i;
      createRandomFile(dir, name, length, si.getId());
      files.add(name);
    }
    si.setFiles(files);
    si.getCodec().compoundFormat().write(dir, si, IOContext.DEFAULT);
    return si;
  }

  /**
   * Records the contexts the compound data file is opened with, and counts closes. Slices match
   * like memory-mapped inputs using ADVISE_BY_CONTEXT, or match every context.
   */
  private static class DataOpens extends FilterDirectory {
    final List<IOContext> opens = Collections.synchronizedList(new ArrayList<>());
    final boolean advises;
    volatile int closes;
    volatile boolean gone;
    volatile boolean failNextOpen;

    DataOpens(Directory in) {
      this(in, true);
    }

    DataOpens(Directory in, boolean advises) {
      super(in);
      this.advises = advises;
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
      if (name.endsWith("." + Lucene90CompoundFormat.DATA_EXTENSION) == false) {
        return super.openInput(name, context);
      }
      if (gone && opens.isEmpty() == false) {
        throw new NoSuchFileException(name);
      }
      if (failNextOpen) {
        failNextOpen = false;
        throw new IOException("simulated failure opening " + name);
      }
      opens.add(context);
      return new FilterIndexInput(name, super.openInput(name, context)) {
        @Override
        public void close() throws IOException {
          synchronized (DataOpens.this) {
            closes++;
          }
          super.close();
        }

        @Override
        public IndexInput clone() {
          return in.clone();
        }

        @Override
        public IndexInput slice(String description, long offset, long length) throws IOException {
          return in.slice(description, offset, length);
        }

        @Override
        public IndexInput slice(String description, long offset, long length, IOContext context)
            throws IOException {
          return new Advised(in.slice(description, offset, length, context), context, advises);
        }
      };
    }
  }

  /** A slice that reads like a memory-mapped input using ADVISE_BY_CONTEXT. */
  private static final class Advised extends FilterIndexInput {
    private final IOContext context;
    private final boolean advises;

    Advised(IndexInput in, IOContext context, boolean advises) {
      super("Advised(" + in + ")", in);
      this.context = context;
      this.advises = advises;
    }

    @Override
    public boolean readsAs(IOContext other) {
      return advises == false
          || MMapDirectory.ADVISE_BY_CONTEXT
              .apply("", context)
              .equals(MMapDirectory.ADVISE_BY_CONTEXT.apply("", other));
    }

    @Override
    public IndexInput clone() {
      return new Advised(in.clone(), context, advises);
    }

    @Override
    public IndexInput slice(String description, long offset, long length) throws IOException {
      return new Advised(in.slice(description, offset, length), context, advises);
    }
  }
}
