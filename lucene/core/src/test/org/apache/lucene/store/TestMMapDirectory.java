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

import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.apache.lucene.tests.store.BaseDirectoryTestCase;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.NamedThreadFactory;

/** Tests MMapDirectory */
// See: https://issues.apache.org/jira/browse/SOLR-12028 Tests cannot remove files on Windows
// machines occasionally
public class TestMMapDirectory extends BaseDirectoryTestCase {

  @Override
  protected Directory getDirectory(Path path) throws IOException {
    MMapDirectory m = new MMapDirectory(path);
    m.setPreload((file, context) -> random().nextBoolean());
    return m;
  }

  public void testAceWithThreads() throws Exception {
    final int nInts = 8 * 1024 * 1024;

    try (Directory dir = getDirectory(createTempDir("testAceWithThreads"))) {
      try (IndexOutput out = dir.createOutput("test", IOContext.DEFAULT)) {
        final Random random = random();
        for (int i = 0; i < nInts; i++) {
          out.writeInt(random.nextInt());
        }
      }

      final int iters = RANDOM_MULTIPLIER * (TEST_NIGHTLY ? 50 : 10);
      for (int iter = 0; iter < iters; iter++) {
        final IndexInput in = dir.openInput("test", IOContext.DEFAULT);
        final IndexInput clone = in.clone();
        final byte[] accum = new byte[nInts * Integer.BYTES];
        final CountDownLatch shotgun = new CountDownLatch(1);
        final Thread t1 =
            new Thread(
                () -> {
                  try {
                    shotgun.await();
                    for (int i = 0; i < 10; i++) {
                      clone.seek(0);
                      clone.readBytes(accum, 0, accum.length);
                    }
                  } catch (
                      @SuppressWarnings("unused")
                      AlreadyClosedException ok) {
                    // OK
                  } catch (InterruptedException | IOException e) {
                    throw new RuntimeException(e);
                  }
                });
        t1.start();
        shotgun.countDown();
        // this triggers "bad behaviour": closing input while other threads are running
        in.close();
        t1.join();
      }
    }
  }

  public void testNullParamsIndexInput() throws Exception {
    try (Directory mmapDir = getDirectory(createTempDir("testNullParamsIndexInput"))) {
      try (IndexOutput out = mmapDir.createOutput("bytes", newIOContext(random()))) {
        out.alignFilePointer(16);
      }
      try (IndexInput in = mmapDir.openInput("bytes", IOContext.DEFAULT)) {
        assertThrows(NullPointerException.class, () -> in.readBytes(null, 0, 1));
        assertThrows(NullPointerException.class, () -> in.readFloats(null, 0, 1));
        assertThrows(NullPointerException.class, () -> in.readLongs(null, 0, 1));
      }
    }
  }

  public void testMadviseAvail() throws Exception {
    assertEquals(
        "madvise should be supported on Linux and Macos",
        Constants.LINUX || Constants.MAC_OS_X,
        MMapDirectory.supportsMadvise());
  }

  // Opens the input with ReadAdvice.NORMAL to ensure basic code path coverage.
  public void testWithNormal() throws Exception {
    final int size = 8 * 1024;
    byte[] bytes = new byte[size];
    random().nextBytes(bytes);

    try (Directory dir = new MMapDirectory(createTempDir("testWithRandom"))) {
      try (IndexOutput out = dir.createOutput("test", IOContext.DEFAULT)) {
        out.writeBytes(bytes, 0, bytes.length);
      }

      try (final IndexInput in =
          dir.openInput("test", IOContext.DEFAULT.withReadAdvice(ReadAdvice.NORMAL))) {
        final byte[] readBytes = new byte[size];
        in.readBytes(readBytes, 0, readBytes.length);
        assertArrayEquals(bytes, readBytes);
      }
    }
  }

  // Opens the input with ReadAdvice.READONCE to ensure slice and clone are appropriately confined
  public void testConfined() throws Exception {
    final int size = 16;
    byte[] bytes = new byte[size];
    random().nextBytes(bytes);

    try (Directory dir = new MMapDirectory(createTempDir("testConfined"))) {
      try (IndexOutput out = dir.createOutput("test", IOContext.DEFAULT)) {
        out.writeBytes(bytes, 0, bytes.length);
      }

      try (var in = dir.openInput("test", IOContext.READONCE);
          var executor = Executors.newFixedThreadPool(1, new NamedThreadFactory("testConfined"))) {
        // ensure accessible
        assertEquals(16L, in.slice("test", 0, in.length()).length());
        assertEquals(15L, in.slice("test", 1, in.length() - 1).length());

        // ensure not accessible
        Callable<Object> task1 = () -> in.slice("test", 0, in.length());
        var x = expectThrows(ISE, () -> getAndUnwrap(executor.submit(task1)));
        assertTrue(x.getMessage().contains("confined"));

        int offset = random().nextInt((int) in.length());
        int length = (int) in.length() - offset;
        Callable<Object> task2 = () -> in.slice("test", offset, length);
        x = expectThrows(ISE, () -> getAndUnwrap(executor.submit(task2)));
        assertTrue(x.getMessage().contains("confined"));

        // slice.slice
        var slice = in.slice("test", 0, in.length());
        Callable<Object> task3 = () -> slice.slice("test", 0, in.length());
        x = expectThrows(ISE, () -> getAndUnwrap(executor.submit(task3)));
        assertTrue(x.getMessage().contains("confined"));
        // slice.clone
        x = expectThrows(ISE, () -> getAndUnwrap(executor.submit(slice::clone)));
        assertTrue(x.getMessage().contains("confined"));
      }
    }
  }

  static final Class<IllegalStateException> ISE = IllegalStateException.class;

  static Object getAndUnwrap(Future<Object> future) throws Throwable {
    try {
      return future.get();
    } catch (ExecutionException ee) {
      throw ee.getCause();
    }
  }

  public void testArenas() throws Exception {
    Supplier<String> randomGenerationOrNone =
        () -> random().nextBoolean() ? "_" + random().nextInt(5) : "";
    // First, create a number of segment specific file name lists to test with
    var exts =
        List.of(
            ".si", ".cfs", ".cfe", ".dvd", ".dvm", ".nvd", ".nvm", ".fdt", ".vec", ".vex", ".vemf");
    var names =
        IntStream.range(0, 50)
            .mapToObj(i -> "_" + i + randomGenerationOrNone.get())
            .flatMap(s -> exts.stream().map(ext -> s + ext))
            .collect(toList());
    // Second, create a number of non-segment file names
    IntStream.range(0, 50).mapToObj(i -> "foo" + i).forEach(names::add);
    Collections.shuffle(names, random());

    final int size = 6;
    byte[] bytes = new byte[size];
    random().nextBytes(bytes);

    try (var dir = new MMapDirectory(createTempDir("testArenas"))) {
      for (var name : names) {
        try (IndexOutput out = dir.createOutput(name, IOContext.DEFAULT)) {
          out.writeBytes(bytes, 0, bytes.length);
        }
      }

      int nThreads = 10;
      int perListSize = (names.size() + nThreads) / nThreads;
      List<List<String>> nameLists =
          IntStream.range(0, nThreads)
              .mapToObj(
                  i ->
                      names.subList(
                          perListSize * i, Math.min(perListSize * i + perListSize, names.size())))
              .toList();

      var threadFactory = new NamedThreadFactory("testArenas");
      try (var executor = Executors.newFixedThreadPool(nThreads, threadFactory)) {
        var tasks = nameLists.stream().map(l -> new IndicesOpenTask(l, dir)).toList();
        var futures = tasks.stream().map(executor::submit).toList();
        for (var future : futures) {
          future.get();
        }
      }

      if (!(dir.attachment instanceof ConcurrentHashMap<?, ?> map)) {
        throw new AssertionError("unexpected attachment: " + dir.attachment);
      }
      assertEquals(0, map.size());
    }
  }

  static class IndicesOpenTask implements Callable<Void> {
    final List<String> names;
    final Directory dir;

    IndicesOpenTask(List<String> names, Directory dir) {
      this.names = names;
      this.dir = dir;
    }

    @Override
    public Void call() throws Exception {
      List<IndexInput> closeables = new ArrayList<>();
      for (var name : names) {
        closeables.add(dir.openInput(name, IOContext.DEFAULT));
      }
      for (IndexInput closeable : closeables) {
        closeable.close();
      }
      return null;
    }
  }

  // Opens more files in the same group than the ref counting limit.
  public void testArenasManySegmentFiles() throws Exception {
    var names = IntStream.range(0, 1024).mapToObj(i -> "_001.ext" + i).toList();

    final int size = 4;
    byte[] bytes = new byte[size];
    random().nextBytes(bytes);

    try (var dir = new MMapDirectory(createTempDir("testArenasManySegmentFiles"))) {
      for (var name : names) {
        try (IndexOutput out = dir.createOutput(name, IOContext.DEFAULT)) {
          out.writeBytes(bytes, 0, bytes.length);
        }
      }

      List<IndexInput> closeables = new ArrayList<>();
      for (var name : names) {
        closeables.add(dir.openInput(name, IOContext.DEFAULT));
      }
      for (IndexInput closeable : closeables) {
        closeable.close();
      }

      if (!(dir.attachment instanceof ConcurrentHashMap<?, ?> map)) {
        throw new AssertionError("unexpected attachment: " + dir.attachment);
      }
      assertEquals(0, map.size());
    }
  }

  public void testGroupBySegmentFunc() {
    var func = MMapDirectory.GROUP_BY_SEGMENT;
    assertEquals("0", func.apply("_0.doc").orElseThrow());
    assertEquals("51", func.apply("_51.si").orElseThrow());
    assertEquals("51-g", func.apply("_51_1.si").orElseThrow());
    assertEquals("51-g", func.apply("_51_1_gg_ff.si").orElseThrow());
    assertEquals("51-g", func.apply("_51_2_gg_ff.si").orElseThrow());
    assertEquals("51-g", func.apply("_51_3_gg_ff.si").orElseThrow());
    assertEquals("5987654321", func.apply("_5987654321.si").orElseThrow());
    assertEquals("f", func.apply("_f.si").orElseThrow());
    assertEquals("ff", func.apply("_ff.si").orElseThrow());
    assertEquals("51a", func.apply("_51a.si").orElseThrow());
    assertEquals("f51a", func.apply("_f51a.si").orElseThrow());
    assertEquals("segment", func.apply("_segment.si").orElseThrow());

    // old style
    assertEquals("5", func.apply("_5_Lucene90FieldsIndex-doc_ids_0.tmp").orElseThrow());

    assertFalse(func.apply("").isPresent());
    assertFalse(func.apply("_").isPresent());
    assertFalse(func.apply("_.si").isPresent());
    assertFalse(func.apply("foo").isPresent());
    assertFalse(func.apply("_foo").isPresent());
    assertFalse(func.apply("__foo").isPresent());
    assertFalse(func.apply("_segment").isPresent());
    assertFalse(func.apply("segment.si").isPresent());
  }

  public void testNoGroupingFunc() {
    var func = MMapDirectory.NO_GROUPING;
    assertFalse(func.apply("_0.doc").isPresent());
    assertFalse(func.apply("_0.si").isPresent());
    assertFalse(func.apply("_54.si").isPresent());
    assertFalse(func.apply("_ff.si").isPresent());
    assertFalse(func.apply("_.si").isPresent());
    assertFalse(func.apply("foo").isPresent());
    assertFalse(func.apply("_foo").isPresent());
    assertFalse(func.apply("__foo").isPresent());
    assertFalse(func.apply("_segment").isPresent());
    assertFalse(func.apply("_segment.si").isPresent());
    assertFalse(func.apply("segment.si").isPresent());
    assertFalse(func.apply("_51a.si").isPresent());
  }
}
