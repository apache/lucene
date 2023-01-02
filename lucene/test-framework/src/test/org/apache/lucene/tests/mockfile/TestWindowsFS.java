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
package org.apache.lucene.tests.mockfile;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Random;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.lucene.util.Constants;

/** Basic tests for WindowsFS */
public class TestWindowsFS extends MockFileSystemTestCase {

  @Override
  public void setUp() throws Exception {
    super.setUp();
    // irony: currently we don't emulate windows well enough to work on windows!
    // TODO: Can we fork this class and create a new class with only those tests that can run on
    // Windows and then check if
    // the Lucene WindowsFS error is same as the one OG Windows throws
    assumeFalse("windows is not supported", Constants.WINDOWS);
  }

  @Override
  protected Path wrap(Path path) {
    WindowsFS provider = new WindowsFS(path.getFileSystem());
    return provider.wrapPath(path);
  }

  /** Test Files.delete fails if a file has an open inputstream against it */
  public void testDeleteOpenFile() throws IOException {
    Path dir = wrap(createTempDir());

    OutputStream file = Files.newOutputStream(dir.resolve("stillopen"));
    file.write(5);
    file.close();
    InputStream is = Files.newInputStream(dir.resolve("stillopen"));

    IOException e = expectThrows(IOException.class, () -> Files.delete(dir.resolve("stillopen")));
    assertTrue(e.getMessage().contains("access denied"));
    is.close();
  }

  /** Test Files.deleteIfExists fails if a file has an open inputstream against it */
  public void testDeleteIfExistsOpenFile() throws IOException {
    Path dir = wrap(createTempDir());

    OutputStream file = Files.newOutputStream(dir.resolve("stillopen"));
    file.write(5);
    file.close();
    InputStream is = Files.newInputStream(dir.resolve("stillopen"));

    IOException e =
        expectThrows(IOException.class, () -> Files.deleteIfExists(dir.resolve("stillopen")));
    assertTrue(e.getMessage().contains("access denied"));
    is.close();
  }

  /** Test Files.rename fails if a file has an open inputstream against it */
  // TODO: what does windows do here?
  public void testRenameOpenFile() throws IOException {
    Path dir = wrap(createTempDir());

    OutputStream file = Files.newOutputStream(dir.resolve("stillopen"));
    file.write(5);
    file.close();
    InputStream is = Files.newInputStream(dir.resolve("stillopen"));

    IOException e =
        expectThrows(
            IOException.class,
            () ->
                Files.move(
                    dir.resolve("stillopen"),
                    dir.resolve("target"),
                    StandardCopyOption.ATOMIC_MOVE));
    assertTrue(e.getMessage().contains("access denied"));
    is.close();
  }

  public void testOpenDeleteConcurrently() throws IOException, Exception {
    final Path dir = wrap(createTempDir());
    final Path file = dir.resolve("thefile");
    final CyclicBarrier barrier = new CyclicBarrier(2);
    final AtomicBoolean stopped = new AtomicBoolean(false);
    Thread t =
        new Thread() {
          @Override
          public void run() {
            try {
              barrier.await();
            } catch (Exception ex) {
              throw new RuntimeException(ex);
            }
            while (stopped.get() == false) {
              try {
                if (random().nextBoolean()) {
                  Files.delete(file);
                } else if (random().nextBoolean()) {
                  Files.deleteIfExists(file);
                } else {
                  Path target = file.resolveSibling("other");
                  Files.move(file, target);
                  Files.delete(target);
                }
              } catch (
                  @SuppressWarnings("unused")
                  IOException ignored) {
                // continue
              }
            }
          }
        };
    t.start();
    barrier.await();
    try {
      final int iters = 10 + random().nextInt(100);
      for (int i = 0; i < iters; i++) {
        boolean opened = false;
        try (OutputStream stream = Files.newOutputStream(file)) {
          opened = true;
          stream.write(0);
          // just create
        } catch (@SuppressWarnings("unused") FileNotFoundException | NoSuchFileException ex) {
          assertEquals(
              "File handle leaked - file is closed but still registered",
              0,
              ((WindowsFS) dir.getFileSystem().provider()).openFiles.size());
          assertFalse("caught FNF on close", opened);
        }
        assertEquals(
            "File handle leaked - file is closed but still registered",
            0,
            ((WindowsFS) dir.getFileSystem().provider()).openFiles.size());
        Files.deleteIfExists(file);
      }
    } finally {
      stopped.set(true);
      t.join();
    }
  }

  public void testMove() throws IOException {
    Path dir = wrap(createTempDir());
    OutputStream file = Files.newOutputStream(dir.resolve("file"));
    file.write(1);
    file.close();
    Files.move(dir.resolve("file"), dir.resolve("target"));
    assertTrue(Files.exists(dir.resolve("target")));
    assertFalse(Files.exists(dir.resolve("file")));
    try (InputStream stream = Files.newInputStream(dir.resolve("target"))) {
      assertEquals(1, stream.read());
    }
    file = Files.newOutputStream(dir.resolve("otherFile"));
    file.write(2);
    file.close();

    Files.move(
        dir.resolve("otherFile"), dir.resolve("target"), StandardCopyOption.REPLACE_EXISTING);
    assertTrue(Files.exists(dir.resolve("target")));
    assertFalse(Files.exists(dir.resolve("otherFile")));
    try (InputStream stream = Files.newInputStream(dir.resolve("target"))) {
      assertEquals(2, stream.read());
    }
  }

  public void testFileName() {
    Character[] reservedCharacters = WindowsPath.RESERVED_CHARACTERS.toArray(new Character[0]);
    String[] reservedNames = WindowsPath.RESERVED_NAMES.toArray(new String[0]);
    String fileName;
    Random r = random();
    Path dir = wrap(createTempDir());

    if (r.nextBoolean()) {
      // We need at least one char after the special char
      // For example, in case of '/' we interpret `foo/` to just be a file `foo` which is valid
      fileName =
          RandomStrings.randomAsciiLettersOfLength(r, r.nextInt(10))
              + reservedCharacters[r.nextInt(reservedCharacters.length)]
              + RandomStrings.randomAsciiLettersOfLength(r, 1 + r.nextInt(9));
    } else {
      fileName = reservedNames[r.nextInt(reservedNames.length)];
    }
    expectThrows(InvalidPathException.class, () -> dir.resolve(fileName));

    // some other basic tests
    expectThrows(InvalidPathException.class, () -> dir.resolve("foo:bar"));
    expectThrows(InvalidPathException.class, () -> dir.resolve("foo:bar:tar"));
    expectThrows(InvalidPathException.class, () -> dir.resolve("foo?bar"));
    expectThrows(InvalidPathException.class, () -> dir.resolve("foo<bar"));
    expectThrows(InvalidPathException.class, () -> dir.resolve("foo\\bar"));
    expectThrows(InvalidPathException.class, () -> dir.resolve("foo*bar|tar"));
    expectThrows(InvalidPathException.class, () -> dir.resolve("foo|bar?tar"));
  }
}
