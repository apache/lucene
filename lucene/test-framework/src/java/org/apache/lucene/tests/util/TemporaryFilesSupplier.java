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
package org.apache.lucene.tests.util;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.spi.FileSystemProvider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Random;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.lucene.tests.mockfile.DisableFsyncFS;
import org.apache.lucene.tests.mockfile.ExtrasFS;
import org.apache.lucene.tests.mockfile.HandleLimitFS;
import org.apache.lucene.tests.mockfile.LeakFS;
import org.apache.lucene.tests.mockfile.ShuffleFS;
import org.apache.lucene.tests.mockfile.VerboseFS;
import org.apache.lucene.tests.mockfile.WindowsFS;
import org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems;
import org.apache.lucene.tests.util.LuceneTestCase.SuppressFsync;
import org.apache.lucene.tests.util.LuceneTestCase.SuppressTempFileChecks;
import org.apache.lucene.util.IOUtils;

/**
 * Checks and cleans up temporary files.
 *
 * @see LuceneTestCase#createTempDir()
 * @see LuceneTestCase#createTempFile()
 */
final class TemporaryFilesSupplier implements BeforeAfterCallback {
  private final Supplier<Random> randomSupplier;
  private final Supplier<Class<?>> targetClassSupplier;

  /** Retry to create temporary file name this many times. */
  private static final int TEMP_NAME_RETRY_THRESHOLD = 9999;

  /** Writeable temporary base folder. */
  private Path javaTempDir;

  /** Per-test class temporary folder. */
  private Path tempDirBase;

  /** Per-test filesystem */
  private FileSystem fileSystem;

  /** Suite failure marker. */
  private final SuiteFailureState failureMarker;

  /**
   * A queue of paths to be removed after the suite completes.
   *
   * @see #registerToCloseAfterSuite(Path)
   */
  private static final ArrayList<Path> pathQueue = new ArrayList<>();

  /**
   * A queue of closeables to be closed after the suite completes.
   *
   * @see #registerToCloseAfterSuite(Closeable)
   */
  private static final ArrayList<Closeable> closeableQueue = new ArrayList<>();

  public TemporaryFilesSupplier(
      SuiteFailureState failureMarker,
      Supplier<Random> randomSupplier,
      Supplier<Class<?>> targetClassSupplier) {
    this.failureMarker = failureMarker;
    this.randomSupplier = randomSupplier;
    this.targetClassSupplier = targetClassSupplier;
  }

  /** Register temporary folder for removal after the suite completes. */
  void registerToCloseAfterSuite(Closeable c) {
    synchronized (closeableQueue) {
      closeableQueue.addLast(c);
    }
  }

  /** Register temporary folder for removal after the suite completes. */
  void registerToCloseAfterSuite(Path f) {
    assert f != null;

    if (LuceneTestCase.LEAVE_TEMPORARY) {
      System.err.println("INFO: Will leave temporary file: " + f.toAbsolutePath());
      return;
    }

    synchronized (pathQueue) {
      pathQueue.addLast(f);
    }
  }

  @Override
  public void before() throws Exception {
    assert tempDirBase == null;
    assert pathQueue.isEmpty() && closeableQueue.isEmpty();

    fileSystem = initializeFileSystem();
    if (fileSystem != FileSystems.getDefault()) {
      registerToCloseAfterSuite(fileSystem);
    }

    javaTempDir = initializeJavaTempDir();
  }

  private boolean allowed(Set<String> avoid, Class<? extends FileSystemProvider> clazz) {
    if (avoid.contains("*") || avoid.contains(clazz.getSimpleName())) {
      return false;
    } else {
      return true;
    }
  }

  private FileSystem initializeFileSystem() {
    Class<?> targetClass = targetClassSupplier.get();
    Set<String> avoid = new HashSet<>();
    if (targetClass.isAnnotationPresent(SuppressFileSystems.class)) {
      SuppressFileSystems a = targetClass.getAnnotation(SuppressFileSystems.class);
      avoid.addAll(Arrays.asList(a.value()));
    }
    FileSystem fs = FileSystems.getDefault();
    if (LuceneTestCase.INFOSTREAM && allowed(avoid, VerboseFS.class)) {
      fs =
          new VerboseFS(
                  fs,
                  new SetupAndRestoreStaticEnv.ThreadNameFixingPrintStreamInfoStream(System.out))
              .getFileSystem(null);
    }

    Random random = randomSupplier.get();

    // speed up tests by omitting actual fsync calls to the hardware most of the time.
    if (targetClass.isAnnotationPresent(SuppressFsync.class) || random.nextInt(100) > 0) {
      if (allowed(avoid, DisableFsyncFS.class)) {
        fs = new DisableFsyncFS(fs).getFileSystem(null);
      }
    }

    // impacts test reproducibility across platforms.
    if (random.nextInt(100) > 0) {
      if (allowed(avoid, ShuffleFS.class)) {
        fs = new ShuffleFS(fs, random.nextLong()).getFileSystem(null);
      }
    }

    // otherwise, wrap with mockfilesystems for additional checks. some
    // of these have side effects (e.g. concurrency) so it doesn't always happen.
    if (random.nextInt(10) > 0) {
      if (allowed(avoid, LeakFS.class)) {
        fs = new LeakFS(fs).getFileSystem(null);
      }
      if (allowed(avoid, HandleLimitFS.class)) {
        int limit = HandleLimitFS.MaxOpenHandles.MAX_OPEN_FILES;
        if (targetClass.isAnnotationPresent(HandleLimitFS.MaxOpenHandles.class)) {
          limit = targetClass.getAnnotation(HandleLimitFS.MaxOpenHandles.class).limit();
        }
        fs = new HandleLimitFS(fs, limit).getFileSystem(null);
      }
      // windows is currently slow
      if (random.nextInt(10) == 0 && allowed(avoid, WindowsFS.class)) {
        fs = new WindowsFS(fs).getFileSystem(null);
      }
      if (allowed(avoid, ExtrasFS.class)) {
        fs = new ExtrasFS(fs, random.nextInt(4) == 0, random.nextBoolean()).getFileSystem(null);
      }
    }
    if (LuceneTestCase.VERBOSE) {
      System.out.println("filesystem: " + fs.provider());
    }

    return fs.provider().getFileSystem(URI.create("file:///"));
  }

  private Path initializeJavaTempDir() throws IOException {
    Path javaTempDir =
        fileSystem.getPath(System.getProperty("tempDir", System.getProperty("java.io.tmpdir")));

    Files.createDirectories(javaTempDir);

    assert Files.isDirectory(javaTempDir) && Files.isWritable(javaTempDir);

    return javaTempDir.toRealPath();
  }

  @Override
  public void after() throws Exception {
    final String tempDirBasePath =
        (tempDirBase != null ? tempDirBase.toAbsolutePath().toString() : null);
    tempDirBase = null;

    // Drain cleanup queue and clear it.
    final Path[] paths;
    synchronized (pathQueue) {
      paths = pathQueue.reversed().toArray(Path[]::new);
      pathQueue.clear();
    }

    // Only check and throw an IOException on un-removable files if the test
    // was successful. Otherwise, just report the path of temporary files
    // and leave them there.
    if (failureMarker.wasSuccessful()) {
      registerToCloseAfterSuite(
          () -> {
            try {
              IOUtils.rm(paths);
            } catch (IOException e) {
              Class<?> suiteClass = targetClassSupplier.get();
              if (suiteClass.isAnnotationPresent(SuppressTempFileChecks.class)) {
                System.err.println(
                    "WARNING: Leftover undeleted temporary files (bugUrl: "
                        + suiteClass.getAnnotation(SuppressTempFileChecks.class).bugUrl()
                        + "): "
                        + e.getMessage());
                return;
              }
              throw e;
            }
          });
    } else {
      if (tempDirBasePath != null) {
        System.err.println("NOTE: leaving temporary files on disk at: " + tempDirBasePath);
      }
    }

    Closeable[] toClose;
    synchronized (closeableQueue) {
      toClose = closeableQueue.reversed().toArray(Closeable[]::new);
      closeableQueue.clear();
    }

    IOUtils.close(toClose);
  }

  Path getPerTestClassTempDir() {
    if (tempDirBase == null) {
      Class<?> clazz = targetClassSupplier.get();
      String prefix = clazz.getName();
      prefix = prefix.replaceFirst("^org.apache.lucene.", "lucene.");

      int attempt = 0;
      Path f;
      boolean success = false;
      do {
        if (attempt++ >= TEMP_NAME_RETRY_THRESHOLD) {
          throw new RuntimeException(
              "Failed to get a temporary name too many times, check your temp directory and consider manually cleaning it: "
                  + javaTempDir.toAbsolutePath());
        }
        f = javaTempDir.resolve(prefix + "_" + String.format(Locale.ENGLISH, "%03d", attempt));
        try {
          Files.createDirectory(f);
          success = true;
        } catch (IOException _) {
        }
      } while (!success);

      tempDirBase = f;
      registerToCloseAfterSuite(tempDirBase);
    }
    return tempDirBase;
  }

  /**
   * @see LuceneTestCase#createTempDir()
   */
  public Path createTempDir(String prefix) {
    Path base = getPerTestClassTempDir();

    int attempt = 0;
    Path f;
    boolean success = false;
    do {
      if (attempt++ >= TEMP_NAME_RETRY_THRESHOLD) {
        throw new RuntimeException(
            "Failed to get a temporary name too many times, check your temp directory and consider manually cleaning it: "
                + base.toAbsolutePath());
      }
      f = base.resolve(prefix + "-" + String.format(Locale.ENGLISH, "%03d", attempt));
      try {
        Files.createDirectory(f);
        success = true;
      } catch (IOException _) {
      }
    } while (!success);

    registerToCloseAfterSuite(f);
    return f;
  }

  /**
   * @see LuceneTestCase#createTempFile()
   */
  public Path createTempFile(String prefix, String suffix) throws IOException {
    Path base = getPerTestClassTempDir();

    int attempt = 0;
    Path f;
    boolean success = false;
    do {
      if (attempt++ >= TEMP_NAME_RETRY_THRESHOLD) {
        throw new RuntimeException(
            "Failed to get a temporary name too many times, check your temp directory and consider manually cleaning it: "
                + base.toAbsolutePath());
      }
      f = base.resolve(prefix + "-" + String.format(Locale.ENGLISH, "%03d", attempt) + suffix);
      try {
        Files.createFile(f);
        success = true;
      } catch (IOException _) {
      }
    } while (!success);

    registerToCloseAfterSuite(f);
    return f;
  }
}
