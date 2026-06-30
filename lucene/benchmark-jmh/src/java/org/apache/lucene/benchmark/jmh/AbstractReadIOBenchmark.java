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
package org.apache.lucene.benchmark.jmh;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Locale;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

/**
 * Abstract base for read I/O benchmarks. Provides shared FFI handles (pread, open, close,
 * posix_madvise, fcntl), thread-local buffers, and file/mmap/fd setups.
 */
@State(Scope.Benchmark)
public abstract class AbstractReadIOBenchmark {

  private static final int O_RDONLY = 0;
  private static final int O_DIRECT = 0x4000; // Linux
  private static final int F_NOCACHE = 48;

  private static final boolean IS_MAC =
      System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("mac");
  private static final boolean IS_LINUX =
      System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("linux");

  /** Max read size for buffer pre-allocation. Actual read size is a @Param on subclasses. */
  protected static final int MAX_READ_SIZE = 1024 * 1024;

  /**
   * Max reads per operation for buffer pre-allocation. Actual readsPerOp is a @Param on subclasses.
   */
  protected static final int MAX_READS_PER_OPERATION = 256;

  protected static final long FILE_SIZE =
      Long.parseLong(getConfigFromEnvOrProp("BENCH_FILE_SIZE_MB", "bench.fileSizeMB", "1024"))
          * 1024L
          * 1024L;

  protected static final String BENCH_FILE =
      getConfigFromEnvOrProp("BENCH_FILE", "bench.file", "/tmp/pread-bench.dat");

  protected static final boolean DROP_PAGE_CACHE =
      Boolean.parseBoolean(
          getConfigFromEnvOrProp("BENCH_DROP_CACHES", "bench.dropPageCache", "false"));

  protected static final int POSIX_MADV_RANDOM = 1;
  protected static final int POSIX_MADV_SEQUENTIAL = 2;
  protected static final int POSIX_MADV_WILLNEED = 3;

  // FFI handles
  protected static final MethodHandle PREAD;
  protected static final MethodHandle OPEN;
  protected static final MethodHandle CLOSE;
  protected static final MethodHandle POSIX_MADVISE;
  protected static final MethodHandle FCNTL;

  protected static final int PAGE_SIZE;

  static {
    final Linker linker = Linker.nativeLinker();
    final SymbolLookup stdlib = linker.defaultLookup();

    PREAD =
        findFunction(
            linker,
            stdlib,
            "pread",
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_INT,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG));

    OPEN =
        findFunction(
            linker,
            stdlib,
            "open",
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_INT));

    CLOSE =
        findFunction(
            linker,
            stdlib,
            "close",
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT));

    POSIX_MADVISE =
        findFunction(
            linker,
            stdlib,
            "posix_madvise",
            FunctionDescriptor.of(
                ValueLayout.JAVA_INT,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_INT));

    FCNTL =
        findFunction(
            linker,
            stdlib,
            "fcntl",
            FunctionDescriptor.of(
                ValueLayout.JAVA_INT,
                ValueLayout.JAVA_INT,
                ValueLayout.JAVA_INT,
                ValueLayout.JAVA_INT));

    try {
      PAGE_SIZE =
          (int)
              findFunction(
                      linker, stdlib, "getpagesize", FunctionDescriptor.of(ValueLayout.JAVA_INT))
                  .invokeExact();
    } catch (Throwable e) {
      throw new RuntimeException("getpagesize() failed", e);
    }
  }

  @SuppressWarnings("restricted") // unsafe functionality is used
  private static MethodHandle findFunction(
      Linker linker, SymbolLookup lookup, String name, FunctionDescriptor desc) {
    final MemorySegment symbol =
        lookup
            .find(name)
            .orElseThrow(
                () ->
                    new UnsupportedOperationException(
                        "Platform has no symbol for '" + name + "' in libc."));
    return linker.downcallHandle(symbol, desc);
  }

  /** Per-thread pre-allocated buffers. */
  @State(Scope.Thread)
  public static class ThreadBuffers {
    public ByteBuffer directBuf;
    public byte[] heapBuf;
    public Arena ffiArena;
    public MemorySegment ffiBuf;
    public MemorySegment ffiDirectIOBuf;
    public long[] offsets;

    @Setup(Level.Trial)
    public void setup() {
      directBuf = ByteBuffer.allocateDirect(MAX_READ_SIZE);
      heapBuf = new byte[MAX_READ_SIZE];
      ffiArena = Arena.ofConfined();
      ffiBuf = ffiArena.allocate(MAX_READ_SIZE);
      ffiDirectIOBuf = ffiArena.allocate(MAX_READ_SIZE, PAGE_SIZE);
      offsets = new long[MAX_READS_PER_OPERATION];
    }

    @TearDown(Level.Trial)
    public void tearDown() {
      ffiArena.close();
    }
  }

  protected Path benchFile;
  protected FileChannel fileChannel;
  protected MemorySegment mmapSegmentNormal;
  protected MemorySegment mmapSegmentSequential;
  protected MemorySegment mmapSegmentRandom;
  protected int preadFd;
  protected int directIOFd;
  protected Arena arena;

  protected void validateReadSize(int readSize) {
    if (readSize > MAX_READ_SIZE) {
      throw new IllegalArgumentException(
          "readSize (" + readSize + ") exceeds MAX_READ_SIZE (" + MAX_READ_SIZE + ").");
    }
  }

  protected void validateReadsPerOp(int readsPerOp) {
    if (readsPerOp > MAX_READS_PER_OPERATION) {
      throw new IllegalArgumentException(
          "readsPerOp ("
              + readsPerOp
              + ") exceeds MAX_READS_PER_OPERATION ("
              + MAX_READS_PER_OPERATION
              + ").");
    }
  }

  @Setup(Level.Trial)
  public void setup() throws Exception {
    benchFile = Path.of(BENCH_FILE);
    if (!Files.exists(benchFile)) {
      throw new IOException(
          "Benchmark file not found: "
              + benchFile
              + "\nCreate it with: dd if=/dev/urandom of="
              + BENCH_FILE
              + " bs=1M count="
              + (FILE_SIZE / (1024 * 1024)));
    }

    long size = Files.size(benchFile);
    if (size < FILE_SIZE) {
      throw new IOException(
          "Benchmark file too small: "
              + size
              + " bytes, expected at least "
              + FILE_SIZE
              + "\nRecreate with: dd if=/dev/urandom of="
              + BENCH_FILE
              + " bs=1M count="
              + (FILE_SIZE / (1024 * 1024)));
    }

    arena = Arena.ofShared();
    fileChannel = FileChannel.open(benchFile, StandardOpenOption.READ);
    try {
      setupMmap();
      setupPreadFd();
      setupDirectIOFd();
    } catch (Exception e) {
      tearDown();
      throw e;
    }
  }

  private void setupMmap() throws IOException {
    mmapSegmentNormal = fileChannel.map(MapMode.READ_ONLY, 0, FILE_SIZE, arena);
    mmapSegmentSequential = fileChannel.map(MapMode.READ_ONLY, 0, FILE_SIZE, arena);
    mmapSegmentRandom = fileChannel.map(MapMode.READ_ONLY, 0, FILE_SIZE, arena);
    madvise(mmapSegmentSequential, POSIX_MADV_SEQUENTIAL);
    madvise(mmapSegmentRandom, POSIX_MADV_RANDOM);
  }

  protected void madvise(MemorySegment segment, int advice) throws IOException {
    if (segment.byteSize() == 0L) {
      return;
    }
    final int rc;
    try {
      rc = (int) POSIX_MADVISE.invokeExact(segment, segment.byteSize(), advice);
    } catch (Throwable t) {
      throw new RuntimeException("posix_madvise failed", t);
    }
    if (rc != 0) {
      throw new IOException("posix_madvise(" + advice + ") returned " + rc);
    }
  }

  private void setupPreadFd() throws IOException {
    MemorySegment pathStr = arena.allocateFrom(benchFile.toString());
    try {
      preadFd = (int) OPEN.invokeExact(pathStr, O_RDONLY);
    } catch (Throwable t) {
      throw new RuntimeException("FFI open() failed", t);
    }
    if (preadFd < 0) {
      throw new IOException("open() for pread returned fd=" + preadFd);
    }
  }

  private void setupDirectIOFd() throws IOException {
    MemorySegment pathStr = arena.allocateFrom(benchFile.toString());
    try {
      if (IS_LINUX) {
        directIOFd = (int) OPEN.invokeExact(pathStr, O_RDONLY | O_DIRECT);
        if (directIOFd < 0) {
          throw new IOException("open(O_DIRECT) returned fd=" + directIOFd);
        }
      } else if (IS_MAC) {
        directIOFd = (int) OPEN.invokeExact(pathStr, O_RDONLY);
        if (directIOFd < 0) {
          throw new IOException("open() for direct I/O returned fd=" + directIOFd);
        }
        int rc = (int) FCNTL.invokeExact(directIOFd, F_NOCACHE, 1);
        if (rc != 0) {
          CLOSE.invokeExact(directIOFd);
          directIOFd = -1;
          throw new IOException("fcntl(F_NOCACHE) returned " + rc);
        }
      } else {
        // skip direct I/O benchmarks for other platforms
        directIOFd = -1;
      }
    } catch (IOException e) {
      throw e;
    } catch (Throwable t) {
      throw new RuntimeException("Failed to open for direct I/O", t);
    }
  }

  @TearDown(Level.Trial)
  @SuppressWarnings({"restricted", "unused"})
  public void tearDown() throws Exception {
    if (fileChannel != null) {
      fileChannel.close();
    }
    try {
      if (preadFd > 0) {
        int rc = (int) CLOSE.invokeExact(preadFd);
      }
      if (directIOFd > 0) {
        int rc = (int) CLOSE.invokeExact(directIOFd);
      }
    } catch (Throwable t) {
      throw new RuntimeException("Failed to close fd", t);
    }
    if (arena != null) {
      arena.close();
    }
  }

  @Setup(Level.Iteration)
  public void setupIteration() throws IOException {
    if (DROP_PAGE_CACHE) {
      dropPageCaches();
    }
  }

  private static void dropPageCaches() throws IOException {
    if (IS_MAC) {
      exec("/usr/bin/sudo", "purge");
    } else if (IS_LINUX) {
      exec("/usr/bin/sync");
      exec("/usr/bin/sudo", "/usr/bin/bash", "-c", "echo 3 > /proc/sys/vm/drop_caches");
    }
  }

  private static void exec(String... command) throws IOException {
    Process proc = new ProcessBuilder(command).inheritIO().start();
    try {
      int exitCode = proc.waitFor();
      if (exitCode != 0) {
        throw new IOException(
            "Command failed with exit code " + exitCode + ": " + String.join(" ", command));
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted waiting for: " + String.join(" ", command), e);
    }
  }

  /** Reads a config value from env var first, then system property, then default. */
  protected static String getConfigFromEnvOrProp(
      String envKey, String propKey, String defaultValue) {
    String env = System.getenv(envKey);
    if (env != null && !env.isEmpty()) {
      return env;
    }
    return System.getProperty(propKey, defaultValue);
  }
}
