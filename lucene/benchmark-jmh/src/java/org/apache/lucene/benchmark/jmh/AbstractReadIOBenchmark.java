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
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

/**
 * Abstract base for read I/O benchmarks. Provides shared FFI handles (pread, open, close,
 * posix_madvise), thread-local buffers, file/mmap setup, and configuration parsing.
 *
 * <p>Subclasses define their own offset bounds and benchmark methods.
 */
@State(Scope.Benchmark)
@SuppressWarnings("restricted")
public abstract class AbstractReadIOBenchmark {

  protected static final int READ_SIZE = 4 * 1024; // 4 KiB
  protected static final int READS_PER_OP = 16;
  protected static final long ALIGNMENT = 4096;

  protected static final long FILE_SIZE =
      Long.parseLong(envOrProp("BENCH_FILE_SIZE_MIB", "bench.fileSizeMiB", "1024")) * 1024L * 1024L;

  protected static final String BENCH_FILE =
      envOrProp("BENCH_FILE", "bench.file", "/tmp/pread-bench.dat");

  protected static final boolean DROP_CACHES =
      Boolean.parseBoolean(envOrProp("BENCH_DROP_CACHES", "bench.dropCaches", "false"));

  protected static final int MADV_NORMAL = 0;
  protected static final int MADV_RANDOM = 1;
  protected static final int MADV_WILLNEED = 3;

  // FFI handles
  protected static final MethodHandle PREAD;
  protected static final MethodHandle OPEN;
  protected static final MethodHandle CLOSE;
  protected static final MethodHandle POSIX_MADVISE;

  static {
    Linker linker = Linker.nativeLinker();
    SymbolLookup lookup = linker.defaultLookup();

    PREAD =
        linker.downcallHandle(
            lookup.find("pread").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_INT,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG));

    OPEN =
        linker.downcallHandle(
            lookup.find("open").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_INT));

    CLOSE =
        linker.downcallHandle(
            lookup.find("close").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT));

    POSIX_MADVISE =
        linker.downcallHandle(
            lookup.find("posix_madvise").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_INT,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_INT));
  }

  /** Per-thread pre-allocated buffers to avoid allocation noise in the measured path. */
  @State(Scope.Thread)
  public static class ThreadBuffers {
    public ByteBuffer directBuf;
    public ByteBuffer heapBuf;
    public Arena ffiArena;
    public MemorySegment ffiBuf;
    public MemorySegment ffiDirectIoBuf;

    @Setup(Level.Trial)
    public void setup() {
      directBuf = ByteBuffer.allocateDirect(READ_SIZE);
      heapBuf = ByteBuffer.allocate(READ_SIZE);
      ffiArena = Arena.ofConfined();
      ffiBuf = ffiArena.allocate(READ_SIZE);
      ffiDirectIoBuf = ffiArena.allocate(READ_SIZE, 4096);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
      ffiArena.close();
    }
  }

  protected Path tempFile;
  protected FileChannel fileChannel;
  protected MemorySegment mmapSegmentNormal;
  protected MemorySegment mmapSegmentMadvRandom;
  protected int nativeFd;
  protected int directIoFd;
  protected Arena arena;

  /** Subclasses return a name for logging (e.g. "RandomReadIOBenchmark"). */
  protected abstract String benchmarkName();

  /** Subclasses may print extra config lines. */
  protected void printExtraConfig() {}

  @Setup(Level.Trial)
  public void setup() throws Exception {
    System.out.println("[bench] ===== " + benchmarkName() + " Configuration =====");
    System.out.println("[bench]   file:       " + BENCH_FILE);
    System.out.println("[bench]   fileSizeMiB:  " + (FILE_SIZE / (1024 * 1024)));
    System.out.println("[bench]   dropCaches:   " + DROP_CACHES);
    System.out.println("[bench]   readSize:     " + READ_SIZE + " bytes");
    System.out.println("[bench]   readsPerOp:   " + READS_PER_OP);
    printExtraConfig();
    System.out.println("[bench] ===============================================");

    tempFile = Path.of(BENCH_FILE);
    if (!Files.exists(tempFile)) {
      throw new IOException(
          "Benchmark file not found: "
              + tempFile
              + "\nCreate it with: dd if=/dev/urandom of="
              + BENCH_FILE
              + " bs=1M count="
              + (FILE_SIZE / (1024 * 1024)));
    }
    long size = Files.size(tempFile);
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

    fileChannel = FileChannel.open(tempFile, StandardOpenOption.READ);

    arena = Arena.ofShared();

    mmapSegmentNormal = fileChannel.map(MapMode.READ_ONLY, 0, FILE_SIZE, arena);

    mmapSegmentMadvRandom = fileChannel.map(MapMode.READ_ONLY, 0, FILE_SIZE, arena);
    try {
      int rc = (int) POSIX_MADVISE.invokeExact(mmapSegmentMadvRandom, FILE_SIZE, MADV_RANDOM);
      if (rc != 0) {
        System.err.println("WARNING: posix_madvise(MADV_RANDOM) returned " + rc);
      }
    } catch (Throwable t) {
      throw new RuntimeException("posix_madvise(MADV_RANDOM) failed", t);
    }

    MemorySegment pathStr = arena.allocateFrom(tempFile.toString());
    int O_RDONLY = 0;
    try {
      nativeFd = (int) OPEN.invokeExact(pathStr, O_RDONLY);
    } catch (Throwable t) {
      throw new RuntimeException("Failed to open file via FFI", t);
    }
    if (nativeFd < 0) {
      throw new IOException("FFI open() returned " + nativeFd);
    }

    // Open native fd with O_DIRECT for Direct I/O (Linux only, bypasses page cache)
    int O_DIRECT = 0x4000;
    try {
      directIoFd = (int) OPEN.invokeExact(pathStr, O_RDONLY | O_DIRECT);
    } catch (Throwable t) {
      throw new RuntimeException("Failed to open file with O_DIRECT via FFI", t);
    }
    if (directIoFd < 0) {
      System.err.println(
          "WARNING: O_DIRECT open failed (fd="
              + directIoFd
              + "). "
              + "Direct I/O benchmarks will fail. Use a filesystem that supports O_DIRECT.");
      directIoFd = -1;
    }
  }

  @TearDown(Level.Trial)
  public void tearDown() throws Exception {
    fileChannel.close();
    try {
      int rc = (int) CLOSE.invokeExact(nativeFd);
      if (directIoFd >= 0) {
        rc = (int) CLOSE.invokeExact(directIoFd);
      }
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
    arena.close();
  }

  /**
   * Drops page caches before each iteration (warmup and measurement). This ensures each iteration
   * starts with a cold page cache. JIT still warms up across iterations since the JVM persists
   * across the fork.
   */
  @Setup(Level.Iteration)
  public void setupIteration() throws IOException {
    if (DROP_CACHES) {
      dropPageCaches();
    }
  }

  /**
   * Drops the kernel page cache to simulate cold-cache / memory-constrained scenarios. Requires
   * running as root or with passwordless sudo. Uses: sync && echo 3 > /proc/sys/vm/drop_caches
   */
  private static void dropPageCaches() throws IOException {
    Process sync = new ProcessBuilder("/usr/bin/sync").inheritIO().start();
    try {
      if (sync.waitFor() != 0) {
        throw new IOException("sync failed with exit code " + sync.exitValue());
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted during sync", e);
    }

    Process drop =
        new ProcessBuilder("/usr/bin/sudo", "/usr/bin/bash", "-c", "echo 3 > /proc/sys/vm/drop_caches")
            .inheritIO()
            .start();
    try {
      if (drop.waitFor() != 0) {
        throw new IOException(
            "Failed to drop page caches (exit code "
                + drop.exitValue()
                + "). Run as root or with: sudo sysctl vm.drop_caches=3");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted during drop_caches", e);
    }
    System.out.println("[bench] Page caches dropped.");
  }

  /** Reads a config value from env var first, then system property, then default. */
  protected static String envOrProp(String envKey, String propKey, String defaultValue) {
    String env = System.getenv(envKey);
    if (env != null && !env.isEmpty()) {
      return env;
    }
    return System.getProperty(propKey, defaultValue);
  }
}
