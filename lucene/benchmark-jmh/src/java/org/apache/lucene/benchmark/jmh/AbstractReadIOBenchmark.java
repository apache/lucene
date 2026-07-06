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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.ReadAdvice;
import org.apache.lucene.util.Constants;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;

/**
 * Abstract base for read I/O benchmarks. Provides shared FFI handles (pread, open, close, fcntl),
 * Lucene Directory/IndexInput setups, and per-thread state.
 */
public abstract class AbstractReadIOBenchmark {

  /** POSIX open(2) flag: read-only. */
  private static final int O_RDONLY = 0;

  /** Linux open(2) flag: bypass the page cache (requires aligned buffers and offsets). */
  private static final int O_DIRECT = 0x4000;

  /** macOS fcntl(2) command: disable unified buffer cache for this fd (equivalent to O_DIRECT). */
  private static final int F_NOCACHE = 48;

  protected static final long FILE_SIZE =
      Long.parseLong(getConfigFromEnvOrProp("BENCH_FILE_SIZE_MB", "bench.fileSizeMB", "1024"))
          * 1024L
          * 1024L;

  protected static final String BENCH_FILE =
      getConfigFromEnvOrProp("BENCH_FILE", "bench.file", "/tmp/pread-bench.dat");

  protected static final boolean DROP_PAGE_CACHE =
      Boolean.parseBoolean(
          getConfigFromEnvOrProp("BENCH_DROP_CACHES", "bench.dropPageCache", "false"));

  protected static final MethodHandle PREAD;
  protected static final MethodHandle OPEN;
  protected static final MethodHandle CLOSE;
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

  @SuppressWarnings("restricted")
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

  protected MMapDirectory mmapDir;
  protected IndexInput mmapInput;

  protected MMapDirectory mmapSequentialDir;
  protected IndexInput mmapSequentialInput;

  protected MMapDirectory mmapRandomDir;
  protected IndexInput mmapRandomInput;

  protected NIOFSDirectory niofsDir;
  protected IndexInput niofsInput;

  // FFI pread file descriptors
  protected int preadFd;
  protected int directIOFd;
  protected Arena ffiArena;

  @Setup(Level.Trial)
  public void setup() throws Exception {
    Path benchFile = Path.of(BENCH_FILE);
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

    String fileName = benchFile.getFileName().toString();
    Path dirPath = benchFile.getParent();

    setupMMapAndNIOFSDirectory(dirPath, fileName);

    // Setup FFI pread file descriptors
    ffiArena = Arena.ofShared();
    setupPreadFd(benchFile);
    setupDirectIOFd(benchFile);
  }

  private void setupMMapAndNIOFSDirectory(Path dirPath, String fileName) throws IOException {
    mmapDir = new MMapDirectory(dirPath);
    mmapInput = mmapDir.openInput(fileName, IOContext.DEFAULT);

    mmapSequentialDir = new MMapDirectory(dirPath);
    mmapSequentialDir.setReadAdvice(
        (name, context) -> {
          if (name.equals(fileName)) {
            return Optional.of(ReadAdvice.SEQUENTIAL);
          }
          return Optional.empty();
        });
    mmapSequentialInput = mmapSequentialDir.openInput(fileName, IOContext.DEFAULT);

    mmapRandomDir = new MMapDirectory(dirPath);
    mmapRandomDir.setReadAdvice(
        (name, context) -> {
          if (name.equals(fileName)) {
            return Optional.of(ReadAdvice.RANDOM);
          }
          return Optional.empty();
        });
    mmapRandomInput = mmapRandomDir.openInput(fileName, IOContext.DEFAULT);

    niofsDir = new NIOFSDirectory(dirPath);
    niofsInput = niofsDir.openInput(fileName, IOContext.DEFAULT);
  }

  private void setupPreadFd(Path benchFile) throws IOException {
    MemorySegment pathStr = ffiArena.allocateFrom(benchFile.toString());
    try {
      preadFd = (int) OPEN.invokeExact(pathStr, O_RDONLY);
    } catch (Throwable t) {
      throw new RuntimeException("FFI open() failed", t);
    }
    if (preadFd < 0) {
      throw new IOException("open() for pread returned fd=" + preadFd);
    }
  }

  private void setupDirectIOFd(Path benchFile) throws IOException {
    MemorySegment pathStr = ffiArena.allocateFrom(benchFile.toString());
    try {
      if (Constants.LINUX) {
        directIOFd = (int) OPEN.invokeExact(pathStr, O_RDONLY | O_DIRECT);
        if (directIOFd < 0) {
          throw new IOException("open(O_DIRECT) returned fd=" + directIOFd);
        }
      } else if (Constants.MAC_OS_X) {
        directIOFd = (int) OPEN.invokeExact(pathStr, O_RDONLY);
        if (directIOFd < 0) {
          throw new IOException("open() for direct I/O returned fd=" + directIOFd);
        }
        int rc = (int) FCNTL.invokeExact(directIOFd, F_NOCACHE, 1);
        if (rc != 0) {
          CLOSE.invokeExact(directIOFd);
          throw new IOException("fcntl(F_NOCACHE) returned " + rc);
        }
      } else {
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
    // Close Lucene IndexInputs and Directories
    if (mmapInput != null) {
      mmapInput.close();
    }
    if (mmapSequentialInput != null) {
      mmapSequentialInput.close();
    }
    if (mmapRandomInput != null) {
      mmapRandomInput.close();
    }
    if (mmapDir != null) {
      mmapDir.close();
    }
    if (mmapSequentialDir != null) {
      mmapSequentialDir.close();
    }
    if (mmapRandomDir != null) {
      mmapRandomDir.close();
    }
    if (niofsInput != null) {
      niofsInput.close();
    }
    if (niofsDir != null) {
      niofsDir.close();
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
    if (ffiArena != null) {
      ffiArena.close();
    }
  }

  @Setup(Level.Iteration)
  public void setupIteration() throws IOException {
    if (DROP_PAGE_CACHE) {
      dropPageCaches();
    }
  }

  private static void dropPageCaches() throws IOException {
    if (Constants.MAC_OS_X) {
      exec("/usr/bin/sudo", "purge");
    } else if (Constants.LINUX) {
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

  public static class BaseThreadState {
    public IndexInput mmapInput;
    public IndexInput mmapSequentialInput;
    public IndexInput mmapRandomInput;
    public IndexInput niofsInput;
    public Arena ffiArena;
    public MemorySegment ffiBuf;
    public MemorySegment ffiDirectIOBuf;
    public byte[] heapBuf;

    public void init(AbstractReadIOBenchmark bench, int readSize) throws IOException {
      mmapInput = bench.mmapInput.clone();
      mmapSequentialInput = bench.mmapSequentialInput.clone();
      mmapRandomInput = bench.mmapRandomInput.clone();
      niofsInput = bench.niofsInput.clone();
      ffiArena = Arena.ofConfined();
      ffiBuf = ffiArena.allocate(readSize);
      ffiDirectIOBuf = ffiArena.allocate(readSize, PAGE_SIZE);
      heapBuf = new byte[readSize];
    }

    public void cleanup() throws IOException {
      mmapInput.close();
      mmapSequentialInput.close();
      mmapRandomInput.close();
      niofsInput.close();
      ffiArena.close();
    }
  }
}
