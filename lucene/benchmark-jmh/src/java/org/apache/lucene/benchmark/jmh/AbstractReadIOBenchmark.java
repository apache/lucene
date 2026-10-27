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
import java.util.OptionalLong;
import org.apache.lucene.misc.store.DirectIODirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.ReadAdvice;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.IOUtils;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.BenchmarkParams;

/**
 * Base class for {@link RandomReadIOBenchmark} and {@link SequentialReadIOBenchmark}, which measure
 * the read throughput of one large file with different I/O strategies. The first one reads at
 * random offsets, the second one scans the file sequentially.
 *
 * <p>One benchmark invocation, which is what JMH counts as one op in the reported ops/ms, issues
 * {@code readsPerOp} reads of {@code readSize} bytes each.
 *
 * <p>Strategies:
 *
 * <ul>
 *   <li>{@code mmap}: {@link MMapDirectory} with the default advice ({@link ReadAdvice#NORMAL}, so
 *       no madvise call).
 *   <li>{@code mmapRandom}: {@link MMapDirectory} with {@link ReadAdvice#RANDOM}.
 *   <li>{@code mmapSequential}: {@link MMapDirectory} with {@link ReadAdvice#SEQUENTIAL}.
 *   <li>{@code *BatchedPrefetch}: each invocation first calls {@link IndexInput#prefetch} on the
 *       offsets of all its reads, then does the reads one by one.
 *   <li>{@code fileChannelNIOFS}: {@link NIOFSDirectory}, positional {@code FileChannel} reads.
 *   <li>{@code ffiPread}: pread(2) through the FFI into a native buffer.
 *   <li>{@code directIO}: {@link DirectIODirectory}, which reads with direct I/O and bypasses the
 *       OS page cache.
 * </ul>
 *
 * <p>How to run:
 *
 * <pre>
 * dd if=/dev/urandom of=/path/to/pread-bench-16G.dat bs=1M count=16384
 * ./gradlew :lucene:benchmark-jmh:assemble
 * java -jar lucene/benchmark-jmh/build/benchmarks/lucene-benchmark-jmh-*.jar RandomReadIOBenchmark \
 *   -p readSize=16384 -p readsPerOp=16 \
 *   -jvmArgsAppend "-Dbench.file=/path/to/pread-bench-16G.dat -Dbench.fileSizeMB=16384 -Dbench.dropPageCache=false"
 * </pre>
 *
 * <p>JMH parameters ({@code -p}):
 *
 * <ul>
 *   <li>{@code readSize}: bytes per read, default 16384.
 *   <li>{@code readsPerOp}: reads per benchmark invocation, default 16 for {@link
 *       RandomReadIOBenchmark} and 128 for {@link SequentialReadIOBenchmark}.
 * </ul>
 *
 * <p>Configuration (environment variable first, then system property):
 *
 * <ul>
 *   <li>{@code BENCH_FILE} / {@code bench.file}: the file to read, default {@code
 *       /tmp/pread-bench.dat}.
 *   <li>{@code BENCH_FILE_SIZE_MB} / {@code bench.fileSizeMB}: the size of the region to read,
 *       starting at offset 0, in MiB (1 MiB = 1024 * 1024 bytes). The file must be at least this
 *       large. Default 1024.
 *   <li>{@code BENCH_DROP_CACHES} / {@code bench.dropPageCache}: drop the OS page cache before
 *       every iteration, default {@code false}.
 * </ul>
 *
 * <p>The benchmarks work like a Java version of fio: each strategy issues the same reads as a fio
 * job, through the Lucene or JDK API it stands for, so the two tools can be compared side by side.
 * One JMH op issues {@code readsPerOp} reads, so ops/ms x 1000 x {@code readsPerOp} gives the IOPS
 * that fio reports. To make both tools do the same work:
 *
 * <ul>
 *   <li>Drop the page cache before each run of either tool, so both start from a cold cache.
 *   <li>Run JMH with {@code -f 1 -wi 0 -i 1 -r 30s}, one 30 second run without warmup, to match
 *       fio's {@code --time_based --runtime=30}.
 * </ul>
 *
 * <p>Some examples:
 *
 * <ul>
 *   <li>Random 16 KiB reads with {@code mmap}, 4 threads, one read in flight per thread:
 *       <pre>
 * java -jar lucene/benchmark-jmh/build/benchmarks/lucene-benchmark-jmh-*.jar 'RandomReadIOBenchmark\.mmap_T04$' \
 *   -p readSize=16384 -p readsPerOp=1 -f 1 -wi 0 -i 1 -r 30s \
 *   -jvmArgsAppend "-Dbench.file=/path/to/pread-bench-16G.dat -Dbench.fileSizeMB=16384"
 *
 * fio --name=rand-mmap --filename=/path/to/pread-bench-16G.dat --size=16384m --readonly \
 *   --rw=randread --bs=16k --norandommap --randrepeat=0 --blockalign=1 \
 *   --ioengine=mmap --fadvise_hint=0 --numjobs=4 --thread --iodepth=1 \
 *   --time_based --runtime=30 --group_reporting
 * </pre>
 *   <li>Random 16 KiB reads with {@code fileChannelNIOFS}, 16 threads, the same as fio's psync
 *       engine with buffered I/O:
 *       <pre>
 * java -jar lucene/benchmark-jmh/build/benchmarks/lucene-benchmark-jmh-*.jar 'RandomReadIOBenchmark\.fileChannelNIOFS_T16$' \
 *   -p readSize=16384 -p readsPerOp=1 -f 1 -wi 0 -i 1 -r 30s \
 *   -jvmArgsAppend "-Dbench.file=/path/to/pread-bench-16G.dat -Dbench.fileSizeMB=16384"
 *
 * fio --name=rand-niofs --filename=/path/to/pread-bench-16G.dat --size=16384m --readonly \
 *   --rw=randread --bs=16k --norandommap --randrepeat=0 --blockalign=1 \
 *   --ioengine=psync --direct=0 --fadvise_hint=0 --numjobs=16 --thread --iodepth=1 \
 *   --time_based --runtime=30 --group_reporting
 * </pre>
 *   <li>Random 16 KiB reads with {@code mmapRandomBatchedPrefetch}, 1 thread, 16 reads in flight
 *       per thread. This saturates the device just like fio does with an I/O depth of 16:
 *       <pre>
 * java -jar lucene/benchmark-jmh/build/benchmarks/lucene-benchmark-jmh-*.jar 'RandomReadIOBenchmark\.mmapRandomBatchedPrefetch_T01$' \
 *   -p readSize=16384 -p readsPerOp=16 -f 1 -wi 0 -i 1 -r 30s \
 *   -jvmArgsAppend "-Dbench.file=/path/to/pread-bench-16G.dat -Dbench.fileSizeMB=16384"
 *
 * fio --name=rand-prefetch --filename=/path/to/pread-bench-16G.dat --size=16384m --readonly \
 *   --rw=randread --bs=16k --norandommap --randrepeat=0 --blockalign=1 \
 *   --ioengine=io_uring --direct=0 --fadvise_hint=random --numjobs=1 --thread \
 *   --iodepth=16 --iodepth_batch_submit=16 --iodepth_batch_complete_min=16 \
 *   --time_based --runtime=30 --group_reporting
 * </pre>
 * </ul>
 *
 * <p>Watch out for:
 *
 * <ul>
 *   <li>The benchmark does not create memory pressure by itself, the host does. Make the file
 *       larger than the memory available for the page cache, or run inside a memory-limited cgroup.
 *   <li>Each benchmark runs in its own JVM fork, but the OS page cache is shared across forks, and
 *       benchmarks run one after another. Without {@code bench.dropPageCache=true}, a benchmark
 *       starts with whatever the previous one left in the cache. If you run the memory pressure
 *       scenario, clearing the cache or not makes little difference: pages get evicted all the
 *       time, so the cache holds the same mix of hot, warm and cold pages either way. Warm the file
 *       before the first run (for example {@code cat /path/to/file > /dev/null}), otherwise the
 *       first buffered benchmark may spend its whole run filling the cache.
 *   <li>Put the file on a real disk. On some Linux distributions {@code /tmp} is a tmpfs, which
 *       keeps the whole file in memory and makes the results meaningless.
 * </ul>
 */
public abstract class AbstractReadIOBenchmark {

  /** POSIX open(2) flag: read-only. */
  private static final int O_RDONLY = 0;

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

  /** Only opened for direct I/O benchmarks, otherwise null. */
  protected DirectIODirectory directIODir;

  protected IndexInput directIOInput;

  // FFI pread file descriptor, -1 until opened
  protected int preadFd = -1;
  protected Arena ffiArena;

  /** Size in bytes of a single read. */
  protected abstract int readSize();

  @Setup(Level.Trial)
  public void setup(BenchmarkParams params) throws Exception {
    Path benchFile = Path.of(BENCH_FILE);
    if (Files.exists(benchFile) == false) {
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

    // Setup FFI pread file descriptor
    ffiArena = Arena.ofShared();
    setupPreadFd(benchFile);

    if (isDirectIOBenchmark(params)) {
      setupDirectIODirectory(dirPath, fileName);
    }
  }

  /**
   * Benchmark methods whose name starts with this prefix read through {@link #directIOInput}. Only
   * those open it.
   */
  protected static final String DIRECT_IO_BENCHMARK_PREFIX = "directIO";

  private static boolean isDirectIOBenchmark(BenchmarkParams params) {
    return params.getBenchmark().contains("." + DIRECT_IO_BENCHMARK_PREFIX);
  }

  private void setupMMapAndNIOFSDirectory(Path dirPath, String fileName) throws IOException {
    mmapDir = new MMapDirectory(dirPath);
    mmapInput = mmapDir.openInput(fileName, IOContext.DEFAULT);

    mmapSequentialDir = new MMapDirectory(dirPath);
    mmapSequentialDir.setReadAdvice(
        (name, _) -> {
          if (name.equals(fileName)) {
            return Optional.of(ReadAdvice.SEQUENTIAL);
          }
          return Optional.empty();
        });
    mmapSequentialInput = mmapSequentialDir.openInput(fileName, IOContext.DEFAULT);

    mmapRandomDir = new MMapDirectory(dirPath);
    mmapRandomDir.setReadAdvice(
        (name, _) -> {
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

  private void setupDirectIODirectory(Path dirPath, String fileName) throws IOException {
    directIODir =
        new DirectIODirectory(new NIOFSDirectory(dirPath), readSize(), 0L) {
          @Override
          protected boolean useDirectIO(String name, IOContext context, OptionalLong fileLength) {
            return true;
          }
        };
    directIOInput = directIODir.openInput(fileName, IOContext.DEFAULT);
  }

  @TearDown(Level.Trial)
  public void tearDown() throws IOException {
    IOUtils.close(
        mmapInput,
        mmapSequentialInput,
        mmapRandomInput,
        niofsInput,
        directIOInput,
        mmapDir,
        mmapSequentialDir,
        mmapRandomDir,
        niofsDir,
        directIODir,
        () -> closeFd(preadFd),
        () -> {
          if (ffiArena != null) {
            ffiArena.close();
          }
        });
  }

  /**
   * Reads {@code length} bytes at {@code offset} with pread(2) into {@code buf}.
   *
   * @return the number of bytes read, always {@code length}
   * @throws IOException if pread fails or returns a short read, so that a failed read never counts
   *     as a fast one
   */
  protected final long pread(MemorySegment buf, int length, long offset) throws IOException {
    long n;
    try {
      n = (long) PREAD.invokeExact(preadFd, buf, (long) length, offset);
    } catch (Throwable t) {
      throw new IOException("pread() failed at offset " + offset, t);
    }
    if (n != length) {
      throw new IOException(
          "pread() returned " + n + " at offset " + offset + ", expected " + length);
    }
    return n;
  }

  private static void closeFd(int fd) throws IOException {
    if (fd < 0) {
      return;
    }
    int rc;
    try {
      rc = (int) CLOSE.invokeExact(fd);
    } catch (Throwable t) {
      throw new IOException("close(" + fd + ") failed", t);
    }
    if (rc != 0) {
      throw new IOException("close(" + fd + ") returned " + rc);
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
      exec("/usr/bin/sudo", "-n", "purge");
    } else if (Constants.LINUX) {
      exec("/bin/sync");
      exec("/usr/bin/sudo", "-n", "/bin/sh", "-c", "echo 3 > /proc/sys/vm/drop_caches");
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
    public IndexInput directIOInput;
    public Arena ffiArena;
    public MemorySegment ffiBuf;
    public byte[] heapBuf;

    public void init(AbstractReadIOBenchmark bench, int readSize) throws IOException {
      mmapInput = bench.mmapInput.clone();
      mmapSequentialInput = bench.mmapSequentialInput.clone();
      mmapRandomInput = bench.mmapRandomInput.clone();
      niofsInput = bench.niofsInput.clone();
      if (bench.directIOInput != null) {
        directIOInput = bench.directIOInput.clone();
      }
      ffiArena = Arena.ofConfined();
      ffiBuf = ffiArena.allocate(readSize);
      heapBuf = new byte[readSize];
    }

    public void cleanup() {
      ffiArena.close();
    }
  }
}
