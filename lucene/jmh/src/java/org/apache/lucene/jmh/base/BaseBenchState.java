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
package org.apache.lucene.jmh.base;

import com.sun.management.HotSpotDiagnosticMXBean;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.SplittableRandom;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.management.MBeanServer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.jmh.base.meter.Meter;
import org.apache.lucene.jmh.base.rndgen.Docs;
import org.apache.lucene.jmh.base.rndgen.RndGen;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.lucene.util.SuppressForbidden;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.BenchmarkParams;

/** The type Base bench state. */
@State(Scope.Benchmark)
public class BaseBenchState {

  /** The constant DEBUG. */
  public static final boolean DEBUG = Boolean.getBoolean("debug");

  private static final long RANDOM_SEED = 6624420638116043983L;

  /** The base random. */
  public SplittableRandom random;

  private IndexWriter writer;
  private ByteBuffersDirectory directory;

  /** Instantiates a new Base bench state. */
  public BaseBenchState() {
    /* TODO document why this constructor is empty */
  }

  /**
   * Gets random seed.
   *
   * @return the random seed
   */
  public Long getRandomSeed() {
    return random.split().nextLong();
  }

  private static final AtomicBoolean HEAP_DUMPED = new AtomicBoolean();

  /** The constant QUIET_LOG. */
  public static final boolean QUIET_LOG = Boolean.getBoolean("quietLog");

  /**
   * Log.
   *
   * @param value the value
   */
  public static void log(String value) {
    log(value, false);
  }

  /**
   * Log.
   *
   * @param value the value
   * @param newLine the new line
   */
  public static void log(String value, boolean newLine) {
    if (!QUIET_LOG) {
      if (newLine) {
        System.err.println("");
      }
      System.err.println(
          new StringBuilder()
              .append(Instant.now())
              .append(" ")
              .append(value.isEmpty() ? "" : "--> ")
              .append(value)
              .toString());
    }
  }

  /** The Work dir. */
  public String workDir;

  /**
   * Do setup.
   *
   * @param benchmarkParams the benchmark params
   */
  @Setup(Level.Trial)
  public void doSetup(BenchmarkParams benchmarkParams) {
    random = new SplittableRandom(getInitRandomeSeed());
    workDir = System.getProperty("workBaseDir", "build/work");
  }

  /**
   * Do tear down.
   *
   * @param benchmarkParams the benchmark params
   * @throws Exception the exception
   */
  @TearDown(Level.Trial)
  public void doTearDown(BenchmarkParams benchmarkParams) throws Exception {
    dumpHeap(benchmarkParams);

    for (String s : RndGen.countsReport()) {
      System.out.println(s);
    }

    if (writer != null) {
      writer.close();
    }
    if (writer != null) {
      directory.close();
    }
  }

  /**
   * Index index writer.
   *
   * @param directory the directory
   * @param docs the docs
   * @param docCount the doc count
   * @return the index writer
   * @throws Exception the exception
   */
  public IndexWriter index(Directory directory, Docs docs, int docCount) throws Exception {
    return index(directory, docs, docCount, Integer.MAX_VALUE);
  }

  /**
   * Index index writer.
   *
   * @param directory the directory
   * @param docs the docs
   * @param docCount the doc count
   * @param segmentCount the segment count
   * @return the index writer
   * @throws Exception the exception
   */
  public IndexWriter index(Directory directory, Docs docs, int docCount, int segmentCount)
      throws Exception {

    log("indexing data for benchmark...");
    Meter meter = new Meter();

    IndexWriterConfig iwc = new IndexWriterConfig(new WhitespaceAnalyzer());
    iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    iwc.setMaxBufferedDocs(100);
    iwc.setRAMBufferSizeMB(Integer.MAX_VALUE);
    TieredMergePolicy mergePolicy = new TieredMergePolicy();
    mergePolicy.setSegmentsPerTier(30);
    iwc.setMergePolicy(mergePolicy);
    writer = new IndexWriter(directory, iwc);

    ExecutorService executorService =
        Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors(), new NamedThreadFactory("SolrJMH Indexer"));
    ScheduledExecutorService scheduledExecutor =
        Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("JMH Indexer Progress"));
    scheduledExecutor.scheduleAtFixedRate(
        () -> {
          if (meter.getCount() == docCount) {
            scheduledExecutor.shutdown();
          } else {
            log(meter.getCount() + " docs at " + meter.getMeanRate() + " doc/s");
          }
        },
        10,
        10,
        TimeUnit.SECONDS);
    for (int i = 0; i < docCount; i++) {
      executorService.submit(
          () -> {
            Document doc = docs.document();
            // log("add doc " + doc);

            meter.mark();

            try {
              writer.addDocument(doc);
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          });
    }

    log("done adding docs, waiting for executor to terminate...");

    executorService.shutdown();
    boolean result = false;
    while (!result) {
      result = executorService.awaitTermination(600, TimeUnit.MINUTES);
    }

    scheduledExecutor.shutdown();

    log("done indexing data for benchmark");

    log("committing data ...");
    writer.commit();
    log("done committing data");

    log("merge to " + segmentCount + " segments");
    writer.forceMerge(segmentCount);
    DirectoryReader reader = DirectoryReader.open(writer);
    log("done merging to " + reader.getIndexCommit().getSegmentCount() + " segments");
    reader.close();
    return writer;
  }

  /**
   * Dump heap.
   *
   * @param benchmarkParams the benchmark params
   * @throws IOException the io exception
   */
  @SuppressForbidden(reason = "access to force heapdump")
  public static void dumpHeap(BenchmarkParams benchmarkParams) throws IOException {
    String heapDump = System.getProperty("dumpheap");
    if (heapDump != null) {

      boolean dumpHeap = HEAP_DUMPED.compareAndExchange(false, true);
      if (dumpHeap) {
        Path file = Paths.get(heapDump);
        deleteDirectory(file);
        Files.createDirectories(file);
        Path dumpFile = file.resolve(benchmarkParams.id() + ".hprof");

        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        HotSpotDiagnosticMXBean mxBean =
            ManagementFactory.newPlatformMXBeanProxy(
                server, "com.sun.management:type=HotSpotDiagnostic", HotSpotDiagnosticMXBean.class);
        mxBean.dumpHeap(dumpFile.toString(), true);
      }
    }
  }

  private static boolean loggedSeed = false;

  /**
   * Gets init randome seed.
   *
   * @return the init randome seed
   */
  public static Long getInitRandomeSeed() {
    Long seed = Long.getLong("lucene.bench.seed");

    if (seed == null) {
      seed = RANDOM_SEED;
    }

    if (!loggedSeed) {
      log("benchmark random seed: " + seed, true);
    } else {
      loggedSeed = true;
    }

    return seed;
  }

  /**
   * Delete directory.
   *
   * @param path the path
   */
  public static void deleteDirectory(Path path) {
    List<Path> files = new ArrayList<>(32);
    try {

      Files.walkFileTree(
          path,
          new FileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
                throws IOException {
              return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException impossible)
                throws IOException {
              files.add(dir);
              return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                throws IOException {
              files.add(file);
              return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
              files.add(file);
              return FileVisitResult.CONTINUE;
            }
          });
    } catch (IOException impossible) {
      throw new AssertionError("visitor threw exception", impossible);
    }
    files.sort(Comparator.reverseOrder());

    for (Path file : files) {
      try {
        Files.deleteIfExists(file);
      } catch (NoSuchFileException e) {
        log(e.getMessage());
      } catch (IOException e) {
        log(String.format(Locale.ROOT, "WARN: could not delete file: %s %s", path, e.getMessage()));
      }
    }
  }

  /**
   * Directory byte buffers directory.
   *
   * @param dir the dir
   * @return the byte buffers directory
   */
  public ByteBuffersDirectory directory(String dir) {
    if (dir.equals("ram")) {
      this.directory = new ByteBuffersDirectory();
      return directory;
    }

    throw new IllegalArgumentException("Unknown directory impl requests, impl=" + dir);
  }
}
