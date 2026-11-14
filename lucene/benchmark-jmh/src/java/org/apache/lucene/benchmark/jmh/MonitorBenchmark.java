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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.monitor.MatcherFactory;
import org.apache.lucene.monitor.MatchingQueries;
import org.apache.lucene.monitor.Monitor;
import org.apache.lucene.monitor.MonitorConfiguration;
import org.apache.lucene.monitor.MonitorQuery;
import org.apache.lucene.monitor.ParallelMatcher;
import org.apache.lucene.monitor.PartitionMatcher;
import org.apache.lucene.monitor.QueryMatch;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.NamedThreadFactory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Measures the latency of matching a single document against a {@link Monitor}.
 *
 * <p>Registered queries are conjunctions of two random terms. Each document holds {@link #docTerms}
 * random terms from the same vocabulary, so a sizeable fraction of the registered queries pass the
 * presearcher and must be verified against the document. The query index is built with exactly
 * {@link #numSegments} segments.
 *
 * <p>Modes:
 *
 * <ul>
 *   <li>{@code serial}: the default single-threaded matcher.
 *   <li>{@code parallelMatcher}: {@link ParallelMatcher}, verifying candidates on worker threads.
 *   <li>{@code partitionMatcher}: {@link PartitionMatcher}, verifying batches of candidates on
 *       worker threads.
 *   <li>{@code executor}: the default matcher, with the query index searched concurrently through
 *       {@link MonitorConfiguration#setExecutor}.
 * </ul>
 *
 * <h2>Usage</h2>
 *
 * <p>Run all modes:
 *
 * <pre>
 * java -jar lucene-benchmark-jmh.jar "MonitorBenchmark"
 * </pre>
 *
 * <p>Compare the serial and concurrent query index search on a single segment:
 *
 * <pre>
 * java -jar lucene-benchmark-jmh.jar "MonitorBenchmark" -p mode=serial,executor -p numSegments=1
 * </pre>
 *
 * <p>Command-line {@code -jvmArgsAppend} replaces the heap settings of {@link Fork}, so repeat them
 * there.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@Fork(
    value = 1,
    jvmArgsAppend = {"-Xmx8g", "-Xms8g", "-XX:+AlwaysPreTouch"})
public class MonitorBenchmark {

  private static final String FIELD = "body";
  private static final int NUM_DOCS = 64;

  @Param({"100000"})
  int numQueries;

  @Param({"1", "4", "16"})
  int numSegments;

  @Param({"2000"})
  int vocabularySize;

  @Param({"300"})
  int docTerms;

  @Param({"4"})
  int threads;

  @Param({"serial", "parallelMatcher", "partitionMatcher", "executor"})
  String mode;

  private ExecutorService executor;
  private Monitor monitor;
  private MatcherFactory<QueryMatch> matcherFactory;
  private Document[] docs;
  private int nextDoc;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    Random random = new Random(42);
    String[] vocabulary = new String[vocabularySize];
    for (int i = 0; i < vocabularySize; i++) {
      vocabulary[i] = randomWord(random);
    }

    executor = Executors.newFixedThreadPool(threads, new NamedThreadFactory("monitor-benchmark"));
    matcherFactory =
        switch (mode) {
          case "serial", "executor" -> QueryMatch.SIMPLE_MATCHER;
          case "parallelMatcher" ->
              ParallelMatcher.factory(executor, QueryMatch.SIMPLE_MATCHER, threads);
          case "partitionMatcher" ->
              PartitionMatcher.factory(executor, QueryMatch.SIMPLE_MATCHER, threads);
          default -> throw new IllegalArgumentException("Unknown mode: " + mode);
        };

    MonitorConfiguration config =
        new MonitorConfiguration() {
          @Override
          protected IndexWriterConfig getIndexWriterConfig() {
            // one segment per commit, see setQueryUpdateBufferSize below
            return super.getIndexWriterConfig()
                .setMergePolicy(NoMergePolicy.INSTANCE)
                .setRAMBufferSizeMB(1024);
          }
        };
    config.setQueryUpdateBufferSize(numQueries / numSegments);
    if (mode.equals("executor")) {
      config.setExecutor(executor);
    }
    monitor = new Monitor(new StandardAnalyzer(), config);

    List<MonitorQuery> queries = new ArrayList<>(numQueries);
    for (int i = 0; i < numQueries; i++) {
      BooleanQuery query =
          new BooleanQuery.Builder()
              .add(termQuery(vocabulary, random), BooleanClause.Occur.MUST)
              .add(termQuery(vocabulary, random), BooleanClause.Occur.MUST)
              .build();
      queries.add(new MonitorQuery(Integer.toString(i), query));
    }
    monitor.register(queries);

    docs = new Document[NUM_DOCS];
    for (int i = 0; i < NUM_DOCS; i++) {
      StringBuilder text = new StringBuilder();
      for (int j = 0; j < docTerms; j++) {
        text.append(vocabulary[random.nextInt(vocabularySize)]).append(' ');
      }
      Document doc = new Document();
      doc.add(new TextField(FIELD, text.toString(), Field.Store.NO));
      docs[i] = doc;
    }
  }

  @TearDown(Level.Trial)
  public void tearDown() throws IOException {
    monitor.close();
    executor.shutdown();
  }

  @Benchmark
  public MatchingQueries<QueryMatch> match() throws IOException {
    Document doc = docs[nextDoc];
    nextDoc = (nextDoc + 1) % NUM_DOCS;
    return monitor.match(doc, matcherFactory);
  }

  private static TermQuery termQuery(String[] vocabulary, Random random) {
    return new TermQuery(new Term(FIELD, vocabulary[random.nextInt(vocabulary.length)]));
  }

  private static String randomWord(Random random) {
    int length = 4 + random.nextInt(7);
    char[] chars = new char[length];
    for (int i = 0; i < length; i++) {
      chars[i] = (char) ('a' + random.nextInt(26));
    }
    return new String(chars);
  }
}
