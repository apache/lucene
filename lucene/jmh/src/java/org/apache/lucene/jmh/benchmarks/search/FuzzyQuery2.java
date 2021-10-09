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
package org.apache.lucene.jmh.benchmarks.search;

import static org.apache.lucene.jmh.base.BaseBenchState.log;
import static org.apache.lucene.jmh.base.rndgen.Docs.docs;
import static org.apache.lucene.jmh.base.rndgen.SourceDSL.strings;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.jmh.base.BaseBenchState;
import org.apache.lucene.jmh.base.rndgen.BenchmarkRandomSource;
import org.apache.lucene.jmh.base.rndgen.Distribution;
import org.apache.lucene.jmh.base.rndgen.Docs;
import org.apache.lucene.jmh.base.rndgen.Queries;
import org.apache.lucene.jmh.base.rndgen.RndCollector;
import org.apache.lucene.jmh.base.rndgen.SplittableRandomGenerator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.BenchmarkParams;

/** The type. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Threads(1)
@Warmup(time = 15, iterations = 10)
@Measurement(time = 15, iterations = 10)
@Fork(value = 1)
@Timeout(time = 600)
public class FuzzyQuery2 {

  private static final boolean DEBUG = true;
  /** The constant SHORT_FIELD. */
  public static final String SHORT_FIELD = "short";
  /** The constant LONG_PREFIX_SHORT_POST. */
  public static final String LONG_PREFIX_SHORT_POST = "longPrefixShortPost";

  private static LongAdder hits = new LongAdder();
  private static LongAdder queries = new LongAdder();

  static {
    log("query index via FuzzyQuery with varying edit distance and constant prefix size");
  }

  /** Instantiates a new FuzzyQuery benchmark. */
  public FuzzyQuery2() {
    // happy linter
  }

  /** The type Bench state. */
  @State(Scope.Benchmark)
  public static class BenchState {

    private IndexReader indexReader;
    private IndexSearcher indexSearcher;

    /** The Num docs. */
    @Param("1")
    int numDocs;

    /** The Prefix. */
    @Param({"0", "15", "45"})
    int prefix;

    /** The Edit distance. */
    @Param({"1", "2"})
    int editDistance;

    private Iterator<Query> queryIterator;

    /** Instantiates a new Bench state. */
    public BenchState() {
      // happy linter
    }

    /**
     * Benchmark setup.
     *
     * @param baseBenchState the base bench state
     * @throws Exception the exception
     */
    @Setup(Level.Trial)
    public void setup(BaseBenchState baseBenchState) throws Exception {
      BenchmarkRandomSource random =
          new BenchmarkRandomSource(new SplittableRandomGenerator(baseBenchState.getRandomSeed()));

      RndCollector<String> collector = new RndCollector<>(random, 15);

      Docs docs =
          docs()
              .field(
                  SHORT_FIELD,
                  strings()
                      .wordList()
                      .withDistribution(Distribution.ZIPFIAN)
                      .withCollector(collector)
                      .multi(5));

      ByteBuffersDirectory directory = baseBenchState.directory("ram");

      baseBenchState.index(directory, docs, numDocs, 10);

      indexReader = DirectoryReader.open(directory);

      indexSearcher = new IndexSearcher(indexReader);

      List<String> collected = collector.getValues();

      log("collected=" + collected);

      Queries queryGen =
          Queries.queries(
              () ->
                  new org.apache.lucene.search.FuzzyQuery(
                      new Term(SHORT_FIELD, collector.getRandomValue()), editDistance));

      queryIterator = queryGen.preGenerate(30);
    }

    /**
     * Teardown.
     *
     * @param benchmarkParams the benchmark params
     * @throws Exception the exception
     */
    @TearDown(Level.Trial)
    public void teardown(BenchmarkParams benchmarkParams) throws Exception {
      if (DEBUG) {
        log("hits per query: " + hits.sum() / queries.sum());
      }

      indexReader.close();
    }
  }

  /**
   * search
   *
   * @param state the state
   * @return the object
   * @throws Exception the exception
   */
  @Benchmark
  public Object fuzzySearch(BenchState state) throws Exception {
    TopDocs res = state.indexSearcher.search(state.queryIterator.next(), 10);
    if (DEBUG) {
      hits.add(res.totalHits.value);
      queries.increment();
    }
    return res;
  }
}
