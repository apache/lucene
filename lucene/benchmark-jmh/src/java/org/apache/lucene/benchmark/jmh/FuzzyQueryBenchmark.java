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
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FuzzyAutomatonCache;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.automaton.CompiledAutomaton;
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
 * Splits the cost of rewriting a {@link FuzzyQuery} into building its Levenshtein automata and
 * intersecting them with the terms dictionary, and measures what a {@link FuzzyAutomatonCache}
 * saves.
 *
 * <ul>
 *   <li>{@link #buildAutomaton} only builds the automaton for the maximum edit distance. A rewrite
 *       builds one automaton per edit distance from 0 to the maximum, so it costs more than this.
 *   <li>{@link #rewrite} is a full rewrite that builds its automata every time.
 *   <li>{@link #rewriteWithCache} is a full rewrite with every lookup answered from the cache, so
 *       it is the best case: what remains is the intersection with the terms dictionary.
 * </ul>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(
    value = 1,
    jvmArgsAppend = {"-Xmx1g", "-Xms1g"})
public class FuzzyQueryBenchmark {

  private static final String FIELD = "field";
  private static final int QUERY_COUNT = 64; // power of two

  /** Number of unique terms in the index. */
  @Param({"100000", "1000000"})
  public int numTerms;

  /** Length of the query terms; indexed terms are between 3 and 14 characters long. */
  @Param({"4", "8", "12"})
  public int termLength;

  @Param({"1", "2"})
  public int maxEdits;

  private Directory dir;
  private DirectoryReader reader;
  private IndexSearcher searcher;
  private FuzzyAutomatonCache cache;
  private String[] queryTerms;
  private int next;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    Random random = new Random(42);
    dir = new ByteBuffersDirectory();
    try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
      Set<String> terms = new HashSet<>();
      while (terms.size() < numTerms) {
        String term = randomWord(random, 3 + random.nextInt(12));
        if (terms.add(term)) {
          Document doc = new Document();
          doc.add(new StringField(FIELD, term, Field.Store.NO));
          w.addDocument(doc);
        }
      }
      w.forceMerge(1);
    }
    reader = DirectoryReader.open(dir);
    searcher = new IndexSearcher(reader);

    queryTerms = new String[QUERY_COUNT];
    for (int i = 0; i < QUERY_COUNT; i++) {
      queryTerms[i] = randomWord(random, termLength);
    }
    cache = new FuzzyAutomatonCache(QUERY_COUNT * 2, 256L * 1024 * 1024);
    for (String term : queryTerms) {
      searcher.rewrite(newQuery(term, cache)); // warm the cache
    }
  }

  @TearDown(Level.Trial)
  public void tearDown() throws IOException {
    reader.close();
    dir.close();
  }

  // A small alphabet keeps the terms dictionary dense, so that fuzzy queries have many candidates.
  private static String randomWord(Random random, int length) {
    char[] chars = new char[length];
    for (int i = 0; i < length; i++) {
      chars[i] = (char) ('a' + random.nextInt(12));
    }
    return new String(chars);
  }

  private FuzzyQuery newQuery(String term, FuzzyAutomatonCache automatonCache) {
    return new FuzzyQuery(
        new Term(FIELD, term),
        maxEdits,
        0,
        FuzzyQuery.defaultMaxExpansions,
        FuzzyQuery.defaultTranspositions,
        FuzzyQuery.defaultRewriteMethod(FuzzyQuery.defaultMaxExpansions),
        automatonCache);
  }

  private String nextTerm() {
    return queryTerms[next++ & (QUERY_COUNT - 1)];
  }

  @Benchmark
  public CompiledAutomaton buildAutomaton() {
    return FuzzyQuery.getFuzzyAutomaton(nextTerm(), maxEdits, 0, FuzzyQuery.defaultTranspositions);
  }

  @Benchmark
  public Query rewrite() throws IOException {
    return searcher.rewrite(newQuery(nextTerm(), null));
  }

  @Benchmark
  public Query rewriteWithCache() throws IOException {
    return searcher.rewrite(newQuery(nextTerm(), cache));
  }
}
