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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.sandbox.search.ArrayTermInSetQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.BytesRef;
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
import org.openjdk.jmh.infra.Blackhole;

/**
 * Compares {@link TermInSetQuery} (PrefixCodedTerms-backed) against {@link ArrayTermInSetQuery}
 * (sorted-{@code BytesRef[]}-backed) along these dimensions:
 *
 * <ul>
 *   <li>Construction cost only ({@code construct*} benchmarks)
 *   <li>Construction + per-segment terms iteration ({@code constructAndIterate*} benchmarks)
 *   <li>{@code equals()} on cache-hit (equal) queries ({@code equals*} benchmarks)
 * </ul>
 *
 * <p>Parameters:
 *
 * <ul>
 *   <li>{@code numTerms} — query set size: 30, 300, 3000, 30000.
 *   <li>{@code numSegments} — total segment count in the index: 5, 20, 50. Unused by construct-only
 *       benchmarks.
 *   <li>{@code indexContent} — what lives in each segment's terms dictionary: {@code QUERY_ONLY},
 *       {@code SPARSE}, {@code RANDOM_50K}. Unused by construct-only benchmarks.
 *   <li>{@code inputShape} — how the query terms are passed to the ctor: {@code UNSORTED_LIST}
 *       (both queries radix-sort) or {@code SORTED_SET} (a {@link TreeSet} with natural-order
 *       comparator that hits the skip-sort fast path in both queries).
 * </ul>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 5, time = 5)
@Fork(1)
public class ArrayTermInSetQueryBenchmark {

  private static final String FIELD = "allowed_groups";

  enum IndexContent {
    /** Index exactly the query terms — every query term matches in every segment. */
    QUERY_ONLY,
    /** Index only 2 of the query terms per segment — most seeks are misses. */
    SPARSE,
    /** Index 50k random terms independent of the query — large dictionary, zero matches. */
    RANDOM_50K
  }

  enum InputShape {
    /** {@code Arrays.asList(shuffledArray)} — both queries radix-sort internally. */
    UNSORTED_LIST,
    /**
     * {@code TreeSet<BytesRef>} with natural-order comparator — both queries hit the skip-sort fast
     * path. Isolates the storage-shape cost from the sort cost.
     */
    SORTED_SET
  }

  @State(Scope.Benchmark)
  public static class BenchState {

    @Param({"30", "300", "3000", "30000"})
    public int numTerms;

    @Param({"5", "20", "50"})
    public int numSegments;

    @Param({"QUERY_ONLY", "SPARSE", "RANDOM_50K"})
    public String indexContent;

    @Param({"UNSORTED_LIST", "SORTED_SET"})
    public String inputShape;

    Collection<BytesRef> termsInput;
    IndexSearcher searcher;

    private DirectoryReader reader;
    private ByteBuffersDirectory directory;

    @Setup(Level.Trial)
    public void setup() throws IOException {
      Random rng = new Random(42);
      BytesRef[] sortedTerms = generateSortedTerms(rng, numTerms);

      // Build the input collection in the requested shape.
      InputShape shape = InputShape.valueOf(inputShape);
      switch (shape) {
        case UNSORTED_LIST:
          {
            List<BytesRef> shuffled = new ArrayList<>(Arrays.asList(sortedTerms));
            Collections.shuffle(shuffled, new Random(rng.nextLong()));
            termsInput = shuffled;
            break;
          }
        case SORTED_SET:
          {
            // TreeSet with no explicit comparator → natural-order; both query ctors will skip
            // the radix sort.
            TreeSet<BytesRef> set = new TreeSet<>();
            Collections.addAll(set, sortedTerms);
            termsInput = set;
            break;
          }
      }

      IndexContent content = IndexContent.valueOf(indexContent);
      // Two deterministic entries from the query set — first and middle term.
      BytesRef[] sparseTerms = new BytesRef[] {sortedTerms[0], sortedTerms[numTerms / 2]};
      // Extra terms pre-generated only when needed.
      BytesRef[] extraTerms =
          content == IndexContent.RANDOM_50K ? generateSortedTerms(rng, 50_000) : null;

      directory = new ByteBuffersDirectory();
      IndexWriter writer =
          new IndexWriter(
              directory, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE));

      for (int seg = 0; seg < numSegments; seg++) {
        switch (content) {
          case QUERY_ONLY:
            for (BytesRef term : sortedTerms) {
              addDoc(writer, term.utf8ToString());
            }
            break;
          case SPARSE:
            for (BytesRef term : sparseTerms) {
              addDoc(writer, term.utf8ToString());
            }
            break;
          case RANDOM_50K:
            for (BytesRef term : extraTerms) {
              addDoc(writer, term.utf8ToString());
            }
            break;
        }
        writer.commit();
      }

      reader = DirectoryReader.open(writer);
      writer.close();
      searcher = new IndexSearcher(reader);
      searcher.setQueryCache(null);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
      reader.close();
      directory.close();
    }

    private static BytesRef[] generateSortedTerms(Random rng, int count) {
      BytesRef[] terms = new BytesRef[count];
      for (int i = 0; i < count; i++) {
        terms[i] = new BytesRef(String.format(Locale.ROOT, "%016x", rng.nextLong()));
      }
      Arrays.sort(terms);
      return terms;
    }

    private static void addDoc(IndexWriter writer, String value) throws IOException {
      Document doc = new Document();
      doc.add(new StringField(FIELD, value, Field.Store.NO));
      writer.addDocument(doc);
    }
  }

  @Benchmark
  public void constructTermInSetQuery(BenchState state, Blackhole bh) {
    bh.consume(new TermInSetQuery(FIELD, state.termsInput));
  }

  @Benchmark
  public void constructArrayTermInSetQuery(BenchState state, Blackhole bh) {
    bh.consume(new ArrayTermInSetQuery(FIELD, state.termsInput));
  }

  @Benchmark
  public void constructAndIterateTermInSetQuery(BenchState state, Blackhole bh) throws IOException {
    bh.consume(state.searcher.count(new TermInSetQuery(FIELD, state.termsInput)));
  }

  @Benchmark
  public void constructAndIterateArrayTermInSetQuery(BenchState state, Blackhole bh)
      throws IOException {
    bh.consume(state.searcher.count(new ArrayTermInSetQuery(FIELD, state.termsInput)));
  }

  /**
   * State for {@code equals*} benchmarks. Builds equal query pairs so we measure {@code equals()}
   * on cache-hit (equal) queries — the hot path the packed-{@code byte[]} fast path targets.
   *
   * <p>Strategies under comparison: {@link TermInSetQuery} (Lucene baseline), {@link
   * ArrayTermInSetQuery} with VInt-prefix packing, flat-packed {@code byte[]} without boundaries
   * (incorrect — distinct term boundaries with the same concatenation collide; included only as a
   * perf reference), and packed {@code byte[]} + separate {@code int[]} lengths array (correct
   * alternative shape with two {@code memcmp}s).
   */
  @State(Scope.Benchmark)
  public static class EqualsState {

    @Param({"300", "3000", "30000"})
    public int numTerms;

    Query termInSetA;
    Query termInSetB;
    Query arrayTermInSetA;
    Query arrayTermInSetB;

    byte[] flatPackedA;
    byte[] flatPackedB;
    int flatHashA;
    int flatHashB;

    byte[] lengthsPackedA;
    byte[] lengthsPackedB;
    int[] termLengthsA;
    int[] termLengthsB;
    int lengthsHashA;
    int lengthsHashB;

    @Setup(Level.Trial)
    public void setup() {
      Random rng = new Random(42);
      BytesRef[] sorted = generateSortedTerms(rng, numTerms);

      List<BytesRef> termsList = Arrays.asList(sorted);
      termInSetA = new TermInSetQuery(FIELD, termsList);
      termInSetB = new TermInSetQuery(FIELD, termsList);

      List<BytesRef> copyA = new ArrayList<>(termsList.size());
      List<BytesRef> copyB = new ArrayList<>(termsList.size());
      for (BytesRef t : sorted) {
        copyA.add(BytesRef.deepCopyOf(t));
        copyB.add(BytesRef.deepCopyOf(t));
      }
      arrayTermInSetA = new ArrayTermInSetQuery(FIELD, copyA);
      arrayTermInSetB = new ArrayTermInSetQuery(FIELD, copyB);

      flatPackedA = flatPack(sorted);
      flatPackedB = flatPack(sorted);
      flatHashA = Arrays.hashCode(flatPackedA);
      flatHashB = Arrays.hashCode(flatPackedB);

      lengthsPackedA = flatPack(sorted);
      lengthsPackedB = flatPack(sorted);
      termLengthsA = extractLengths(sorted);
      termLengthsB = extractLengths(sorted);
      lengthsHashA = 31 * Arrays.hashCode(lengthsPackedA) + Arrays.hashCode(termLengthsA);
      lengthsHashB = 31 * Arrays.hashCode(lengthsPackedB) + Arrays.hashCode(termLengthsB);
    }

    private static BytesRef[] generateSortedTerms(Random rng, int count) {
      BytesRef[] terms = new BytesRef[count];
      for (int i = 0; i < count; i++) {
        terms[i] = new BytesRef(String.format(Locale.ROOT, "%016x", rng.nextLong()));
      }
      Arrays.sort(terms);
      return terms;
    }

    private static byte[] flatPack(BytesRef[] terms) {
      int total = 0;
      for (BytesRef t : terms) {
        total += t.length;
      }
      byte[] packed = new byte[total];
      int pos = 0;
      for (BytesRef t : terms) {
        System.arraycopy(t.bytes, t.offset, packed, pos, t.length);
        pos += t.length;
      }
      return packed;
    }

    private static int[] extractLengths(BytesRef[] terms) {
      int[] lengths = new int[terms.length];
      for (int i = 0; i < terms.length; i++) {
        lengths[i] = terms[i].length;
      }
      return lengths;
    }
  }

  @Benchmark
  public void equalsTermInSetQuery(EqualsState state, Blackhole bh) {
    bh.consume(state.termInSetA.equals(state.termInSetB));
  }

  /** VInt-prefix packed — what {@link ArrayTermInSetQuery} ships. Single {@code Arrays.equals}. */
  @Benchmark
  public void equalsArrayTermInSetQuery(EqualsState state, Blackhole bh) {
    bh.consume(state.arrayTermInSetA.equals(state.arrayTermInSetB));
  }

  /**
   * Flat packed without term boundaries — incorrect (distinct term boundaries with the same
   * concatenation collide). Included only as a perf reference for the equals fast path.
   */
  @Benchmark
  public void equalsFlatPacked(EqualsState state, Blackhole bh) {
    bh.consume(
        state.flatHashA == state.flatHashB && Arrays.equals(state.flatPackedA, state.flatPackedB));
  }

  /** Packed {@code byte[]} + separate {@code int[]} lengths — correct alternative shape. */
  @Benchmark
  public void equalsPackedPlusLengths(EqualsState state, Blackhole bh) {
    bh.consume(
        state.lengthsHashA == state.lengthsHashB
            && Arrays.equals(state.lengthsPackedA, state.lengthsPackedB)
            && Arrays.equals(state.termLengthsA, state.termLengthsB));
  }
}
