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

package org.apache.lucene.queries.intervals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

/**
 * Checks that combining fixed-width disjunction alternatives into a single source gives the same
 * minimal matches as fully expanding the cross product of all alternatives
 */
public class TestRandomDisjunctionEquivalence extends LuceneTestCase {

  private static final String FIELD = "f";
  private static final String[] VOCAB = {"a", "b", "c", "d", "e", "f", "g", "h", "i"};
  private static final int TERMS_PER_CLAUSE = 3;

  // Each clause draws from its own terms: if clauses can match the same term, the expanded form
  // deduplicates identical sources and so is not comparable

  public void testGroupedMatchesFullyExpanded() throws IOException {
    try (Directory dir = newDirectory()) {
      indexRandomDocs(dir);
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        int iters = atLeast(300);
        for (int i = 0; i < iters; i++) {
          boolean ordered = random().nextBoolean();
          int clauseCount = org.apache.lucene.tests.util.TestUtil.nextInt(random(), 2, 3);
          List<List<IntervalsSource>> alternatives = new ArrayList<>();
          for (int c = 0; c < clauseCount; c++) {
            alternatives.add(randomAlternatives(c));
          }
          // -1 means no gap limit
          int maxGaps = random().nextInt(7) - 1;
          IntervalsSource actual = combine(ordered, maxGaps, toDisjunctions(alternatives));
          IntervalsSource expected = expand(ordered, maxGaps, alternatives);
          for (LeafReaderContext ctx : reader.leaves()) {
            assertEquals(
                (ordered ? "ordered" : "unordered")
                    + " maxGaps="
                    + maxGaps
                    + " clauses="
                    + alternatives,
                minimalMatches(expected, ctx),
                minimalMatches(actual, ctx));
          }
        }
      }
    }
  }

  private static List<IntervalsSource> toDisjunctions(List<List<IntervalsSource>> alternatives) {
    List<IntervalsSource> disjunctions = new ArrayList<>();
    for (List<IntervalsSource> alts : alternatives) {
      disjunctions.add(Intervals.or(alts));
    }
    return disjunctions;
  }

  private static IntervalsSource combine(
      boolean ordered, int maxGaps, List<IntervalsSource> clauses) {
    IntervalsSource[] array = clauses.toArray(IntervalsSource[]::new);
    IntervalsSource combined = ordered ? Intervals.ordered(array) : Intervals.unordered(array);
    return maxGaps < 0 ? combined : Intervals.maxgaps(maxGaps, combined);
  }

  // Reference: one source per element of the cross product of the alternatives, with no grouping
  private static IntervalsSource expand(
      boolean ordered, int maxGaps, List<List<IntervalsSource>> alternatives) {
    List<List<IntervalsSource>> product = new ArrayList<>();
    product.add(new ArrayList<>());
    for (List<IntervalsSource> alts : alternatives) {
      List<List<IntervalsSource>> next = new ArrayList<>();
      for (List<IntervalsSource> prefix : product) {
        for (IntervalsSource alt : alts) {
          List<IntervalsSource> extended = new ArrayList<>(prefix);
          extended.add(alt);
          next.add(extended);
        }
      }
      product = next;
    }
    List<IntervalsSource> sources = new ArrayList<>();
    for (List<IntervalsSource> clauses : product) {
      sources.add(combine(ordered, maxGaps, clauses));
    }
    return Intervals.or(false, sources);
  }

  private static List<IntervalsSource> randomAlternatives(int clause) {
    int count = org.apache.lucene.tests.util.TestUtil.nextInt(random(), 1, 3);
    List<IntervalsSource> alts = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      alts.add(randomAlternative(clause));
    }
    return alts;
  }

  private static IntervalsSource randomAlternative(int clause) {
    int length = org.apache.lucene.tests.util.TestUtil.nextInt(random(), 1, 3);
    String[] terms = new String[length];
    for (int i = 0; i < length; i++) {
      terms[i] = VOCAB[clause * TERMS_PER_CLAUSE + random().nextInt(TERMS_PER_CLAUSE)];
    }
    IntervalsSource source = length == 1 ? Intervals.term(terms[0]) : Intervals.phrase(terms);
    switch (random().nextInt(9)) {
      case 0:
        return Intervals.maxgaps(0, source);
      case 1:
        return Intervals.containing(source, Intervals.term(terms[0]));
      case 2:
        return Intervals.extend(source, random().nextInt(2), random().nextInt(3));
      case 3:
        return Intervals.extend(source, 1, random().nextInt(2));
      case 4:
        return Intervals.prefix(new BytesRef(terms[0]));
      default:
        return source;
    }
  }

  // Intervals that do not strictly contain another interval from the same document
  private static Set<String> minimalMatches(IntervalsSource source, LeafReaderContext ctx)
      throws IOException {
    Set<String> result = new TreeSet<>();
    IntervalIterator it = source.intervals(FIELD, ctx);
    if (it == null) {
      return result;
    }
    while (it.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
      List<int[]> intervals = new ArrayList<>();
      while (it.nextInterval() != IntervalIterator.NO_MORE_INTERVALS) {
        intervals.add(new int[] {it.start(), it.end()});
      }
      for (int[] interval : intervals) {
        boolean minimal = true;
        for (int[] other : intervals) {
          boolean contains = interval[0] <= other[0] && other[1] <= interval[1];
          boolean same = interval[0] == other[0] && interval[1] == other[1];
          if (contains && same == false) {
            minimal = false;
            break;
          }
        }
        if (minimal) {
          result.add(it.docID() + ":" + interval[0] + "-" + interval[1]);
        }
      }
    }
    return result;
  }

  private static void indexRandomDocs(Directory dir) throws IOException {
    try (RandomIndexWriter writer =
        new RandomIndexWriter(
            random(),
            dir,
            newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false)))) {
      int docs = atLeast(50);
      for (int d = 0; d < docs; d++) {
        StringBuilder text = new StringBuilder();
        int length = org.apache.lucene.tests.util.TestUtil.nextInt(random(), 4, 20);
        for (int i = 0; i < length; i++) {
          text.append(VOCAB[random().nextInt(VOCAB.length)]).append(' ');
        }
        Document doc = new Document();
        doc.add(new TextField(FIELD, text.toString(), Field.Store.NO));
        writer.addDocument(doc);
      }
    }
  }
}
