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
package org.apache.lucene.search;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;

/**
 * Generates random {@link Query} trees over a {@link FieldSpec} schema for differential oracle
 * testing. Driven by a caller-supplied {@link Random}; a failing seed replays exactly.
 */
class RandomLuceneQueryGenerator {

  static final int MAX_DEPTH = 3;

  private final Random random;
  private final List<FieldSpec> fields;
  // wide enough for a conjunction to touch every field kind plus a duplicate
  private final int maxClauses;

  RandomLuceneQueryGenerator(Random random, List<FieldSpec> fields) {
    this.random = random;
    this.fields = fields;
    this.maxClauses = Math.max(5, fields.size() + 1);
  }

  Query next() {
    // bias toward DenseConjunctionBulkScorer (all-FILTER conjunctions) and BooleanScorer's
    // bucketed path (wide flat disjunctions)
    if (random.nextInt(3) == 0) {
      return randomFilterConjunction();
    }
    if (random.nextInt(8) == 0) {
      return randomWideDisjunction();
    }
    return generate(MAX_DEPTH);
  }

  private Query randomFilterConjunction() {
    int numClauses = TestUtil.nextInt(random, 2, maxClauses);
    BooleanQuery.Builder b = new BooleanQuery.Builder();
    for (int i = 0; i < numClauses; i++) {
      b.add(randomLeaf(), Occur.FILTER);
    }
    return b.build();
  }

  private Query randomWideDisjunction() {
    int numClauses = TestUtil.nextInt(random, 2, 16);
    BooleanQuery.Builder b = new BooleanQuery.Builder();
    for (int i = 0; i < numClauses; i++) {
      b.add(randomLeaf(), Occur.SHOULD);
    }
    b.setMinimumNumberShouldMatch(random.nextInt(3));
    return b.build();
  }

  private Query generate(int depth) {
    // 30% early-out below the root tapers tree depth
    if (depth == 0 || (depth < MAX_DEPTH && random.nextInt(10) < 3)) {
      return randomLeaf();
    }
    return randomBool(depth);
  }

  private Query randomBool(int depth) {
    int numClauses = TestUtil.nextInt(random, 1, maxClauses);
    BooleanQuery.Builder b = new BooleanQuery.Builder();

    int mustNotCount = 0;
    List<Query> clauses = new ArrayList<>(numClauses);
    List<Occur> occurs = new ArrayList<>(numClauses);

    for (int i = 0; i < numClauses; i++) {
      clauses.add(generate(depth - 1));
      occurs.add(RandomPicks.randomFrom(random, Occur.values()));
      if (occurs.get(i) == Occur.MUST_NOT) {
        mustNotCount++;
      }
    }

    if (mustNotCount == numClauses) {
      occurs.set(random.nextInt(numClauses), Occur.FILTER);
    }

    int numShould = 0;
    for (int i = 0; i < numClauses; i++) {
      b.add(clauses.get(i), occurs.get(i));
      if (occurs.get(i) == Occur.SHOULD) {
        numShould++;
      }
    }

    if (numShould > 0) {
      b.setMinimumNumberShouldMatch(random.nextInt(numShould + 1));
    }

    return b.build();
  }

  private Query randomLeaf() {
    int roll = random.nextInt(20);
    if (roll == 0) return new MatchAllDocsQuery();
    if (roll == 1) return new MatchNoDocsQuery();
    if (roll == 2) return new ConstantScoreQuery(randomLeafFromField());
    return randomLeafFromField();
  }

  private Query randomLeafFromField() {
    FieldSpec f = RandomPicks.randomFrom(random, fields);
    return switch (f.kind()) {
      case INDEXED_KEYWORD -> randomIndexedKeywordLeaf(f);
      case DV_KEYWORD -> randomDvKeywordLeaf(f);
      case DV_NUMERIC -> randomDvNumericLeaf(f);
      case POINT_LONG -> randomPointLongLeaf(f);
    };
  }

  private Query randomIndexedKeywordLeaf(FieldSpec f) {
    return switch (random.nextInt(3)) {
      case 0 -> new TermQuery(new Term(f.name(), randomTerm(f)));
      case 1 -> randomTermInSet(f);
      case 2 -> randomTermRange(f);
      default -> throw new AssertionError();
    };
  }

  private Query randomTermInSet(FieldSpec f) {
    int n = TestUtil.nextInt(random, 1, Math.min(3, f.terms().size()));
    List<BytesRef> set = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      set.add(randomTerm(f));
    }
    return new TermInSetQuery(f.name(), set);
  }

  private Query randomTermRange(FieldSpec f) {
    BytesRef lo = randomTermOrNull(f);
    BytesRef hi = randomTermOrNull(f);
    if (lo != null && hi != null && lo.compareTo(hi) > 0) {
      BytesRef tmp = lo;
      lo = hi;
      hi = tmp;
    }
    return new TermRangeQuery(f.name(), lo, hi, random.nextBoolean(), random.nextBoolean());
  }

  private Query randomDvKeywordLeaf(FieldSpec f) {
    return switch (random.nextInt(3)) {
      case 0 -> SortedSetDocValuesField.newSlowExactQuery(f.name(), randomTerm(f));
      case 1 -> randomDvKeywordRangeLeaf(f);
      case 2 -> randomDvKeywordSetLeaf(f);
      default -> throw new AssertionError();
    };
  }

  private Query randomDvKeywordRangeLeaf(FieldSpec f) {
    BytesRef a = randomTerm(f);
    BytesRef b = randomTerm(f);
    boolean ordered = a.compareTo(b) <= 0;
    return SortedSetDocValuesField.newSlowRangeQuery(
        f.name(), ordered ? a : b, ordered ? b : a, true, true);
  }

  /**
   * Picks every other term so there is always an ordinal gap, forcing the non-contiguous ordinal
   * set iterator whose {@code docIDRunEnd()} had the GH#16450 bug.
   */
  private Query randomDvKeywordSetLeaf(FieldSpec f) {
    List<BytesRef> all = f.terms();
    if (all.size() < 3) {
      return SortedSetDocValuesField.newSlowExactQuery(f.name(), randomTerm(f));
    }
    int start = random.nextInt(2);
    List<BytesRef> subset = new ArrayList<>();
    for (int i = start; i < all.size(); i += 2) {
      subset.add(all.get(i));
    }
    if (subset.size() < 2) {
      subset.add(all.get(all.size() - 1));
    }
    return SortedSetDocValuesField.newSlowSetQuery(f.name(), subset);
  }

  private Query randomDvNumericLeaf(FieldSpec f) {
    if (random.nextBoolean()) {
      return SortedNumericDocValuesField.newSlowExactQuery(f.name(), randomLong(f));
    }
    long a = randomLong(f);
    long b = randomLong(f);
    return SortedNumericDocValuesField.newSlowRangeQuery(f.name(), Math.min(a, b), Math.max(a, b));
  }

  private Query randomPointLongLeaf(FieldSpec f) {
    long a = randomLong(f);
    long b = randomLong(f);
    long lo = Math.min(a, b);
    long hi = Math.max(a, b);
    Query pointQuery = LongPoint.newRangeQuery(f.name() + "_point", lo, hi);
    Query dvQuery = SortedNumericDocValuesField.newSlowRangeQuery(f.name() + "_dv", lo, hi);
    if (random.nextBoolean()) {
      return new IndexOrDocValuesQuery(pointQuery, dvQuery);
    }
    return random.nextBoolean() ? pointQuery : dvQuery;
  }

  private BytesRef randomTerm(FieldSpec f) {
    if (random.nextInt(5) == 0 || f.terms().isEmpty()) {
      return new BytesRef("zzz_outside_" + random.nextInt(100));
    }
    return RandomPicks.randomFrom(random, f.terms());
  }

  private BytesRef randomTermOrNull(FieldSpec f) {
    return random.nextInt(5) == 0 ? null : randomTerm(f);
  }

  private long randomLong(FieldSpec f) {
    if (random.nextInt(5) == 0) {
      return f.maxValue() + TestUtil.nextInt(random, 1, 10);
    }
    return TestUtil.nextLong(random, f.minValue(), f.maxValue());
  }
}
