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

package org.apache.lucene.monitor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;

/** NotSlots must never drop a matching query, and must exclude some that would be selected */
public class TestNotSlotRandomized extends LuceneTestCase {

  private static final Analyzer ANALYZER = new WhitespaceAnalyzer();
  private static final int MAX_BATCH_SIZE = 16;

  /** The fields, vocabulary size and document shapes of one iteration */
  private record TestConfig(
      List<String> stringFields,
      List<String> textFields,
      int vocabularySize,
      int maxValues,
      double fieldProbability) {

    List<String> allFields() {
      List<String> all = new ArrayList<>(stringFields);
      all.addAll(textFields);
      return all;
    }

    String field(Random r) {
      List<String> all = allFields();
      return all.get(r.nextInt(all.size()));
    }

    /** Skewed towards low indexes, so that some values are common */
    String value(Random r) {
      double u = r.nextDouble();
      return "v" + (int) (vocabularySize * u * u * u);
    }

    @Override
    public String toString() {
      return "fields="
          + stringFields.size()
          + "+"
          + textFields.size()
          + " vocabularySize="
          + vocabularySize
          + " maxValues="
          + maxValues
          + " fieldProbability="
          + String.format(java.util.Locale.ROOT, "%.2f", fieldProbability);
    }
  }

  private static TestConfig randomTestConfig(Random r) {
    // Few fields: documents and queries share fields, so negated terms often exclude. Many fields:
    // most query terms are on fields a document lacks, and the term filter rejects most terms.
    List<String> stringFields = new ArrayList<>();
    List<String> textFields = new ArrayList<>();
    int fieldCount = TestUtil.nextInt(r, 3, 40);
    for (int i = 0; i < fieldCount; i++) {
      (r.nextInt(10) < 3 ? textFields : stringFields).add("f" + i);
    }
    int vocabularySize = TestUtil.nextInt(r, 3, r.nextBoolean() ? 10 : 200);
    return new TestConfig(
        stringFields,
        textFields,
        vocabularySize,
        TestUtil.nextInt(r, 1, 10),
        0.2 + 0.8 * r.nextDouble());
  }

  private static Query randomLeaf(Random r, TestConfig config) {
    String field = config.field(r);
    return switch (r.nextInt(12)) {
      case 0, 1, 2, 3 -> new TermQuery(new Term(field, config.value(r)));
      case 4 -> new PhraseQuery(field, config.value(r), config.value(r));
      case 5 -> new PrefixQuery(new Term(field, "v" + r.nextInt(10)));
      case 6 -> TermRangeQuery.newStringRange(field, "v1", "v5", r.nextBoolean(), r.nextBoolean());
      case 7 -> MatchAllDocsQuery.INSTANCE;
      case 8 ->
          new SynonymQuery.Builder(field)
              .addTerm(new Term(field, config.value(r)))
              .addTerm(new Term(field, config.value(r)))
              .build();
      case 9 ->
          new RegexpQuery(
              new Term(field, r.nextBoolean() ? config.value(r) + ".*" : "." + config.value(r)));
      case 10 ->
          new TermInSetQuery(
              field,
              r.nextBoolean()
                  ? List.of(new BytesRef(config.value(r)))
                  : List.of(new BytesRef(config.value(r)), new BytesRef(config.value(r))));
      default -> {
        List<BytesRef> terms = new ArrayList<>();
        for (int i = TestUtil.nextInt(r, 2, 3); i > 0; i--) {
          terms.add(new BytesRef(config.value(r)));
        }
        yield new TermInSetQuery(field, terms);
      }
    };
  }

  private static Query randomQuery(Random r, TestConfig config, int depth) {
    if (depth == 0 || r.nextInt(4) == 0) {
      return randomLeaf(r, config);
    }
    switch (r.nextInt(8)) {
      case 0:
        return new BoostQuery(randomQuery(r, config, depth - 1), 2);
      case 1:
        return new ConstantScoreQuery(randomQuery(r, config, depth - 1));
      default:
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        int shoulds = 0;
        for (int i = TestUtil.nextInt(r, 1, 5); i > 0; i--) {
          BooleanClause.Occur occur =
              BooleanClause.Occur.values()[r.nextInt(BooleanClause.Occur.values().length)];
          if (occur == BooleanClause.Occur.SHOULD) {
            shoulds++;
          }
          builder.add(randomQuery(r, config, depth - 1), occur);
        }
        if (shoulds > 1 && r.nextInt(5) == 0) {
          builder.setMinimumNumberShouldMatch(TestUtil.nextInt(r, 1, shoulds));
        }
        return builder.build();
    }
  }

  private static Document randomDocument(Random r, TestConfig config) {
    Document doc = new Document();
    for (String field : config.allFields()) {
      if (r.nextDouble() < config.fieldProbability()) {
        for (int i = TestUtil.nextInt(r, 1, config.maxValues()); i > 0; i--) {
          doc.add(randomField(r, config, field));
        }
      }
    }
    return doc;
  }

  private static Field randomField(Random r, TestConfig config, String field) {
    if (config.textFields().contains(field)) {
      StringBuilder text = new StringBuilder(config.value(r));
      for (int i = r.nextInt(3); i > 0; i--) {
        text.append(' ').append(config.value(r));
      }
      return new TextField(field, text.toString(), Field.Store.NO);
    }
    return new StringField(field, config.value(r), Field.Store.NO);
  }

  private static Document[] randomBatch(Random r, TestConfig config) {
    Document[] docs = new Document[TestUtil.nextInt(r, 1, MAX_BATCH_SIZE)];
    for (int i = 0; i < docs.length; i++) {
      docs[i] = randomDocument(r, config);
    }
    return docs;
  }

  private static TermWeightor randomWeightor(Random r) {
    long seed = r.nextLong();
    TermWeightor weights = t -> 0.05 + 5 * new Random(seed ^ t.hashCode()).nextDouble();
    return switch (r.nextInt(3)) {
      case 0 -> TermWeightor.DEFAULT;
      case 1 -> weights;
      default ->
          weights.withNegation(w -> 0.01 + 3 * new Random(~seed ^ Double.hashCode(w)).nextDouble());
    };
  }

  private static Presearcher presearcher(
      int passes, TermWeightor weightor, List<CustomQueryHandler> handlers, int maxNotSlots) {
    return passes == 0
        ? new TermFilteredPresearcher(weightor, handlers, Set.of(), maxNotSlots)
        : new MultipassTermFilteredPresearcher(
            passes, 0, weightor, handlers, Set.of(), maxNotSlots);
  }

  private static Set<String> matchIds(MultiMatchingQueries<QueryMatch> matches, int doc) {
    Set<String> ids = new HashSet<>();
    for (QueryMatch match : matches.getMatches(doc)) {
      ids.add(match.getQueryId());
    }
    return ids;
  }

  /** Records the ids of the queries a presearcher selects, without matching them */
  private static Set<String> selectedIds(Monitor monitor, Document[] docs) throws IOException {
    Set<String> ids = new HashSet<>();
    monitor.match(
        docs,
        searcher ->
            new CandidateMatcher<>(searcher) {
              @Override
              public void matchQuery(String queryId, Query matchQuery, Map<String, String> md) {
                ids.add(queryId);
              }

              @Override
              public QueryMatch resolve(QueryMatch match1, QueryMatch match2) {
                return match1;
              }
            });
    return ids;
  }

  @Nightly
  public void testNotSlotsNeverDropMatches() throws IOException {
    Random r = random();
    long excluded = 0;
    for (int iteration = atLeast(5); iteration > 0; iteration--) {
      TestConfig config = randomTestConfig(r);
      int maxNotSlots = TestUtil.nextInt(r, 1, 2);
      TermWeightor weightor = randomWeightor(r);
      List<CustomQueryHandler> handlers = new ArrayList<>();
      if (r.nextBoolean()) {
        handlers.add(new RegexpQueryHandler());
      }
      handlers.add(new TermInSetQueryHandler(r.nextBoolean()));
      int passes = r.nextInt(7);
      String description =
          "passes="
              + passes
              + " maxNotSlots="
              + maxNotSlots
              + " handlers="
              + handlers.size()
              + " "
              + config;

      try (Monitor oracle = new Monitor(ANALYZER, Presearcher.NO_FILTERING);
          Monitor withoutNotSlots =
              new Monitor(ANALYZER, presearcher(passes, weightor, handlers, 0));
          Monitor monitor =
              new Monitor(ANALYZER, presearcher(passes, weightor, handlers, maxNotSlots))) {
        List<MonitorQuery> queries = new ArrayList<>();
        for (int i = 0; i < 150; i++) {
          queries.add(new MonitorQuery(Integer.toString(i), randomQuery(r, config, 3)));
        }
        oracle.register(queries);
        withoutNotSlots.register(queries);
        monitor.register(queries);

        for (int batch = 0; batch < 20; batch++) {
          Document[] docs = randomBatch(r, config);
          MultiMatchingQueries<QueryMatch> expected = oracle.match(docs, QueryMatch.SIMPLE_MATCHER);
          MultiMatchingQueries<QueryMatch> actual = monitor.match(docs, QueryMatch.SIMPLE_MATCHER);
          for (int i = 0; i < docs.length; i++) {
            Set<String> missing = matchIds(expected, i);
            missing.removeAll(matchIds(actual, i));
            if (!missing.isEmpty()) {
              StringBuilder message =
                  new StringBuilder(description + ": dropped matches for " + docs[i] + "\n");
              for (String id : missing) {
                message
                    .append(id)
                    .append(": ")
                    .append(queries.get(Integer.parseInt(id)).getQuery())
                    .append('\n');
              }
              fail(message.toString());
            }
          }
          Set<String> exclusions = selectedIds(withoutNotSlots, docs);
          exclusions.removeAll(selectedIds(monitor, docs));
          excluded += exclusions.size();
        }
      }
    }
    assertTrue("NotSlots never excluded a query", excluded > 0);
  }
}
