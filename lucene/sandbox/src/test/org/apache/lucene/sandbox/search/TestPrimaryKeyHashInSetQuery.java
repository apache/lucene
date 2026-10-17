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
package org.apache.lucene.sandbox.search;

import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashTestUtil.randomKey;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SoftDeletesRetentionMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.sandbox.codecs.pkhash.HashTerms;
import org.apache.lucene.sandbox.codecs.pkhash.HashedTerms;
import org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashPostingsFormat;
import org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashTestUtil;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.AssertingDirectoryReader;
import org.apache.lucene.tests.search.FixedBitSetCollector;
import org.apache.lucene.tests.search.QueryUtils;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.RamUsageTester;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.automaton.ByteRunnable;

/**
 * Checks that {@link PrimaryKeyHashInSetQuery} matches exactly what {@link TermInSetQuery} matches,
 * on segments written with {@link PrimaryKeyHashPostingsFormat}, with another format, and on
 * indexes that mix both, through readers that expose {@link HashTerms} and readers that hide it.
 */
public class TestPrimaryKeyHashInSetQuery extends LuceneTestCase {

  private static final String ID = "id";
  private static final String SOFT_DELETES = "soft_deletes";

  /** Absent from every test index. */
  private static final String MISSING_FIELD = "missing";

  /** How the segments of a test index are written. */
  private enum Layout {
    /** Every segment with {@link PrimaryKeyHashPostingsFormat}. */
    HASH,
    /** Every segment with the test framework's codec. */
    DEFAULT,
    /** Both kinds of segments: the codec changes between writer sessions. */
    MIXED
  }

  private static Codec hashCodec(Random random) {
    return TestUtil.alwaysPostingsFormat(PrimaryKeyHashTestUtil.randomFormat(random));
  }

  /** A key that differs from {@code key} by one bit, is a prefix of it, or extends it. */
  private static BytesRef nearMiss(Random random, BytesRef key) {
    byte[] bytes = BytesRef.deepCopyOf(key).bytes;
    int length = key.length;
    switch (length == 0 ? 2 : random.nextInt(3)) {
      case 0:
        bytes[random.nextInt(length)] ^= (byte) (1 << random.nextInt(8));
        return new BytesRef(bytes, 0, length);
      case 1:
        return new BytesRef(bytes, 0, random.nextInt(length));
      default:
        byte[] longer = Arrays.copyOf(bytes, length + 1);
        longer[length] = (byte) random.nextInt(256);
        return new BytesRef(longer);
    }
  }

  private static Document doc(BytesRef key, int seq) {
    Document doc = new Document();
    doc.add(new StringField(ID, key, Field.Store.NO));
    doc.add(new StringField("seq", Integer.toString(seq), Field.Store.NO));
    return doc;
  }

  /** An index and every key ever added to it, deleted or not. */
  private record TestIndex(Directory dir, DirectoryReader reader, List<BytesRef> keys)
      implements Closeable {
    @Override
    public void close() throws IOException {
      IOUtils.close(reader, dir);
    }
  }

  /**
   * Builds an index of about {@code numDocs} documents. Most add a new key; some add a key that is
   * already there, update a key (with hard or soft deletes) or delete one, so that keys with
   * several documents, including deleted ones, occur.
   */
  private static TestIndex buildIndex(Layout layout, int numDocs, boolean allowDeletes)
      throws IOException {
    final Random random = random();
    final int shape = random.nextInt(6); // 5 mixes all shapes
    final boolean softDeletes = allowDeletes && random.nextInt(4) == 0;
    final int numSessions =
        layout == Layout.MIXED ? TestUtil.nextInt(random, 2, 4) : TestUtil.nextInt(random, 1, 2);
    final boolean hashFirst = random.nextBoolean();
    final List<BytesRef> keys = new ArrayList<>();
    final Directory dir = newDirectory();
    int seq = 0;
    for (int session = 0; session < numSessions; session++) {
      final boolean hash =
          switch (layout) {
            case HASH -> true;
            case DEFAULT -> false;
            case MIXED -> (session % 2 == 0) == hashFirst;
          };
      IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random));
      if (hash) {
        iwc.setCodec(hashCodec(random));
      }
      if (layout == Layout.MIXED) {
        // keep segments of both kinds, and not too many of them
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        iwc.setMaxBufferedDocs(TestUtil.nextInt(random, 100, 1000));
      }
      if (softDeletes) {
        iwc.setSoftDeletesField(SOFT_DELETES);
        iwc.setMergePolicy(
            new SoftDeletesRetentionMergePolicy(
                SOFT_DELETES, MatchAllDocsQuery::new, iwc.getMergePolicy()));
      }
      try (IndexWriter writer = new IndexWriter(dir, iwc)) {
        final int end = (int) ((long) numDocs * (session + 1) / numSessions);
        for (; seq < end; seq++) {
          final int op = keys.isEmpty() ? 3 : random.nextInt(20);
          if (op == 0) {
            writer.addDocument(doc(keys.get(random.nextInt(keys.size())), seq));
          } else if (op == 1 && allowDeletes) {
            BytesRef key = keys.get(random.nextInt(keys.size()));
            if (softDeletes) {
              writer.softUpdateDocument(
                  new Term(ID, key), doc(key, seq), new NumericDocValuesField(SOFT_DELETES, 1));
            } else {
              writer.updateDocument(new Term(ID, key), doc(key, seq));
            }
          } else if (op == 2 && allowDeletes) {
            writer.deleteDocuments(new Term(ID, keys.get(random.nextInt(keys.size()))));
          } else {
            BytesRef key = randomKey(random, shape == 5 ? random.nextInt(5) : shape, seq);
            keys.add(key);
            writer.addDocument(doc(key, seq));
          }
          if (random.nextInt(200) == 0) {
            writer.commit();
          }
        }
        if (layout != Layout.MIXED && random.nextInt(5) == 0) {
          writer.forceMerge(TestUtil.nextInt(random, 1, 3));
        }
      }
    }
    return new TestIndex(dir, DirectoryReader.open(dir), keys);
  }

  /**
   * Random query keys: indexed keys (deleted ones included), duplicates, near misses and absent
   * keys. Usually a few dozen, sometimes thousands, sometimes none.
   */
  private static List<BytesRef> randomQueryKeys(Random random, List<BytesRef> indexed, int max) {
    final int numKeys =
        random.nextInt(10) == 0
            ? TestUtil.nextInt(random, 0, max)
            : Math.min(max, random.nextInt(1 << TestUtil.nextInt(random, 1, 7)));
    final List<BytesRef> keys = new ArrayList<>(numKeys);
    for (int i = 0; i < numKeys; i++) {
      final int kind = random.nextInt(10);
      if (kind == 6 && keys.isEmpty() == false) {
        keys.add(BytesRef.deepCopyOf(keys.get(random.nextInt(keys.size()))));
      } else if (kind == 7) {
        keys.add(nearMiss(random, indexed.get(random.nextInt(indexed.size()))));
      } else if (kind >= 8) {
        keys.add(randomKey(random, random.nextInt(5), random.nextInt(1_000_000)));
      } else {
        keys.add(BytesRef.deepCopyOf(indexed.get(random.nextInt(indexed.size()))));
      }
    }
    return keys;
  }

  /**
   * Searchers over the reader: one from {@link #newSearcher(org.apache.lucene.index.IndexReader)},
   * one whose leaves are the segment readers themselves, so that hash tables are visible, and one
   * whose leaves wrap the segment readers, so that they are not.
   */
  private static List<IndexSearcher> searchers(DirectoryReader reader) throws IOException {
    return List.of(
        newSearcher(reader),
        newSearcher(reader, false),
        newSearcher(new AssertingDirectoryReader(reader), false));
  }

  private static void assertSameMatches(IndexSearcher searcher, Collection<BytesRef> keys)
      throws IOException {
    final float boost = random().nextBoolean() ? 1f : 10 * random().nextFloat();
    Query expected = new TermInSetQuery(ID, keys);
    Query actual = new PrimaryKeyHashInSetQuery(ID, keys);
    if (boost != 1f) {
      expected = new BoostQuery(expected, boost);
      actual = new BoostQuery(actual, boost);
    }
    final int maxDoc = Math.max(1, searcher.getIndexReader().maxDoc());

    TopDocs expectedTop = searcher.search(expected, maxDoc);
    TopDocs actualTop = searcher.search(actual, maxDoc);
    assertEquals(expectedTop.totalHits, actualTop.totalHits);
    assertEquals(expectedTop.scoreDocs.length, actualTop.scoreDocs.length);
    for (int i = 0; i < expectedTop.scoreDocs.length; i++) {
      assertEquals(expectedTop.scoreDocs[i].doc, actualTop.scoreDocs[i].doc);
      assertEquals(expectedTop.scoreDocs[i].score, actualTop.scoreDocs[i].score, 0f);
    }

    FixedBitSet expectedSet = searcher.search(expected, FixedBitSetCollector.createManager(maxDoc));
    FixedBitSet actualSet = searcher.search(actual, FixedBitSetCollector.createManager(maxDoc));
    assertEquals(expectedSet, actualSet);
    assertEquals(searcher.count(expected), searcher.count(actual));
  }

  public void testDuelHashSegments() throws IOException {
    doTestDuel(Layout.HASH);
  }

  public void testDuelDefaultSegments() throws IOException {
    doTestDuel(Layout.DEFAULT);
  }

  public void testDuelMixedSegments() throws IOException {
    doTestDuel(Layout.MIXED);
  }

  private void doTestDuel(Layout layout) throws IOException {
    try (TestIndex index = buildIndex(layout, atLeast(500), true)) {
      int hashLeaves = 0;
      int otherLeaves = 0;
      for (LeafReaderContext context : index.reader.leaves()) {
        Terms terms = context.reader().terms(ID);
        if (terms instanceof HashTerms) {
          hashLeaves++;
        } else if (terms != null) {
          otherLeaves++;
        }
      }
      if (layout == Layout.DEFAULT) {
        assertEquals(0, hashLeaves);
      } else if (layout == Layout.MIXED) {
        assertTrue("no segment with a hash table", hashLeaves > 0);
        assertTrue("no segment without a hash table", otherLeaves > 0);
      }

      for (IndexSearcher searcher : searchers(index.reader)) {
        final int iters = atLeast(10);
        for (int iter = 0; iter < iters; iter++) {
          assertSameMatches(searcher, randomQueryKeys(random(), index.keys, 5000));
        }
        // every indexed key at once
        assertSameMatches(searcher, index.keys);
      }
    }
  }

  /**
   * The checks of {@link QueryUtils}. Its matches check expects deleted documents not to match,
   * which {@link TermInSetQuery} does not guarantee either, so these indexes have no deletions (but
   * do have keys with several documents).
   */
  public void testQueryUtils() throws IOException {
    for (Layout layout : Layout.values()) {
      // QueryUtils creates a scorer per document, so keep this small
      try (TestIndex index = buildIndex(layout, atLeast(100), false)) {
        for (IndexSearcher searcher : searchers(index.reader)) {
          QueryUtils.check(
              random(),
              new PrimaryKeyHashInSetQuery(ID, randomQueryKeys(random(), index.keys, 50)),
              searcher);
        }
      }
    }
  }

  /**
   * Keys with several documents in a segment (added twice, or updated so that the old version is
   * still in the postings) are resolved through the terms dictionary, before and after a merge.
   */
  public void testMultiDocumentKeys() throws IOException {
    final Random random = random();
    final int numKeys = TestUtil.nextInt(random, 100, 500);
    final List<BytesRef> keys = new ArrayList<>();
    final Set<Integer> duplicated = new HashSet<>();
    final Set<Integer> updated = new HashSet<>();
    for (int i = 0; i < numKeys; i++) {
      keys.add(new BytesRef(String.format(Locale.ROOT, "key%05d", i)));
    }
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc =
          new IndexWriterConfig(new MockAnalyzer(random))
              .setCodec(hashCodec(random))
              .setMergePolicy(NoMergePolicy.INSTANCE);
      try (IndexWriter writer = new IndexWriter(dir, iwc)) {
        for (int i = 0; i < numKeys; i++) {
          writer.addDocument(doc(keys.get(i), i));
        }
        for (int i = 0; i < numKeys; i++) {
          if (i % 10 == 0) {
            duplicated.add(i);
            writer.addDocument(doc(keys.get(i), numKeys + i));
          } else if (i % 10 == 1) {
            updated.add(i);
            writer.updateDocument(new Term(ID, keys.get(i)), doc(keys.get(i), numKeys + i));
          }
        }
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals(1, reader.leaves().size());
        HashTerms terms = (HashTerms) reader.leaves().get(0).reader().terms(ID);
        int[] docs = new int[numKeys];
        terms.lookupBatch(new HashedTerms(keys), 0, numKeys, docs);
        for (int i = 0; i < numKeys; i++) {
          boolean multi = duplicated.contains(i) || updated.contains(i);
          assertEquals(multi, docs[i] == HashTerms.MULTI);
        }
        checkMultiDocumentKeys(reader, keys, duplicated);
      }

      // after a merge, the deleted versions of updated keys are gone
      IndexWriterConfig mergeConfig =
          new IndexWriterConfig(new MockAnalyzer(random)).setCodec(hashCodec(random));
      try (IndexWriter writer = new IndexWriter(dir, mergeConfig)) {
        writer.forceMerge(1);
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        HashTerms terms = (HashTerms) reader.leaves().get(0).reader().terms(ID);
        int[] docs = new int[numKeys];
        terms.lookupBatch(new HashedTerms(keys), 0, numKeys, docs);
        for (int i = 0; i < numKeys; i++) {
          assertEquals(duplicated.contains(i), docs[i] == HashTerms.MULTI);
        }
        checkMultiDocumentKeys(reader, keys, duplicated);
      }
    }
  }

  /**
   * A segment is looked up in windows of keys: windows of hits and misses, of hits, misses and keys
   * of several documents, of misses only and of hits only all match what {@link TermInSetQuery}
   * matches. The query tells keys of several documents from other results by their order: {@link
   * HashTerms#MULTI} is smaller than {@link HashTerms#ABSENT}, which is negative.
   */
  public void testWindowsOfHitsMissesAndMultiDocumentKeys() throws IOException {
    assertTrue(HashTerms.MULTI < HashTerms.ABSENT && HashTerms.ABSENT < 0);
    final Random random = random();
    final int window = PrimaryKeyHashInSetQuery.LOOKUP_WINDOW;
    final int numKeys = 6 * window + random.nextInt(window);
    final List<BytesRef> probes = new ArrayList<>();
    int expectedCount = 0;
    try (Directory dir = newDirectory()) {
      final IndexWriterConfig iwc =
          new IndexWriterConfig(new MockAnalyzer(random))
              .setCodec(hashCodec(random))
              .setMergePolicy(NoMergePolicy.INSTANCE);
      try (IndexWriter writer = new IndexWriter(dir, iwc)) {
        // Even keys are indexed, odd ones are not. The query sorts the probes, so each range of
        // keys below makes a window of probes, and the last range at least one.
        for (int i = 0; i < numKeys; i++) {
          final BytesRef key = new BytesRef(String.format(Locale.ROOT, "key%06d", i));
          final boolean indexed = i % 2 == 0;
          final boolean multi;
          if (indexed == false || i < window) {
            multi = false;
          } else if (i < 2 * window) {
            multi = i % 6 == 0;
          } else {
            multi = random.nextInt(100) == 0;
          }
          if (indexed) {
            writer.addDocument(doc(key, i));
            if (multi) {
              writer.addDocument(doc(key, numKeys + i));
            }
          }
          final boolean probed;
          if (i < 2 * window) {
            probed = true; // hits and misses, then keys of several documents among them too
          } else if (i < 4 * window) {
            probed = indexed == false; // misses only
          } else {
            probed = indexed; // hits only, a few keys of several documents among them
          }
          if (probed) {
            probes.add(key);
            expectedCount += indexed ? (multi ? 2 : 1) : 0;
          }
        }
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals(1, reader.leaves().size());
        assertTrue(reader.leaves().get(0).reader().terms(ID) instanceof HashTerms);
        final IndexSearcher searcher = newSearcher(reader, false);
        assertEquals(expectedCount, searcher.count(new PrimaryKeyHashInSetQuery(ID, probes)));
        assertSameMatches(searcher, probes);
      }
    }
  }

  private static void checkMultiDocumentKeys(
      DirectoryReader reader, List<BytesRef> keys, Set<Integer> duplicated) throws IOException {
    IndexSearcher searcher = newSearcher(reader, false);
    List<BytesRef> queryKeys = new ArrayList<>();
    int expectedCount = 0;
    for (int i = 0; i < keys.size(); i++) {
      if (random().nextInt(3) == 0) {
        queryKeys.add(keys.get(i));
        expectedCount += duplicated.contains(i) ? 2 : 1;
      }
    }
    assertEquals(expectedCount, searcher.count(new PrimaryKeyHashInSetQuery(ID, queryKeys)));
    assertSameMatches(searcher, queryKeys);
    List<BytesRef> duplicatedKeys = new ArrayList<>();
    for (int i : duplicated) {
      duplicatedKeys.add(keys.get(i));
    }
    assertEquals(
        2 * duplicated.size(), searcher.count(new PrimaryKeyHashInSetQuery(ID, duplicatedKeys)));
    assertSameMatches(searcher, duplicatedKeys);
    assertSameMatches(searcher, keys);
  }

  private static void addLetters(Directory dir, Codec codec) throws IOException {
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    if (codec != null) {
      iwc.setCodec(codec);
    }
    try (IndexWriter writer = new IndexWriter(dir, iwc)) {
      for (char ch = 'a'; ch <= 'z'; ch++) {
        Document doc = new Document();
        doc.add(new StringField(ID, Character.toString(ch), Field.Store.NO));
        doc.add(new StringField("content", Character.toString(ch), Field.Store.NO));
        writer.addDocument(doc);
      }
    }
  }

  private static ScorerSupplier scorerSupplier(
      IndexSearcher searcher, Query query, LeafReaderContext context) throws IOException {
    Weight weight =
        searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);
    return weight.scorerSupplier(context);
  }

  /** No scorer for segments without matches; otherwise the exact cost and ascending documents. */
  public void testScorerSupplier() throws IOException {
    for (boolean hash : new boolean[] {true, false}) {
      try (Directory dir = newDirectory()) {
        addLetters(dir, hash ? hashCodec(random()) : null);
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
          IndexSearcher searcher = new IndexSearcher(reader);
          searcher.setQueryCache(null);
          LeafReaderContext context = reader.leaves().get(0);
          assertEquals(hash, context.reader().terms(ID) instanceof HashTerms);

          Query noMatch =
              new PrimaryKeyHashInSetQuery(ID, new BytesRef("aaa"), new BytesRef("bbb"));
          assertNull(scorerSupplier(searcher, noMatch, context));
          assertNull(
              scorerSupplier(
                  searcher,
                  new PrimaryKeyHashInSetQuery(MISSING_FIELD, new BytesRef("a")),
                  context));

          Query match =
              new PrimaryKeyHashInSetQuery(
                  ID, new BytesRef("z"), new BytesRef("a"), new BytesRef("q"), new BytesRef("?"));
          ScorerSupplier supplier = scorerSupplier(searcher, match, context);
          assertNotNull(supplier);
          assertEquals(3, supplier.cost());
          Scorer scorer = supplier.get(Long.MAX_VALUE);
          DocIdSetIterator iterator = scorer.iterator();
          assertEquals(0, iterator.nextDoc());
          assertEquals(1f, scorer.score(), 0f);
          assertEquals('q' - 'a', iterator.nextDoc());
          assertEquals('z' - 'a', iterator.nextDoc());
          assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.nextDoc());

          // a required clause without matches makes the conjunction have no scorer either
          Query all = new TermInSetQuery("content", List.of(new BytesRef("a"), new BytesRef("b")));
          Query conjunction =
              new BooleanQuery.Builder().add(noMatch, Occur.FILTER).add(all, Occur.FILTER).build();
          assertNull(scorerSupplier(searcher, conjunction, context));
          conjunction =
              new BooleanQuery.Builder().add(match, Occur.FILTER).add(all, Occur.FILTER).build();
          assertNotNull(scorerSupplier(searcher, conjunction, context));
          assertEquals(1, searcher.count(conjunction));
        }
      }
    }
  }

  /**
   * Documents with several keys: a document matched by more than one of the keys of a query is
   * returned once, and its scorer advances correctly.
   */
  public void testDocumentsWithSeveralKeys() throws IOException {
    final Random random = random();
    final List<BytesRef> keys = new ArrayList<>();
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc =
          new IndexWriterConfig(new MockAnalyzer(random))
              .setCodec(hashCodec(random))
              .setMergePolicy(NoMergePolicy.INSTANCE);
      final int numDocs = TestUtil.nextInt(random, 1, 200);
      final List<List<BytesRef>> keysByDoc = new ArrayList<>();
      try (IndexWriter writer = new IndexWriter(dir, iwc)) {
        for (int i = 0; i < numDocs; i++) {
          Document doc = new Document();
          List<BytesRef> docKeys = new ArrayList<>();
          int numKeys = TestUtil.nextInt(random, 1, 3);
          for (int k = 0; k < numKeys; k++) {
            BytesRef key = new BytesRef(String.format(Locale.ROOT, "%04d-%d", i, k));
            docKeys.add(key);
            doc.add(new StringField(ID, key, Field.Store.NO));
          }
          keys.addAll(docKeys);
          keysByDoc.add(docKeys);
          writer.addDocument(doc);
        }
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = newSearcher(reader, false);
        searcher.setQueryCache(null);
        LeafReaderContext context = reader.leaves().get(0);
        assertTrue(context.reader().terms(ID) instanceof HashTerms);
        for (int iter = 0; iter < 20; iter++) {
          // usually a few keys, whose documents are sorted in place, sometimes many
          final boolean few = random.nextBoolean();
          int numQueryKeys = few ? random.nextInt(10) : random.nextInt(keys.size() + 1);
          List<BytesRef> queryKeys = new ArrayList<>();
          FixedBitSet expected = new FixedBitSet(numDocs);
          for (int k = 0; k < numQueryKeys; k++) {
            int doc = random.nextInt(numDocs);
            List<BytesRef> docKeys = keysByDoc.get(doc);
            if (random.nextBoolean()) {
              queryKeys.addAll(docKeys); // a document found through each of its keys
            } else {
              queryKeys.add(docKeys.get(random.nextInt(docKeys.size())));
            }
            expected.set(doc);
          }
          assertSameMatches(searcher, queryKeys);
          ScorerSupplier supplier =
              scorerSupplier(searcher, new PrimaryKeyHashInSetQuery(ID, queryKeys), context);
          if (queryKeys.isEmpty()) {
            assertNull(supplier);
            continue;
          }
          if (few) {
            // exact; many documents of a multi-valued field may get an estimate
            assertEquals(expected.cardinality(), supplier.cost());
          }
          DocIdSetIterator iterator = supplier.get(Long.MAX_VALUE).iterator();
          for (int current = -1; current != DocIdSetIterator.NO_MORE_DOCS; ) {
            int target = current + 1 + (random.nextBoolean() ? random.nextInt(3) : 0);
            int doc = target == current + 1 ? iterator.nextDoc() : iterator.advance(target);
            int expectedDoc =
                target < numDocs ? expected.nextSetBit(target) : DocIdSetIterator.NO_MORE_DOCS;
            assertEquals(expectedDoc, doc);
            current = doc;
          }
        }
      }
    }
  }

  public void testEmptyKeySet() throws IOException {
    PrimaryKeyHashInSetQuery empty = new PrimaryKeyHashInSetQuery(ID);
    QueryUtils.checkEqual(empty, new PrimaryKeyHashInSetQuery(ID, Collections.emptyList()));
    assertEquals(0, empty.getKeys().size());
    assertEquals("id:()", empty.toString());
    empty.visit(
        new QueryVisitor() {
          @Override
          public void consumeTerms(Query query, Term... terms) {
            fail("no keys, no terms");
          }

          @Override
          public void consumeTermsMatching(
              Query query, String field, Supplier<ByteRunnable> automaton) {
            fail("no keys, no terms");
          }
        });
    try (Directory dir = newDirectory()) {
      addLetters(dir, random().nextBoolean() ? hashCodec(random()) : null);
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = newSearcher(reader);
        assertTrue(searcher.rewrite(empty) instanceof MatchNoDocsQuery);
        assertEquals(0, searcher.count(empty));
        // the query as it is, without rewriting
        Weight weight = empty.createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1f);
        for (LeafReaderContext context : reader.leaves()) {
          assertNull(weight.scorerSupplier(context));
          assertNull(weight.matches(context, 0));
        }
      }
    }
  }

  public void testEqualsAndHashCode() {
    final int num = atLeast(100);
    final List<BytesRef> keys = new ArrayList<>();
    final Set<BytesRef> uniqueKeys = new HashSet<>();
    for (int i = 0; i < num; i++) {
      String string = TestUtil.randomRealisticUnicodeString(random());
      keys.add(new BytesRef(string));
      uniqueKeys.add(new BytesRef(string));
      PrimaryKeyHashInSetQuery left = new PrimaryKeyHashInSetQuery(ID, uniqueKeys);
      Collections.shuffle(keys, random());
      PrimaryKeyHashInSetQuery right = new PrimaryKeyHashInSetQuery(ID, keys);
      QueryUtils.checkEqual(left, right);
      QueryUtils.checkEqual(left, new PrimaryKeyHashInSetQuery(ID, keys.toArray(new BytesRef[0])));
      QueryUtils.checkUnequal(left, new PrimaryKeyHashInSetQuery("other", keys));
      QueryUtils.checkUnequal(left, new TermInSetQuery(ID, keys));
      if (uniqueKeys.size() > 1) {
        List<BytesRef> fewer = new ArrayList<>(uniqueKeys);
        fewer.remove(0);
        QueryUtils.checkUnequal(left, new PrimaryKeyHashInSetQuery(ID, fewer));
      }
      QueryUtils.check(left);
    }

    // same String hash code, different keys
    assertEquals("AaAaBB".hashCode(), "BBBBBB".hashCode());
    QueryUtils.checkUnequal(
        new PrimaryKeyHashInSetQuery(ID, new BytesRef("AaAaAa"), new BytesRef("AaAaBB")),
        new PrimaryKeyHashInSetQuery(ID, new BytesRef("AaAaAa"), new BytesRef("BBBBBB")));
    // the same keys split differently
    QueryUtils.checkUnequal(
        new PrimaryKeyHashInSetQuery(ID, new BytesRef("ab"), new BytesRef("c")),
        new PrimaryKeyHashInSetQuery(ID, new BytesRef("a"), new BytesRef("bc")));
    assertNotEquals(
        new PrimaryKeyHashInSetQuery("thing", new BytesRef("apple")).hashCode(),
        new PrimaryKeyHashInSetQuery("thing2", new BytesRef("apple")).hashCode());
  }

  public void testGettersAndToString() {
    PrimaryKeyHashInSetQuery query =
        new PrimaryKeyHashInSetQuery(
            "field1", List.of(new BytesRef("c"), new BytesRef("a"), new BytesRef("b")));
    assertEquals("field1", query.getField());
    assertEquals(3, query.getKeys().size());
    assertEquals(new BytesRef("a"), query.getKeys().get(0));
    assertEquals("field1:(a b c)", query.toString());
    assertEquals("field1:(a b c)", query.toString("field1"));

    // like TermInSetQuery, keys that are not UTF-8 are printed as bytes
    BytesRef binary = new BytesRef(new byte[] {(byte) 0xff, (byte) 0xfe});
    assertEquals(
        new TermInSetQuery("f", List.of(binary)).toString(),
        new PrimaryKeyHashInSetQuery("f", binary).toString());
  }

  public void testNullArguments() {
    expectThrows(
        NullPointerException.class,
        () -> new PrimaryKeyHashInSetQuery(null, List.of(new BytesRef("a"))));
    expectThrows(
        NullPointerException.class,
        () -> new PrimaryKeyHashInSetQuery(ID, (Collection<BytesRef>) null));
    expectThrows(
        NullPointerException.class, () -> new PrimaryKeyHashInSetQuery(ID, (BytesRef[]) null));
    expectThrows(
        NullPointerException.class,
        () -> new PrimaryKeyHashInSetQuery(ID, new BytesRef("a"), null, new BytesRef("b")));
  }

  public void testVisitor() {
    PrimaryKeyHashInSetQuery singleton =
        new PrimaryKeyHashInSetQuery("field", new BytesRef("term1"));
    singleton.visit(
        new QueryVisitor() {
          @Override
          public void consumeTerms(Query query, Term... terms) {
            assertEquals(1, terms.length);
            assertEquals(new Term("field", new BytesRef("term1")), terms[0]);
          }

          @Override
          public void consumeTermsMatching(
              Query query, String field, Supplier<ByteRunnable> automaton) {
            fail("a single key is reported as a term");
          }
        });

    List<BytesRef> keys = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      keys.add(new BytesRef("term" + i));
    }
    PrimaryKeyHashInSetQuery query = new PrimaryKeyHashInSetQuery("field", keys);
    int[] calls = new int[1];
    query.visit(
        new QueryVisitor() {
          @Override
          public void consumeTerms(Query query, Term... terms) {
            fail("several keys are reported as an automaton");
          }

          @Override
          public void consumeTermsMatching(
              Query query, String field, Supplier<ByteRunnable> automaton) {
            calls[0]++;
            assertEquals("field", field);
            ByteRunnable a = automaton.get();
            BytesRef test = new BytesRef("nonmatching");
            assertFalse(a.run(test.bytes, test.offset, test.length));
            test = new BytesRef("term");
            assertFalse(a.run(test.bytes, test.offset, test.length));
            for (BytesRef key : keys) {
              assertTrue(a.run(key.bytes, key.offset, key.length));
            }
          }
        });
    assertEquals(1, calls[0]);

    query.visit(
        new QueryVisitor() {
          @Override
          public boolean acceptField(String field) {
            return false;
          }

          @Override
          public void consumeTerms(Query query, Term... terms) {
            fail("field not accepted");
          }

          @Override
          public void consumeTermsMatching(
              Query query, String field, Supplier<ByteRunnable> automaton) {
            fail("field not accepted");
          }
        });
  }

  public void testRamBytesUsed() {
    List<BytesRef> keys = new ArrayList<>();
    final int numKeys = 10000 + random().nextInt(1000);
    for (int i = 0; i < numKeys; i++) {
      keys.add(new BytesRef(TestUtil.randomUnicodeString(random(), 10)));
    }
    PrimaryKeyHashInSetQuery query = new PrimaryKeyHashInSetQuery("f", keys);
    final long actualRamBytesUsed = RamUsageTester.ramUsed(query);
    final long expectedRamBytesUsed = query.ramBytesUsed();
    // error margin within 5%
    assertEquals(
        (double) expectedRamBytesUsed, (double) actualRamBytesUsed, actualRamBytesUsed / 20.d);
  }
}
