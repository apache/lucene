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

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;

/**
 * Tests for the {@link QueryReadHint} plumbing on {@link IndexSearcher}. The hint is opt-in and
 * advisory; this verifies only that {@link IndexSearcher#setReadHints(Set)} stores an immutable,
 * defensively-copied snapshot and that the default is empty. End-to-end reachability of the hint
 * into a reader (the only built-in consumer) is covered by {@code
 * TestHnswVectorReaderPrefetchHint}.
 */
public class TestQueryReadHint extends LuceneTestCase {

  public void testIndexSearcherStoresHintsImmutably() throws IOException {
    try (Directory dir = newDirectory()) {
      try (IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new StringField("f", "v", Field.Store.NO));
        iw.addDocument(doc);
      }
      try (DirectoryReader r = DirectoryReader.open(dir)) {
        IndexSearcher s = new IndexSearcher(r);
        assertEquals(Set.of(), s.getReadHints());

        s.setReadHints(Set.of(QueryAccessHint.POINT));
        assertEquals(Set.of(QueryAccessHint.POINT), s.getReadHints());

        // returned set must be immutable
        Set<QueryReadHint> snapshot = s.getReadHints();
        expectThrows(
            UnsupportedOperationException.class, () -> snapshot.add(QueryAccessHint.POINT));

        // mutating an externally-passed set after handing it over must NOT affect the searcher
        HashSet<QueryReadHint> mutable = new HashSet<>();
        mutable.add(QueryAccessHint.POINT);
        s.setReadHints(mutable);
        mutable.clear();
        assertEquals(Set.of(QueryAccessHint.POINT), s.getReadHints());

        // empty restores default
        s.setReadHints(Set.of());
        assertEquals(Set.of(), s.getReadHints());

        expectThrows(NullPointerException.class, () -> s.setReadHints(null));
      }
    }
  }

  // ---- equals/hashCode contract for the per-query withReadHints withers (advisory but part of
  // query identity, so same-hint copies must be equal AND share a hashCode). ----

  public void testWithReadHintsEqualsAndHashCode() {
    float[] f = new float[] {1f, 0f, 0f};
    byte[] b = new byte[] {1, 2, 3};
    Set<QueryReadHint> point = Set.of(QueryAccessHint.POINT);

    // Two independently-built copies with the same hint are equal and share a hashCode.
    assertEqualAndHash(
        new KnnFloatVectorQuery("f", f, 5).withReadHints(point),
        new KnnFloatVectorQuery("f", f, 5).withReadHints(point));
    assertEqualAndHash(
        new KnnByteVectorQuery("f", b, 5).withReadHints(point),
        new KnnByteVectorQuery("f", b, 5).withReadHints(point));
    assertEqualAndHash(
        new FloatVectorSimilarityQuery("f", f, 0f, 0.5f, null).withReadHints(point),
        new FloatVectorSimilarityQuery("f", f, 0f, 0.5f, null).withReadHints(point));
    assertEqualAndHash(
        new ByteVectorSimilarityQuery("f", b, 0f, 0.5f, null).withReadHints(point),
        new ByteVectorSimilarityQuery("f", b, 0f, 0.5f, null).withReadHints(point));

    // A hinted query differs from its plain origin (distinct cache keys).
    KnnFloatVectorQuery plain = new KnnFloatVectorQuery("f", f, 5);
    assertNotEquals(plain, plain.withReadHints(point));
    // withReadHints(empty) carries no hint, so it equals the plain query.
    assertEqualAndHash(plain, plain.withReadHints(Set.of()));
  }

  public void testWithReadHintsEqualityThroughSeededAndPatience() {
    float[] f = new float[] {1f, 0f, 0f};
    Set<QueryReadHint> point = Set.of(QueryAccessHint.POINT);
    Query seed = new MatchAllDocsQuery();

    KnnFloatVectorQuery plain = new KnnFloatVectorQuery("f", f, 5);
    KnnFloatVectorQuery hinted = plain.withReadHints(point);

    // Seeded/Patience carry no independent hint state; identity flows through the delegate, so a
    // hinted vs. un-hinted inner query yields distinct wrappers.
    assertEqualAndHash(
        SeededKnnVectorQuery.fromFloatQuery(hinted, seed),
        SeededKnnVectorQuery.fromFloatQuery(hinted, seed));
    assertNotEquals(
        SeededKnnVectorQuery.fromFloatQuery(plain, seed),
        SeededKnnVectorQuery.fromFloatQuery(hinted, seed));

    assertEqualAndHash(
        PatienceKnnVectorQuery.fromFloatQuery(hinted),
        PatienceKnnVectorQuery.fromFloatQuery(hinted));
    assertNotEquals(
        PatienceKnnVectorQuery.fromFloatQuery(plain),
        PatienceKnnVectorQuery.fromFloatQuery(hinted));
  }

  private static void assertEqualAndHash(Query a, Query b) {
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
  }
}
