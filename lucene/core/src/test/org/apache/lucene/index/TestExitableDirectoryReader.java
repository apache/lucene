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
package org.apache.lucene.index;

import static com.carrotsearch.randomizedtesting.RandomizedTest.atMost;

import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.KnnVectorField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.ExitableDirectoryReader.ExitingReaderException;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

/**
 * Test that uses a default/lucene Implementation of {@link QueryTimeout} to exit out long running
 * queries that take too long to iterate over Terms.
 */
public class TestExitableDirectoryReader extends LuceneTestCase {
  private static class TestReader extends FilterLeafReader {

    private static class TestTerms extends FilterTerms {
      TestTerms(Terms in) {
        super(in);
      }

      @Override
      public TermsEnum iterator() throws IOException {
        return new TestTermsEnum(super.iterator());
      }
    }

    private static class TestTermsEnum extends FilterTermsEnum {
      public TestTermsEnum(TermsEnum in) {
        super(in);
      }

      /** Sleep between iterations to timeout things. */
      @Override
      public BytesRef next() throws IOException {
        try {
          // Sleep for 100ms before each .next() call.
          Thread.sleep(100);
        } catch (
            @SuppressWarnings("unused")
            InterruptedException e) {
        }
        return in.next();
      }
    }

    public TestReader(LeafReader reader) throws IOException {
      super(reader);
    }

    @Override
    public Terms terms(String field) throws IOException {
      Terms terms = super.terms(field);
      return terms == null ? null : new TestTerms(terms);
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
      return in.getCoreCacheHelper();
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
      return in.getReaderCacheHelper();
    }
  }

  /**
   * Tests timing out of TermsEnum iterations
   *
   * @throws Exception on error
   */
  public void testExitableFilterTermsIndexReader() throws Exception {
    Directory directory = newDirectory();
    IndexWriter writer =
        new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random())));

    Document d1 = new Document();
    d1.add(newTextField("default", "one two", Field.Store.YES));
    writer.addDocument(d1);

    Document d2 = new Document();
    d2.add(newTextField("default", "one three", Field.Store.YES));
    writer.addDocument(d2);

    Document d3 = new Document();
    d3.add(newTextField("default", "ones two four", Field.Store.YES));
    writer.addDocument(d3);

    writer.forceMerge(1);
    writer.commit();
    writer.close();

    DirectoryReader directoryReader;
    DirectoryReader exitableDirectoryReader;
    IndexReader reader;
    IndexSearcher searcher;

    Query query = new PrefixQuery(new Term("default", "o"));

    // Set a fairly high timeout value (infinite) and expect the query to complete in that time
    // frame.
    // Not checking the validity of the result, all we are bothered about in this test is the timing
    // out.
    directoryReader = DirectoryReader.open(directory);
    exitableDirectoryReader = new ExitableDirectoryReader(directoryReader, infiniteQueryTimeout());
    reader = new TestReader(getOnlyLeafReader(exitableDirectoryReader));
    searcher = new IndexSearcher(reader);
    searcher.search(query, 10);
    reader.close();

    // Set a really low timeout value (immediate) and expect an Exception
    directoryReader = DirectoryReader.open(directory);
    exitableDirectoryReader = new ExitableDirectoryReader(directoryReader, immediateQueryTimeout());
    reader = new TestReader(getOnlyLeafReader(exitableDirectoryReader));
    IndexSearcher slowSearcher = new IndexSearcher(reader);
    expectThrows(
        ExitingReaderException.class,
        () -> {
          slowSearcher.search(query, 10);
        });
    reader.close();

    // Set maximum time out and expect the query to complete.
    // Not checking the validity of the result, all we are bothered about in this test is the timing
    // out.
    directoryReader = DirectoryReader.open(directory);
    exitableDirectoryReader = new ExitableDirectoryReader(directoryReader, infiniteQueryTimeout());
    reader = new TestReader(getOnlyLeafReader(exitableDirectoryReader));
    searcher = new IndexSearcher(reader);
    searcher.search(query, 10);
    reader.close();
    directory.close();
  }

  /**
   * Tests time out check sampling of TermsEnum iterations
   *
   * @throws Exception on error
   */
  public void testExitableTermsEnumSampleTimeoutCheck() throws Exception {
    try (Directory directory = newDirectory()) {
      try (IndexWriter writer =
          new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random())))) {
        for (int i = 0; i < 50; i++) {
          Document d1 = new Document();
          d1.add(newTextField("default", "term" + i, Field.Store.YES));
          writer.addDocument(d1);
        }

        writer.forceMerge(1);
        writer.commit();

        DirectoryReader directoryReader;
        DirectoryReader exitableDirectoryReader;
        IndexReader reader;
        IndexSearcher searcher;

        Query query = new PrefixQuery(new Term("default", "term"));

        // Set a fairly high timeout value (infinite) and expect the query to complete in that time
        // frame.
        // Not checking the validity of the result, but checking the sampling kicks in to reduce the
        // number of timeout check
        CountingQueryTimeout queryTimeout = new CountingQueryTimeout();
        directoryReader = DirectoryReader.open(directory);
        exitableDirectoryReader = new ExitableDirectoryReader(directoryReader, queryTimeout);
        reader = new TestReader(getOnlyLeafReader(exitableDirectoryReader));
        searcher = new IndexSearcher(reader);
        searcher.search(query, 300);
        reader.close();
        // The number of sampled query time out check here depends on two factors:
        // 1. ExitableDirectoryReader.ExitableTermsEnum.NUM_CALLS_PER_TIMEOUT_CHECK
        // 2. MultiTermQueryConstantScoreWrapper.BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD
        assertEquals(5, queryTimeout.getShouldExitCallCount());
      }
    }
  }

  /**
   * Tests timing out of PointValues queries
   *
   * @throws Exception on error
   */
  public void testExitablePointValuesIndexReader() throws Exception {
    Directory directory = newDirectory();
    IndexWriter writer =
        new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random())));

    Document d1 = new Document();
    d1.add(new IntPoint("default", 10));
    writer.addDocument(d1);

    Document d2 = new Document();
    d2.add(new IntPoint("default", 100));
    writer.addDocument(d2);

    Document d3 = new Document();
    d3.add(new IntPoint("default", 1000));
    writer.addDocument(d3);

    writer.forceMerge(1);
    writer.commit();
    writer.close();

    DirectoryReader directoryReader;
    DirectoryReader exitableDirectoryReader;
    IndexReader reader;
    IndexSearcher searcher;

    Query query = IntPoint.newRangeQuery("default", 10, 20);

    // Set a fairly high timeout value (infinite) and expect the query to complete in that time
    // frame.
    // Not checking the validity of the result, all we are bothered about in this test is the timing
    // out.
    directoryReader = DirectoryReader.open(directory);
    exitableDirectoryReader = new ExitableDirectoryReader(directoryReader, infiniteQueryTimeout());
    reader = new TestReader(getOnlyLeafReader(exitableDirectoryReader));
    searcher = new IndexSearcher(reader);
    searcher.search(query, 10);
    reader.close();

    // Set a really low timeout value (immediate) and expect an Exception
    directoryReader = DirectoryReader.open(directory);
    exitableDirectoryReader = new ExitableDirectoryReader(directoryReader, immediateQueryTimeout());
    reader = new TestReader(getOnlyLeafReader(exitableDirectoryReader));
    IndexSearcher slowSearcher = new IndexSearcher(reader);
    expectThrows(
        ExitingReaderException.class,
        () -> {
          slowSearcher.search(query, 10);
        });
    reader.close();

    // Set maximum time out and expect the query to complete.
    // Not checking the validity of the result, all we are bothered about in this test is the timing
    // out.
    directoryReader = DirectoryReader.open(directory);
    exitableDirectoryReader = new ExitableDirectoryReader(directoryReader, infiniteQueryTimeout());
    reader = new TestReader(getOnlyLeafReader(exitableDirectoryReader));
    searcher = new IndexSearcher(reader);
    searcher.search(query, 10);
    reader.close();
    directory.close();
  }

  public void testExitableTermsMinAndMax() throws IOException {
    Directory directory = newDirectory();
    IndexWriter w = new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random())));
    Document doc = new Document();
    StringField fooField = new StringField("foo", "bar", Field.Store.NO);
    doc.add(fooField);
    w.addDocument(doc);
    w.flush();

    DirectoryReader directoryReader = DirectoryReader.open(w);
    for (LeafReaderContext lfc : directoryReader.leaves()) {
      ExitableDirectoryReader.ExitableTerms terms =
          new ExitableDirectoryReader.ExitableTerms(
              lfc.reader().terms("foo"), infiniteQueryTimeout()) {
            @Override
            public TermsEnum iterator() {
              fail("min and max should be retrieved from block tree, no need to iterate");
              return null;
            }
          };
      assertEquals("bar", terms.getMin().utf8ToString());
      assertEquals("bar", terms.getMax().utf8ToString());
    }

    w.close();
    directoryReader.close();
    directory.close();
  }

  private static QueryTimeout infiniteQueryTimeout() {
    return () -> false;
  }

  private static class CountingQueryTimeout implements QueryTimeout {
    private int counter = 0;

    @Override
    public boolean shouldExit() {
      counter++;
      return false;
    }

    public int getShouldExitCallCount() {
      return counter;
    }
  }

  private static QueryTimeout immediateQueryTimeout() {
    return () -> true;
  }

  @FunctionalInterface
  interface DvFactory {
    DocValuesIterator create(LeafReader leaf) throws IOException;
  }

  public void testDocValues() throws IOException {
    Directory directory = newDirectory();
    IndexWriter writer =
        new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random())));

    Document d1 = new Document();
    addDVs(d1, 10);
    writer.addDocument(d1);

    Document d2 = new Document();
    addDVs(d2, 100);
    writer.addDocument(d2);

    Document d3 = new Document();
    addDVs(d3, 1000);
    writer.addDocument(d3);

    writer.forceMerge(1);
    writer.commit();
    writer.close();

    DirectoryReader directoryReader;
    DirectoryReader exitableDirectoryReader;

    for (DvFactory dvFactory :
        Arrays.<DvFactory>asList(
            (r) -> r.getSortedDocValues("sorted"),
            (r) -> r.getSortedSetDocValues("sortedset"),
            (r) -> r.getSortedNumericDocValues("sortednumeric"),
            (r) -> r.getNumericDocValues("numeric"),
            (r) -> r.getBinaryDocValues("binary"))) {
      directoryReader = DirectoryReader.open(directory);
      exitableDirectoryReader =
          new ExitableDirectoryReader(directoryReader, immediateQueryTimeout());

      {
        IndexReader reader = new TestReader(getOnlyLeafReader(exitableDirectoryReader));

        expectThrows(
            ExitingReaderException.class,
            () -> {
              LeafReader leaf = reader.leaves().get(0).reader();
              DocValuesIterator iter = dvFactory.create(leaf);
              scan(leaf, iter);
            });
        reader.close();
      }

      directoryReader = DirectoryReader.open(directory);
      exitableDirectoryReader =
          new ExitableDirectoryReader(directoryReader, infiniteQueryTimeout());
      {
        IndexReader reader = new TestReader(getOnlyLeafReader(exitableDirectoryReader));
        final LeafReader leaf = reader.leaves().get(0).reader();
        scan(leaf, dvFactory.create(leaf));
        assertNull(leaf.getNumericDocValues("absent"));
        assertNull(leaf.getBinaryDocValues("absent"));
        assertNull(leaf.getSortedDocValues("absent"));
        assertNull(leaf.getSortedNumericDocValues("absent"));
        assertNull(leaf.getSortedSetDocValues("absent"));

        reader.close();
      }
    }

    directory.close();
  }

  public void testVectorValues() throws IOException {
    Directory directory = newDirectory();
    IndexWriter writer =
        new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random())));

    int numDoc = atLeast(20);
    int deletedDoc = atMost(5);
    int dimension = atLeast(3);

    for (int i = 0; i < numDoc; i++) {
      Document doc = new Document();

      float[] value = new float[dimension];
      for (int j = 0; j < dimension; j++) {
        value[j] = random().nextFloat();
      }
      FieldType fieldType =
          KnnVectorField.createFieldType(dimension, VectorSimilarityFunction.COSINE);
      doc.add(new KnnVectorField("vector", value, fieldType));

      doc.add(new StringField("id", Integer.toString(i), Field.Store.YES));
      writer.addDocument(doc);
    }

    writer.forceMerge(1);
    writer.commit();

    for (int i = 0; i < deletedDoc; i++) {
      writer.deleteDocuments(new Term("id", Integer.toString(i)));
    }

    writer.close();

    QueryTimeout queryTimeout;
    if (random().nextBoolean()) {
      queryTimeout = immediateQueryTimeout();
    } else {
      queryTimeout = infiniteQueryTimeout();
    }

    DirectoryReader directoryReader = DirectoryReader.open(directory);
    DirectoryReader exitableDirectoryReader = directoryReader;
    exitableDirectoryReader = new ExitableDirectoryReader(directoryReader, queryTimeout);
    IndexReader reader = new TestReader(getOnlyLeafReader(exitableDirectoryReader));

    LeafReaderContext context = reader.leaves().get(0);
    LeafReader leaf = context.reader();

    if (queryTimeout.shouldExit()) {
      expectThrows(
          ExitingReaderException.class,
          () -> {
            DocIdSetIterator iter = leaf.getVectorValues("vector");
            scanAndRetrieve(leaf, iter);
          });

      expectThrows(
          ExitingReaderException.class,
          () ->
              leaf.searchNearestVectors(
                  "vector", new float[dimension], 5, leaf.getLiveDocs(), Integer.MAX_VALUE));
    } else {
      DocIdSetIterator iter = leaf.getVectorValues("vector");
      scanAndRetrieve(leaf, iter);

      leaf.searchNearestVectors(
          "vector", new float[dimension], 5, leaf.getLiveDocs(), Integer.MAX_VALUE);
    }

    reader.close();
    directory.close();
  }

  private static void scanAndRetrieve(LeafReader leaf, DocIdSetIterator iter) throws IOException {
    for (iter.nextDoc();
        iter.docID() != DocIdSetIterator.NO_MORE_DOCS && iter.docID() < leaf.maxDoc(); ) {
      final int nextDocId = iter.docID() + 1;
      if (random().nextBoolean() && nextDocId < leaf.maxDoc()) {
        iter.advance(nextDocId);
      } else {
        iter.nextDoc();
      }

      if (random().nextBoolean()
          && iter.docID() != DocIdSetIterator.NO_MORE_DOCS
          && iter instanceof VectorValues) {
        ((VectorValues) iter).vectorValue();
      }
    }
  }

  private static void scan(LeafReader leaf, DocValuesIterator iter) throws IOException {
    for (iter.nextDoc();
        iter.docID() != DocIdSetIterator.NO_MORE_DOCS && iter.docID() < leaf.maxDoc(); ) {
      final int nextDocId = iter.docID() + 1;
      if (random().nextBoolean() && nextDocId < leaf.maxDoc()) {
        if (random().nextBoolean()) {
          iter.advance(nextDocId);
        } else {
          iter.advanceExact(nextDocId);
        }
      } else {
        iter.nextDoc();
      }
    }
  }

  private void addDVs(Document d1, int i) {
    d1.add(new NumericDocValuesField("numeric", i));
    d1.add(new BinaryDocValuesField("binary", new BytesRef("" + i)));
    d1.add(new SortedDocValuesField("sorted", new BytesRef("" + i)));
    d1.add(new SortedNumericDocValuesField("sortednumeric", i));
    d1.add(new SortedSetDocValuesField("sortedset", new BytesRef("" + i)));
  }
}
