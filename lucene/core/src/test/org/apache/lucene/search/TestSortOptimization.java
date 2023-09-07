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

import static org.apache.lucene.search.SortField.FIELD_DOC;
import static org.apache.lucene.search.SortField.FIELD_SCORE;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.IntRange;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.search.CheckHits;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

public class TestSortOptimization extends LuceneTestCase {

  public void testLongSortOptimization() throws IOException {

    final Directory dir = newDirectory();
    IndexWriterConfig config =
        new IndexWriterConfig()
            // Make sure to use the default codec, otherwise some random points formats that have
            // large values for maxPointsPerLeaf might not enable skipping with only 10k docs
            .setCodec(TestUtil.getDefaultCodec());
    final IndexWriter writer = new IndexWriter(dir, config);
    final int numDocs = atLeast(10000);
    for (int i = 0; i < numDocs; ++i) {
      final Document doc = new Document();
      doc.add(new NumericDocValuesField("my_field", i));
      doc.add(new LongPoint("my_field", i));
      writer.addDocument(doc);
      if (i == 7000) writer.flush(); // two segments
    }
    final IndexReader reader = DirectoryReader.open(writer);
    writer.close();
    // single threaded so totalhits is deterministic
    IndexSearcher searcher = newSearcher(reader, true, true, false);
    final SortField sortField = new SortField("my_field", SortField.Type.LONG);
    final Sort sort = new Sort(sortField);
    final int numHits = 3;
    final int totalHitsThreshold = 3;

    { // simple sort
      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          TopFieldCollector.createSharedManager(sort, numHits, null, totalHitsThreshold);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);
      assertEquals(topDocs.scoreDocs.length, numHits);
      for (int i = 0; i < numHits; i++) {
        FieldDoc fieldDoc = (FieldDoc) topDocs.scoreDocs[i];
        assertEquals(i, ((Long) fieldDoc.fields[0]).intValue());
      }
      assertEquals(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO, topDocs.totalHits.relation);
      assertNonCompetitiveHitsAreSkipped(topDocs.totalHits.value, numDocs);
    }

    { // paging sort with after
      long afterValue = 2;
      FieldDoc after = new FieldDoc(2, Float.NaN, new Long[] {afterValue});
      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          TopFieldCollector.createSharedManager(sort, numHits, after, totalHitsThreshold);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);
      assertEquals(topDocs.scoreDocs.length, numHits);
      for (int i = 0; i < numHits; i++) {
        FieldDoc fieldDoc = (FieldDoc) topDocs.scoreDocs[i];
        assertEquals(afterValue + 1 + i, fieldDoc.fields[0]);
      }
      assertEquals(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO, topDocs.totalHits.relation);
      assertNonCompetitiveHitsAreSkipped(topDocs.totalHits.value, numDocs);
    }

    { // test that if there is the secondary sort on _score, scores are filled correctly
      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          TopFieldCollector.createSharedManager(
              new Sort(sortField, FIELD_SCORE), numHits, null, totalHitsThreshold);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);
      assertEquals(topDocs.scoreDocs.length, numHits);
      for (int i = 0; i < numHits; i++) {
        FieldDoc fieldDoc = (FieldDoc) topDocs.scoreDocs[i];
        assertEquals(i, ((Long) fieldDoc.fields[0]).intValue());
        float score = (float) fieldDoc.fields[1];
        assertEquals(1.0, score, 0.001);
      }
      assertEquals(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO, topDocs.totalHits.relation);
      assertNonCompetitiveHitsAreSkipped(topDocs.totalHits.value, numDocs);
    }

    { // test that if numeric field is a secondary sort, no optimization is run
      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          TopFieldCollector.createSharedManager(
              new Sort(FIELD_SCORE, sortField), numHits, null, totalHitsThreshold);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);
      assertEquals(topDocs.scoreDocs.length, numHits);
      assertEquals(
          topDocs.totalHits.value,
          numDocs); // assert that all documents were collected => optimization was not run
    }

    reader.close();
    dir.close();
  }

  /**
   * test that even if a field is not indexed with points, optimized sort still works as expected,
   * although no optimization will be run
   */
  public void testLongSortOptimizationOnFieldNotIndexedWithPoints() throws IOException {
    final Directory dir = newDirectory();
    final IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());
    final int numDocs = atLeast(100);
    // my_field is not indexed with points
    for (int i = 0; i < numDocs; ++i) {
      final Document doc = new Document();
      doc.add(new NumericDocValuesField("my_field", i));
      writer.addDocument(doc);
    }
    final IndexReader reader = DirectoryReader.open(writer);
    writer.close();
    // single threaded so totalhits is deterministic
    IndexSearcher searcher =
        newSearcher(reader, random().nextBoolean(), random().nextBoolean(), false);
    final SortField sortField = new SortField("my_field", SortField.Type.LONG);
    final Sort sort = new Sort(sortField);
    final int numHits = 3;
    final int totalHitsThreshold = 3;

    CollectorManager<TopFieldCollector, TopFieldDocs> manager =
        TopFieldCollector.createSharedManager(sort, numHits, null, totalHitsThreshold);
    TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);
    assertEquals(
        topDocs.scoreDocs.length, numHits); // sort still works and returns expected number of docs
    for (int i = 0; i < numHits; i++) {
      FieldDoc fieldDoc = (FieldDoc) topDocs.scoreDocs[i];
      assertEquals(i, ((Long) fieldDoc.fields[0]).intValue()); // returns expected values
    }
    assertEquals(
        topDocs.totalHits.value,
        numDocs); // assert that all documents were collected => optimization was not run

    reader.close();
    dir.close();
  }

  public void testSortOptimizationWithMissingValues() throws IOException {
    final Directory dir = newDirectory();
    IndexWriterConfig config =
        new IndexWriterConfig()
            // Make sure to use the default codec, otherwise some random points formats that have
            // large values for maxPointsPerLeaf might not enable skipping with only 10k docs
            .setCodec(TestUtil.getDefaultCodec());
    final IndexWriter writer = new IndexWriter(dir, config);
    final int numDocs = atLeast(10000);
    for (int i = 0; i < numDocs; ++i) {
      final Document doc = new Document();
      if ((i % 500) != 0) { // miss values on every 500th document
        doc.add(new NumericDocValuesField("my_field", i));
        doc.add(new LongPoint("my_field", i));
      }
      writer.addDocument(doc);
      if (i == 7000) writer.flush(); // two segments
    }
    final IndexReader reader = DirectoryReader.open(writer);
    writer.close();
    // single threaded so totalhits is deterministic
    IndexSearcher searcher =
        newSearcher(reader, random().nextBoolean(), random().nextBoolean(), false);
    final int numHits = 3;
    final int totalHitsThreshold = 3;

    { // test that optimization is not run when missing value setting of SortField is competitive
      final SortField sortField = new SortField("my_field", SortField.Type.LONG);
      sortField.setMissingValue(0L); // set a competitive missing value
      final Sort sort = new Sort(sortField);
      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          TopFieldCollector.createSharedManager(sort, numHits, null, totalHitsThreshold);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);
      assertEquals(topDocs.scoreDocs.length, numHits);
      assertEquals(
          topDocs.totalHits.value,
          numDocs); // assert that all documents were collected => optimization was not run
    }
    { // test that optimization is run when missing value setting of SortField is NOT competitive
      final SortField sortField = new SortField("my_field", SortField.Type.LONG);
      sortField.setMissingValue(100L); // set a NON competitive missing value
      final Sort sort = new Sort(sortField);
      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          TopFieldCollector.createSharedManager(sort, numHits, null, totalHitsThreshold);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);
      assertEquals(topDocs.scoreDocs.length, numHits);
      assertNonCompetitiveHitsAreSkipped(topDocs.totalHits.value, numDocs);
    }

    { // test that optimization is not run when missing value setting of SortField is competitive
      // with after on asc order
      final long afterValue = Long.MAX_VALUE;
      final int afterDocID = 10 + random().nextInt(1000);
      FieldDoc after = new FieldDoc(afterDocID, Float.NaN, new Long[] {afterValue});
      final SortField sortField = new SortField("my_field", SortField.Type.LONG);
      sortField.setMissingValue(Long.MAX_VALUE); // set a competitive missing value
      final Sort sort = new Sort(sortField);
      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          TopFieldCollector.createSharedManager(sort, numHits, after, totalHitsThreshold);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);
      assertEquals(topDocs.scoreDocs.length, numHits);
      assertEquals(
          topDocs.totalHits.value,
          numDocs); // assert that all documents were collected => optimization was not run
    }

    { // test that optimization is not run when missing value setting of SortField is competitive
      // with after on desc order
      final long afterValue = Long.MAX_VALUE;
      final int afterDocID = 10 + random().nextInt(1000);
      FieldDoc after = new FieldDoc(afterDocID, Float.NaN, new Long[] {afterValue});
      final SortField sortField = new SortField("my_field", SortField.Type.LONG, true);
      sortField.setMissingValue(Long.MAX_VALUE); // set a competitive missing value
      final Sort sort = new Sort(sortField);
      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          TopFieldCollector.createSharedManager(sort, numHits, after, totalHitsThreshold);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);
      assertEquals(topDocs.scoreDocs.length, numHits);
      assertEquals(
          topDocs.totalHits.value,
          numDocs); // assert that all documents were collected => optimization was not run
    }

    reader.close();
    dir.close();
  }

  public void testSortOptimizationEqualValues() throws IOException {
    final Directory dir = newDirectory();
    IndexWriterConfig config =
        new IndexWriterConfig()
            // Make sure to use the default codec, otherwise some random points formats that have
            // large values for maxPointsPerLeaf might not enable skipping with only 10k docs
            .setCodec(TestUtil.getDefaultCodec());
    final IndexWriter writer = new IndexWriter(dir, config);
    final int numDocs = atLeast(TEST_NIGHTLY ? 50_000 : 10_000);
    for (int i = 1; i <= numDocs; ++i) {
      final Document doc = new Document();
      doc.add(
          new NumericDocValuesField("my_field1", 100)); // all docs have the same value of my_field1
      doc.add(new IntPoint("my_field1", 100));
      doc.add(
          new NumericDocValuesField(
              "my_field2", numDocs - i)); // diff values for the field my_field2
      writer.addDocument(doc);
      if (i == 7000) writer.flush(); // two segments
    }
    final IndexReader reader = DirectoryReader.open(writer);
    writer.close();
    // single threaded so totalhits is deterministic
    IndexSearcher searcher =
        newSearcher(reader, random().nextBoolean(), random().nextBoolean(), false);
    final int numHits = 3;
    final int totalHitsThreshold = 3;

    { // test that sorting on a single field with equal values uses the optimization
      final SortField sortField = new SortField("my_field1", SortField.Type.INT);
      final Sort sort = new Sort(sortField);
      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          TopFieldCollector.createSharedManager(sort, numHits, null, totalHitsThreshold);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);
      assertEquals(topDocs.scoreDocs.length, numHits);
      for (int i = 0; i < numHits; i++) {
        FieldDoc fieldDoc = (FieldDoc) topDocs.scoreDocs[i];
        assertEquals(100, fieldDoc.fields[0]);
      }
      assertNonCompetitiveHitsAreSkipped(topDocs.totalHits.value, numDocs);
    }

    { // test that sorting on a single field with equal values and after parameter
      // doesn't use the optimization
      final int afterValue = 100;
      final int afterDocID = 10 + random().nextInt(1000);
      final SortField sortField = new SortField("my_field1", SortField.Type.INT);
      final Sort sort = new Sort(sortField);
      FieldDoc after = new FieldDoc(afterDocID, Float.NaN, new Integer[] {afterValue});
      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          TopFieldCollector.createSharedManager(sort, numHits, after, totalHitsThreshold);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);
      assertEquals(topDocs.scoreDocs.length, numHits);
      for (int i = 0; i < numHits; i++) {
        FieldDoc fieldDoc = (FieldDoc) topDocs.scoreDocs[i];
        assertEquals(100, fieldDoc.fields[0]);
        assertTrue(fieldDoc.doc > afterDocID);
      }
      assertEquals(topDocs.totalHits.value, numDocs);
    }

    { // test that sorting on main field with equal values + another field for tie breaks doesn't
      // use optimization
      final SortField sortField1 = new SortField("my_field1", SortField.Type.INT);
      final SortField sortField2 = new SortField("my_field2", SortField.Type.INT);
      final Sort sort = new Sort(sortField1, sortField2);
      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          TopFieldCollector.createSharedManager(sort, numHits, null, totalHitsThreshold);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);
      assertEquals(topDocs.scoreDocs.length, numHits);
      for (int i = 0; i < numHits; i++) {
        FieldDoc fieldDoc = (FieldDoc) topDocs.scoreDocs[i];
        assertEquals(100, fieldDoc.fields[0]); // sort on 1st field as expected
        assertEquals(i, fieldDoc.fields[1]); // sort on 2nd field as expected
      }
      assertEquals(topDocs.scoreDocs.length, numHits);
      assertEquals(
          topDocs.totalHits.value,
          numDocs); // assert that all documents were collected => optimization was not run
    }

    reader.close();
    dir.close();
  }

  public void testFloatSortOptimization() throws IOException {
    final Directory dir = newDirectory();
    final IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());
    final int numDocs = atLeast(10000);
    for (int i = 0; i < numDocs; ++i) {
      final Document doc = new Document();
      float f = 1f * i;
      doc.add(new FloatDocValuesField("my_field", f));
      doc.add(new FloatPoint("my_field", i));
      writer.addDocument(doc);
    }
    final IndexReader reader = DirectoryReader.open(writer);
    writer.close();
    // single threaded so totalhits is deterministic
    IndexSearcher searcher =
        newSearcher(reader, random().nextBoolean(), random().nextBoolean(), false);
    final SortField sortField = new SortField("my_field", SortField.Type.FLOAT);
    final Sort sort = new Sort(sortField);
    final int numHits = 3;
    final int totalHitsThreshold = 3;

    { // simple sort
      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          TopFieldCollector.createSharedManager(sort, numHits, null, totalHitsThreshold);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);
      assertEquals(topDocs.scoreDocs.length, numHits);
      for (int i = 0; i < numHits; i++) {
        FieldDoc fieldDoc = (FieldDoc) topDocs.scoreDocs[i];
        assertEquals(1f * i, fieldDoc.fields[0]);
      }
      assertEquals(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO, topDocs.totalHits.relation);
      assertNonCompetitiveHitsAreSkipped(topDocs.totalHits.value, numDocs);
    }

    reader.close();
    dir.close();
  }

  /**
   * Test that a search with sort on [_doc, other fields] across multiple indices doesn't miss any
   * documents.
   */
  public void testDocSortOptimizationMultipleIndices() throws IOException {
    final int numIndices = 3;
    final int numDocsInIndex = atLeast(50);
    Directory[] dirs = new Directory[numIndices];
    IndexReader[] readers = new IndexReader[numIndices];
    for (int i = 0; i < numIndices; i++) {
      dirs[i] = newDirectory();
      IndexWriterConfig config =
          new IndexWriterConfig()
              // Make sure to use the default codec, otherwise some random points formats that have
              // large values for maxPointsPerLeaf might not enable skipping with only 10k docs
              .setCodec(TestUtil.getDefaultCodec());
      try (IndexWriter writer = new IndexWriter(dirs[i], config)) {
        for (int docID = 0; docID < numDocsInIndex; docID++) {
          final Document doc = new Document();
          doc.add(new NumericDocValuesField("my_field", docID * numIndices + i));
          writer.addDocument(doc);
        }
        writer.flush();
      }
      readers[i] = DirectoryReader.open(dirs[i]);
    }

    final int size = 7;
    final int totalHitsThreshold = 7;
    final Sort sort = new Sort(FIELD_DOC, new SortField("my_field", SortField.Type.LONG));
    TopFieldDocs[] topDocs = new TopFieldDocs[numIndices];
    int curNumHits;
    FieldDoc after = null;
    long collectedDocs = 0;
    long totalDocs = 0;
    int numHits = 0;
    do {
      for (int i = 0; i < numIndices; i++) {
        // single threaded so totalhits is deterministic
        IndexSearcher searcher =
            newSearcher(readers[i], random().nextBoolean(), random().nextBoolean(), false);
        CollectorManager<TopFieldCollector, TopFieldDocs> manager =
            TopFieldCollector.createSharedManager(sort, size, after, totalHitsThreshold);
        topDocs[i] = searcher.search(new MatchAllDocsQuery(), manager);
        for (int docID = 0; docID < topDocs[i].scoreDocs.length; docID++) {
          topDocs[i].scoreDocs[docID].shardIndex = i;
        }
        collectedDocs += topDocs[i].totalHits.value;
        totalDocs += numDocsInIndex;
      }
      TopFieldDocs mergedTopDcs = TopDocs.merge(sort, size, topDocs);
      curNumHits = mergedTopDcs.scoreDocs.length;
      numHits += curNumHits;
      if (curNumHits > 0) {
        after = (FieldDoc) mergedTopDcs.scoreDocs[curNumHits - 1];
      }
    } while (curNumHits > 0);

    for (int i = 0; i < numIndices; i++) {
      readers[i].close();
      dirs[i].close();
    }

    final int expectedNumHits = numDocsInIndex * numIndices;
    assertEquals(expectedNumHits, numHits);
    assertNonCompetitiveHitsAreSkipped(collectedDocs, totalDocs);
  }

  public void testDocSortOptimizationWithAfter() throws IOException {
    final Directory dir = newDirectory();
    final IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());
    final int numDocs = atLeast(150);
    for (int i = 0; i < numDocs; ++i) {
      final Document doc = new Document();
      writer.addDocument(doc);
      if ((i > 0) && (i % 50 == 0)) {
        writer.flush();
      }
    }

    final IndexReader reader = DirectoryReader.open(writer);
    writer.close();
    // single threaded so totalhits is deterministic
    IndexSearcher searcher =
        newSearcher(reader, random().nextBoolean(), random().nextBoolean(), false);
    final int numHits = 10;
    final int totalHitsThreshold = 10;
    final int[] searchAfters = {3, 10, numDocs - 10};
    for (int searchAfter : searchAfters) {
      // sort by _doc with search after should trigger optimization
      {
        final Sort sort = new Sort(FIELD_DOC);
        FieldDoc after = new FieldDoc(searchAfter, Float.NaN, new Integer[] {searchAfter});
        CollectorManager<TopFieldCollector, TopFieldDocs> manager =
            TopFieldCollector.createSharedManager(sort, numHits, after, totalHitsThreshold);
        TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);
        int expNumHits =
            (searchAfter >= (numDocs - numHits)) ? (numDocs - searchAfter - 1) : numHits;
        assertEquals(expNumHits, topDocs.scoreDocs.length);
        for (int i = 0; i < topDocs.scoreDocs.length; i++) {
          int expectedDocID = searchAfter + 1 + i;
          assertEquals(expectedDocID, topDocs.scoreDocs[i].doc);
        }
        assertEquals(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO, topDocs.totalHits.relation);
        assertNonCompetitiveHitsAreSkipped(topDocs.totalHits.value, numDocs);
      }

      // sort by _doc + _score with search after should trigger optimization
      {
        final Sort sort = new Sort(FIELD_DOC, FIELD_SCORE);
        FieldDoc after = new FieldDoc(searchAfter, Float.NaN, new Object[] {searchAfter, 1.0f});
        CollectorManager<TopFieldCollector, TopFieldDocs> manager =
            TopFieldCollector.createSharedManager(sort, numHits, after, totalHitsThreshold);
        TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);
        int expNumHits =
            (searchAfter >= (numDocs - numHits)) ? (numDocs - searchAfter - 1) : numHits;
        assertEquals(expNumHits, topDocs.scoreDocs.length);
        for (int i = 0; i < topDocs.scoreDocs.length; i++) {
          int expectedDocID = searchAfter + 1 + i;
          assertEquals(expectedDocID, topDocs.scoreDocs[i].doc);
        }
        assertEquals(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO, topDocs.totalHits.relation);
        assertNonCompetitiveHitsAreSkipped(topDocs.totalHits.value, numDocs);
      }

      // sort by _doc desc should not trigger optimization
      {
        final Sort sort = new Sort(new SortField(null, SortField.Type.DOC, true));
        FieldDoc after = new FieldDoc(searchAfter, Float.NaN, new Integer[] {searchAfter});
        CollectorManager<TopFieldCollector, TopFieldDocs> manager =
            TopFieldCollector.createSharedManager(sort, numHits, after, totalHitsThreshold);
        TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);
        int expNumHits = (searchAfter < numHits) ? searchAfter : numHits;
        assertEquals(expNumHits, topDocs.scoreDocs.length);
        for (int i = 0; i < topDocs.scoreDocs.length; i++) {
          int expectedDocID = searchAfter - 1 - i;
          assertEquals(expectedDocID, topDocs.scoreDocs[i].doc);
        }
        // assert that all documents were collected
        assertEquals(numDocs, topDocs.totalHits.value);
      }
    }

    reader.close();
    dir.close();
  }

  public void testDocSortOptimizationWithAfterCollectsAllDocs() throws IOException {
    final Directory dir = newDirectory();
    final IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());
    final int numDocs = atLeast(TEST_NIGHTLY ? 50_000 : 5_000);
    final boolean multipleSegments = random().nextBoolean();
    final int numDocsInSegment = numDocs / 10 + random().nextInt(numDocs / 10);

    for (int i = 1; i <= numDocs; ++i) {
      final Document doc = new Document();
      writer.addDocument(doc);
      if (multipleSegments && (i % numDocsInSegment == 0)) {
        writer.flush();
      }
    }
    writer.flush();

    IndexReader reader = DirectoryReader.open(writer);
    IndexSearcher searcher = newSearcher(reader);
    int visitedHits = 0;
    ScoreDoc after = null;
    while (visitedHits < numDocs) {
      int batch = 1 + random().nextInt(500);
      Query query = new MatchAllDocsQuery();
      TopDocs topDocs = searcher.searchAfter(after, query, batch, new Sort(FIELD_DOC));
      int expectedHits = Math.min(numDocs - visitedHits, batch);
      assertEquals(expectedHits, topDocs.scoreDocs.length);
      after = topDocs.scoreDocs[expectedHits - 1];
      for (int i = 0; i < topDocs.scoreDocs.length; i++) {
        assertEquals(visitedHits, topDocs.scoreDocs[i].doc);
        visitedHits++;
      }
    }
    assertEquals(visitedHits, numDocs);
    IOUtils.close(writer, reader, dir);
  }

  public void testDocSortOptimization() throws IOException {
    final Directory dir = newDirectory();
    final IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());
    final int numDocs = atLeast(100);
    int seg = 1;
    for (int i = 0; i < numDocs; ++i) {
      final Document doc = new Document();
      doc.add(new LongPoint("lf", i));
      doc.add(new StoredField("slf", i));
      doc.add(new StringField("tf", "seg" + seg, Field.Store.YES));
      writer.addDocument(doc);
      if ((i > 0) && (i % 50 == 0)) {
        writer.flush();
        seg++;
      }
    }
    final IndexReader reader = DirectoryReader.open(writer);
    writer.close();
    // single threaded so totalhits is deterministic
    IndexSearcher searcher =
        newSearcher(reader, random().nextBoolean(), random().nextBoolean(), false);

    final int numHits = 3;
    final int totalHitsThreshold = 3;
    final Sort sort = new Sort(FIELD_DOC);

    // sort by _doc should skip all non-competitive documents
    {
      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          TopFieldCollector.createSharedManager(sort, numHits, null, totalHitsThreshold);
      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);
      assertEquals(numHits, topDocs.scoreDocs.length);
      for (int i = 0; i < numHits; i++) {
        assertEquals(i, topDocs.scoreDocs[i].doc);
      }
      assertEquals(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO, topDocs.totalHits.relation);
      assertNonCompetitiveHitsAreSkipped(topDocs.totalHits.value, 10);
    }

    // sort by _doc with a bool query should skip all non-competitive documents
    {
      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          TopFieldCollector.createSharedManager(sort, numHits, null, totalHitsThreshold);
      int lowerRange = 40;
      BooleanQuery.Builder bq = new BooleanQuery.Builder();
      bq.add(LongPoint.newRangeQuery("lf", lowerRange, Long.MAX_VALUE), BooleanClause.Occur.MUST);
      bq.add(new TermQuery(new Term("tf", "seg1")), BooleanClause.Occur.MUST);
      TopDocs topDocs = searcher.search(bq.build(), manager);

      assertEquals(numHits, topDocs.scoreDocs.length);
      StoredFields storedFields = searcher.storedFields();
      for (int i = 0; i < numHits; i++) {
        Document d = storedFields.document(topDocs.scoreDocs[i].doc);
        assertEquals(Integer.toString(i + lowerRange), d.get("slf"));
        assertEquals("seg1", d.get("tf"));
      }
      assertEquals(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO, topDocs.totalHits.relation);
      assertNonCompetitiveHitsAreSkipped(topDocs.totalHits.value, 10);
    }

    reader.close();
    dir.close();
  }

  /**
   * Test that sorting on _doc works correctly. This test goes through
   * DefaultBulkSorter::scoreRange, where scorerIterator is BitSetIterator. As a conjunction of this
   * BitSetIterator with DocComparator's iterator, we get BitSetConjunctionDISI.
   * BitSetConjuctionDISI advances based on the DocComparator's iterator, and doesn't consider that
   * its BitSetIterator may have advanced passed a certain doc.
   */
  public void testDocSort() throws IOException {
    final Directory dir = newDirectory();
    final IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());
    final int numDocs = 4;
    for (int i = 0; i < numDocs; ++i) {
      final Document doc = new Document();
      doc.add(new StringField("id", "id" + i, Field.Store.NO));
      if (i < 2) {
        doc.add(new LongPoint("lf", 1));
      }
      writer.addDocument(doc);
    }
    final IndexReader reader = DirectoryReader.open(writer);
    writer.close();

    IndexSearcher searcher = newSearcher(reader, random().nextBoolean(), random().nextBoolean());
    searcher.setQueryCache(null);
    final int numHits = 10;
    final int totalHitsThreshold = 10;
    final Sort sort = new Sort(FIELD_DOC);

    {
      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          TopFieldCollector.createSharedManager(sort, numHits, null, totalHitsThreshold);
      BooleanQuery.Builder bq = new BooleanQuery.Builder();
      bq.add(LongPoint.newExactQuery("lf", 1), BooleanClause.Occur.MUST);
      bq.add(new TermQuery(new Term("id", "id3")), BooleanClause.Occur.MUST_NOT);
      TopDocs topDocs = searcher.search(bq.build(), manager);
      assertEquals(2, topDocs.scoreDocs.length);
    }

    reader.close();
    dir.close();
  }

  public void testPointValidation() throws IOException {
    final Directory dir = newDirectory();
    final RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();

    doc.add(new IntPoint("intField", 4));
    doc.add(new NumericDocValuesField("intField", 4));

    doc.add(new LongPoint("longField", 42));
    doc.add(new NumericDocValuesField("longField", 42));

    doc.add(new IntRange("intRange", new int[] {1}, new int[] {10}));
    doc.add(new NumericDocValuesField("intRange", 4));

    writer.addDocument(doc);
    IndexReader reader = writer.getReader();
    writer.close();

    IndexSearcher searcher = newSearcher(reader, random().nextBoolean(), random().nextBoolean());

    SortField longSortOnIntField = new SortField("intField", SortField.Type.LONG);
    assertThrows(
        IllegalArgumentException.class,
        () -> searcher.search(new MatchAllDocsQuery(), 1, new Sort(longSortOnIntField)));
    // assert that when sort optimization is disabled we can use LONG sort on int field
    longSortOnIntField.setOptimizeSortWithIndexedData(false);
    searcher.search(new MatchAllDocsQuery(), 1, new Sort(longSortOnIntField));

    SortField intSortOnLongField = new SortField("longField", SortField.Type.INT);
    assertThrows(
        IllegalArgumentException.class,
        () -> searcher.search(new MatchAllDocsQuery(), 1, new Sort(intSortOnLongField)));
    // assert that when sort optimization is disabled we can use INT sort on long field
    intSortOnLongField.setOptimizeSortWithIndexedData(false);
    searcher.search(new MatchAllDocsQuery(), 1, new Sort(intSortOnLongField));

    SortField intSortOnIntRangeField = new SortField("intRange", SortField.Type.INT);
    assertThrows(
        IllegalArgumentException.class,
        () -> searcher.search(new MatchAllDocsQuery(), 1, new Sort(intSortOnIntRangeField)));
    // assert that when sort optimization is disabled we can use INT sort on intRange field
    intSortOnIntRangeField.setOptimizeSortWithIndexedData(false);
    searcher.search(new MatchAllDocsQuery(), 1, new Sort(intSortOnIntRangeField));

    reader.close();
    dir.close();
  }

  public void testMaxDocVisited() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());
    int numDocs = atLeast(10000);
    long offset = 100 + random().nextInt(100);
    long smallestValue = 50 + random().nextInt(50);
    boolean flushed = false;
    for (int i = 0; i < numDocs; ++i) {
      Document doc = new Document();
      doc.add(new NumericDocValuesField("my_field", i + offset));
      doc.add(new LongPoint("my_field", i + offset));
      writer.addDocument(doc);
      if (i >= 5000 && flushed == false) {
        flushed = true;
        writer.flush();
        // Index the smallest value to the first slot of the second segment
        doc = new Document();
        doc.add(new NumericDocValuesField("my_field", smallestValue));
        doc.add(new LongPoint("my_field", smallestValue));
        writer.addDocument(doc);
      }
    }
    IndexReader reader = DirectoryReader.open(writer);
    writer.close();
    IndexSearcher searcher = newSearcher(reader, random().nextBoolean(), random().nextBoolean());
    SortField sortField = new SortField("my_field", SortField.Type.LONG);
    TopFieldDocs topDocs =
        searcher.search(new MatchAllDocsQuery(), 1 + random().nextInt(100), new Sort(sortField));
    FieldDoc fieldDoc = (FieldDoc) topDocs.scoreDocs[0];
    assertEquals(smallestValue, ((Long) fieldDoc.fields[0]).intValue());
    reader.close();
    dir.close();
  }

  public void testRandomLong() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());
    List<Long> seqNos = new ArrayList<>();
    int limit = TEST_NIGHTLY ? 10000 : 1000;
    int iterations = limit + random().nextInt(limit);
    long seqNoGenerator = random().nextInt(1000);
    for (long i = 0; i < iterations; i++) {
      int copies = random().nextInt(100) <= 5 ? 1 : 1 + random().nextInt(5);
      for (int j = 0; j < copies; j++) {
        seqNos.add(seqNoGenerator);
      }
      seqNos.add(seqNoGenerator);
      seqNoGenerator++;
      if (random().nextInt(100) <= 5) {
        seqNoGenerator += random().nextInt(10);
      }
    }

    Collections.shuffle(seqNos, random());
    int pendingDocs = 0;
    for (long seqNo : seqNos) {
      Document doc = new Document();
      doc.add(new NumericDocValuesField("seq_no", seqNo));
      doc.add(new LongPoint("seq_no", seqNo));
      writer.addDocument(doc);
      pendingDocs++;
      if (pendingDocs > 500 && random().nextInt(100) <= 5) {
        pendingDocs = 0;
        writer.flush();
      }
    }
    writer.flush();
    seqNos.sort(Long::compare);
    IndexReader reader = DirectoryReader.open(writer);
    writer.close();
    IndexSearcher searcher = newSearcher(reader, random().nextBoolean(), random().nextBoolean());
    SortField sortField = new SortField("seq_no", SortField.Type.LONG);
    int visitedHits = 0;
    ScoreDoc after = null;
    while (visitedHits < seqNos.size()) {
      int batch = 1 + random().nextInt(100);
      Query query =
          random().nextBoolean()
              ? new MatchAllDocsQuery()
              : LongPoint.newRangeQuery("seq_no", 0, Long.MAX_VALUE);
      TopDocs topDocs = searcher.searchAfter(after, query, batch, new Sort(sortField));
      int expectedHits = Math.min(seqNos.size() - visitedHits, batch);
      assertEquals(expectedHits, topDocs.scoreDocs.length);
      after = topDocs.scoreDocs[expectedHits - 1];
      for (int i = 0; i < topDocs.scoreDocs.length; i++) {
        FieldDoc fieldDoc = (FieldDoc) topDocs.scoreDocs[i];
        long expectedSeqNo = seqNos.get(visitedHits);
        assertEquals(expectedSeqNo, ((Long) fieldDoc.fields[0]).intValue());
        visitedHits++;
      }
    }
    reader.close();
    dir.close();
  }

  // Test that sort on sorted numeric field without sort optimization and
  // with sort optimization produce the same results
  public void testSortOptimizationOnSortedNumericField() throws IOException {
    final Directory dir = newDirectory();
    final IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());
    final int numDocs = atLeast(5000);
    for (int i = 0; i < numDocs; ++i) {
      int value = random().nextInt();
      int value2 = random().nextInt();
      final Document doc = new Document();
      doc.add(new LongField("my_field", value, Field.Store.NO));
      doc.add(new LongField("my_field", value2, Field.Store.NO));
      writer.addDocument(doc);
    }
    final IndexReader reader = DirectoryReader.open(writer);
    writer.close();
    // single threaded so totalhits is deterministic
    IndexSearcher searcher = newSearcher(reader, true, true, false);

    SortedNumericSelector.Type type =
        RandomPicks.randomFrom(random(), SortedNumericSelector.Type.values());
    boolean reverse = random().nextBoolean();
    final SortField sortField = LongField.newSortField("my_field", reverse, type);
    sortField.setOptimizeSortWithIndexedData(false);
    final Sort sort = new Sort(sortField); // sort without sort optimization
    final SortField sortField2 = LongField.newSortField("my_field", reverse, type);
    final Sort sort2 = new Sort(sortField2); // sort with sort optimization
    Query query = new MatchAllDocsQuery();
    final int totalHitsThreshold = 3;

    long expectedCollectedHits = 0;
    long collectedHits = 0;
    long collectedHits2 = 0;
    int visitedHits = 0;
    ScoreDoc after = null;
    while (visitedHits < numDocs) {
      int batch = 1 + random().nextInt(100);
      int expectedHits = Math.min(numDocs - visitedHits, batch);

      CollectorManager<TopFieldCollector, TopFieldDocs> manager =
          TopFieldCollector.createSharedManager(sort, batch, (FieldDoc) after, totalHitsThreshold);
      TopDocs topDocs = searcher.search(query, manager);
      ScoreDoc[] scoreDocs = topDocs.scoreDocs;

      CollectorManager<TopFieldCollector, TopFieldDocs> manager2 =
          TopFieldCollector.createSharedManager(sort2, batch, (FieldDoc) after, totalHitsThreshold);
      TopDocs topDocs2 = searcher.search(query, manager2);
      ScoreDoc[] scoreDocs2 = topDocs2.scoreDocs;

      // assert that the resulting hits are the same
      assertEquals(expectedHits, topDocs.scoreDocs.length);
      assertEquals(topDocs.scoreDocs.length, topDocs2.scoreDocs.length);
      for (int i = 0; i < scoreDocs.length; i++) {
        FieldDoc fieldDoc = (FieldDoc) scoreDocs[i];
        FieldDoc fieldDoc2 = (FieldDoc) scoreDocs2[i];
        assertEquals(fieldDoc.fields[0], fieldDoc2.fields[0]);
        assertEquals(fieldDoc.doc, fieldDoc2.doc);
        visitedHits++;
      }

      expectedCollectedHits += numDocs;
      collectedHits += topDocs.totalHits.value;
      collectedHits2 += topDocs2.totalHits.value;
      after = scoreDocs[expectedHits - 1];
    }
    assertEquals(visitedHits, numDocs);
    assertEquals(expectedCollectedHits, collectedHits);
    // assert that the second sort with optimization collected less or equal hits
    assertTrue(collectedHits >= collectedHits2);
    // System.out.println(expectedCollectedHits + "\t" + collectedHits + "\t" + collectedHits2);

    reader.close();
    dir.close();
  }

  private void assertNonCompetitiveHitsAreSkipped(long collectedHits, long numDocs) {
    if (collectedHits >= numDocs) {
      fail(
          "Expected some non-competitive hits are skipped; got collected_hits="
              + collectedHits
              + " num_docs="
              + numDocs);
    }
  }

  public void testStringSortOptimization() throws IOException {
    final Directory dir = newDirectory();
    final IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());
    final int numDocs = atLeast(10000);
    for (int i = 0; i < numDocs; ++i) {
      final Document doc = new Document();
      final BytesRef value = new BytesRef(Integer.toString(random().nextInt(1000)));
      doc.add(new KeywordField("my_field", value, Field.Store.NO));
      writer.addDocument(doc);
      if (i % 2000 == 0) writer.flush(); // multiple segments
    }
    final DirectoryReader reader = DirectoryReader.open(writer);
    writer.close();
    doTestStringSortOptimization(reader);
    doTestStringSortOptimizationDisabled(reader);
    reader.close();
    dir.close();
  }

  public void testStringSortOptimizationWithMissingValues() throws IOException {
    final Directory dir = newDirectory();
    final IndexWriter writer =
        new IndexWriter(dir, new IndexWriterConfig().setMergePolicy(newLogMergePolicy()));
    final int numDocs = atLeast(10000);
    // one segment with all values missing to start with
    writer.addDocument(new Document());
    for (int i = 0; i < numDocs - 2; ++i) {
      if (i % 2000 == 0) writer.flush(); // multiple segments
      final Document doc = new Document();
      if (random().nextInt(2) == 0) {
        final BytesRef value = new BytesRef(Integer.toString(random().nextInt(1000)));
        doc.add(new KeywordField("my_field", value, Field.Store.NO));
      }
      writer.addDocument(doc);
    }
    writer.flush();
    // And one empty segment with all values missing to finish with
    writer.addDocument(new Document());
    final DirectoryReader reader = DirectoryReader.open(writer);
    writer.close();
    doTestStringSortOptimization(reader);
    reader.close();
    dir.close();
  }

  private void doTestStringSortOptimization(DirectoryReader reader) throws IOException {
    final int numDocs = reader.numDocs();
    final int numHits = 5;

    { // simple ascending sort
      SortField sortField =
          KeywordField.newSortField("my_field", false, SortedSetSelector.Type.MIN);
      sortField.setMissingValue(SortField.STRING_LAST);
      Sort sort = new Sort(sortField);
      TopDocs topDocs = assertSort(reader, sort, numHits, null);
      assertNonCompetitiveHitsAreSkipped(topDocs.totalHits.value, numDocs);
    }

    { // simple descending sort
      SortField sortField = KeywordField.newSortField("my_field", true, SortedSetSelector.Type.MIN);
      sortField.setMissingValue(SortField.STRING_FIRST);
      Sort sort = new Sort(sortField);
      TopDocs topDocs = assertSort(reader, sort, numHits, null);
      assertNonCompetitiveHitsAreSkipped(topDocs.totalHits.value, numDocs);
    }

    { // ascending sort that returns missing values first
      SortField sortField =
          KeywordField.newSortField("my_field", false, SortedSetSelector.Type.MIN);
      sortField.setMissingValue(SortField.STRING_FIRST);
      Sort sort = new Sort(sortField);
      assertSort(reader, sort, numHits, null);
    }

    { // descending sort that returns missing values last
      SortField sortField = KeywordField.newSortField("my_field", true, SortedSetSelector.Type.MIN);
      sortField.setMissingValue(SortField.STRING_LAST);
      Sort sort = new Sort(sortField);
      assertSort(reader, sort, numHits, null);
    }

    { // paging ascending sort with after
      SortField sortField =
          KeywordField.newSortField("my_field", false, SortedSetSelector.Type.MIN);
      sortField.setMissingValue(SortField.STRING_LAST);
      Sort sort = new Sort(sortField);
      BytesRef afterValue = new BytesRef(random().nextBoolean() ? "23" : "230000000");
      FieldDoc after = new FieldDoc(2, Float.NaN, new Object[] {afterValue});
      TopDocs topDocs = assertSort(reader, sort, numHits, after);
      assertNonCompetitiveHitsAreSkipped(topDocs.totalHits.value, numDocs);
    }

    { // paging descending sort with after
      SortField sortField = KeywordField.newSortField("my_field", true, SortedSetSelector.Type.MIN);
      sortField.setMissingValue(SortField.STRING_FIRST);
      Sort sort = new Sort(sortField);
      BytesRef afterValue = new BytesRef(random().nextBoolean() ? "17" : "170000000");
      FieldDoc after = new FieldDoc(2, Float.NaN, new Object[] {afterValue});
      TopDocs topDocs = assertSort(reader, sort, numHits, after);
      assertNonCompetitiveHitsAreSkipped(topDocs.totalHits.value, numDocs);
    }

    { // paging ascending sort with after that returns missing values first
      SortField sortField =
          KeywordField.newSortField("my_field", false, SortedSetSelector.Type.MIN);
      sortField.setMissingValue(SortField.STRING_FIRST);
      Sort sort = new Sort(sortField);
      BytesRef afterValue = new BytesRef(random().nextBoolean() ? "23" : "230000000");
      FieldDoc after = new FieldDoc(2, Float.NaN, new Object[] {afterValue});
      TopDocs topDocs = assertSort(reader, sort, numHits, after);
      assertNonCompetitiveHitsAreSkipped(topDocs.totalHits.value, numDocs);
    }

    { // paging descending sort with after that returns missing values first
      SortField sortField = KeywordField.newSortField("my_field", true, SortedSetSelector.Type.MIN);
      sortField.setMissingValue(SortField.STRING_LAST);
      Sort sort = new Sort(sortField);
      BytesRef afterValue = new BytesRef(random().nextBoolean() ? "17" : "170000000");
      FieldDoc after = new FieldDoc(2, Float.NaN, new Object[] {afterValue});
      TopDocs topDocs = assertSort(reader, sort, numHits, after);
      assertNonCompetitiveHitsAreSkipped(topDocs.totalHits.value, numDocs);
    }

    { // test that if there is the secondary sort on _score, hits are still skipped
      SortField sortField =
          KeywordField.newSortField("my_field", false, SortedSetSelector.Type.MIN);
      sortField.setMissingValue(SortField.STRING_LAST);
      Sort sort = new Sort(sortField, FIELD_SCORE);
      TopDocs topDocs = assertSort(reader, sort, numHits, null);
      assertNonCompetitiveHitsAreSkipped(topDocs.totalHits.value, numDocs);
    }

    { // test that if string field is a secondary sort, no optimization is run
      SortField sortField =
          KeywordField.newSortField("my_field", false, SortedSetSelector.Type.MIN);
      sortField.setMissingValue(SortField.STRING_LAST);
      Sort sort = new Sort(FIELD_SCORE, sortField);
      TopDocs topDocs = assertSort(reader, sort, numHits, null);
      assertEquals(
          topDocs.totalHits.value,
          numDocs); // assert that all documents were collected => optimization was not run
    }
  }

  public void doTestStringSortOptimizationDisabled(DirectoryReader reader) throws IOException {
    SortField sortField = KeywordField.newSortField("my_field", false, SortedSetSelector.Type.MIN);
    sortField.setMissingValue(SortField.STRING_LAST);
    sortField.setOptimizeSortWithIndexedData(false);

    Sort sort = new Sort(sortField);
    final int numDocs = reader.numDocs();
    final int numHits = 5;
    final int totalHitsThreshold = 5;

    CollectorManager<TopFieldCollector, TopFieldDocs> manager =
        TopFieldCollector.createSharedManager(sort, numHits, null, totalHitsThreshold);
    IndexSearcher searcher =
        newSearcher(reader, random().nextBoolean(), random().nextBoolean(), false);
    TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), manager);
    assertEquals(numDocs, topDocs.totalHits.value);
  }

  private TopDocs assertSort(DirectoryReader reader, Sort sort, int n, FieldDoc after)
      throws IOException {
    TopDocs topDocs = assertSearchHits(reader, sort, n, after);
    SortField[] sortField2 = ArrayUtil.growExact(sort.getSort(), sort.getSort().length + 1);
    // A secondary sort on reverse doc ID is the best way to catch bugs if the comparator filters
    // too aggressively
    sortField2[sortField2.length - 1] = new SortField(null, Type.DOC, true);
    FieldDoc after2 = null;
    if (after != null) {
      Object[] afterFields2 = ArrayUtil.growExact(after.fields, after.fields.length + 1);
      afterFields2[afterFields2.length - 1] = Integer.MAX_VALUE;
      after2 = new FieldDoc(after.doc, after.score, afterFields2);
    }
    assertSearchHits(reader, new Sort(sortField2), n, after2);
    return topDocs;
  }

  private TopDocs assertSearchHits(DirectoryReader reader, Sort sort, int n, FieldDoc after)
      throws IOException {
    // single threaded so totalhits is deterministic
    IndexSearcher searcher = newSearcher(reader, true, true, false);
    Query query = new MatchAllDocsQuery();
    CollectorManager<TopFieldCollector, TopFieldDocs> manager =
        TopFieldCollector.createSharedManager(sort, n, after, n);
    TopDocs topDocs = searcher.search(query, manager);
    IndexSearcher unoptimizedSearcher =
        newSearcher(new NoIndexDirectoryReader(reader), true, true, false);
    TopDocs unoptimizedTopDocs = unoptimizedSearcher.search(query, manager);
    CheckHits.checkEqual(query, unoptimizedTopDocs.scoreDocs, topDocs.scoreDocs);
    return topDocs;
  }

  private static final class NoIndexDirectoryReader extends FilterDirectoryReader {

    public NoIndexDirectoryReader(DirectoryReader in) throws IOException {
      super(
          in,
          new SubReaderWrapper() {
            @Override
            public LeafReader wrap(LeafReader reader) {
              return new NoIndexLeafReader(reader);
            }
          });
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
      return null;
    }
  }

  private static final class NoIndexLeafReader extends FilterLeafReader {

    NoIndexLeafReader(LeafReader in) {
      super(in);
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
      return null;
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
      return null;
    }

    @Override
    public Terms terms(String field) throws IOException {
      return null;
    }

    @Override
    public PointValues getPointValues(String field) throws IOException {
      return null;
    }

    @Override
    public FieldInfos getFieldInfos() {
      FieldInfo[] newInfos = new FieldInfo[super.getFieldInfos().size()];
      int i = 0;
      for (FieldInfo fi : super.getFieldInfos()) {
        FieldInfo noIndexFI =
            new FieldInfo(
                fi.name,
                fi.number,
                false,
                false,
                false,
                IndexOptions.NONE,
                fi.getDocValuesType(),
                fi.getDocValuesGen(),
                fi.attributes(),
                0,
                0,
                0,
                0,
                VectorEncoding.FLOAT32,
                VectorSimilarityFunction.DOT_PRODUCT,
                fi.isSoftDeletesField());
        newInfos[i] = noIndexFI;
        i++;
      }
      return new FieldInfos(newInfos);
    }
  }
}
