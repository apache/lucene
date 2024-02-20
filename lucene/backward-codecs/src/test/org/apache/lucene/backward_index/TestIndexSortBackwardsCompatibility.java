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
package org.apache.lucene.backward_index;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Random;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.LineFileDocs;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;

public class TestIndexSortBackwardsCompatibility extends BackwardsCompatibilityTestBase {

  static final String INDEX_NAME = "sorted";
  static final String SUFFIX = "";
  private static final Version FIRST_PARENT_DOC_VERSION = Version.LUCENE_9_11_0;
  private static final String PARENT_FIELD_NAME = "___parent";

  public TestIndexSortBackwardsCompatibility(Version version, String pattern) {
    super(version, pattern);
  }

  /** Provides all sorted versions to the test-framework */
  @ParametersFactory(argumentFormatting = "Lucene-Version:%1$s; Pattern: %2$s")
  public static Iterable<Object[]> testVersionsFactory() throws IllegalAccessException {
    return allVersion(INDEX_NAME, SUFFIX);
  }

  public void testSortedIndexAddDocBlocks() throws Exception {
    final Sort sort;
    try (DirectoryReader reader = DirectoryReader.open(directory)) {
      assertEquals(1, reader.leaves().size());
      sort = reader.leaves().get(0).reader().getMetaData().getSort();
      assertNotNull(sort);
      searchExampleIndex(reader);
    }
    IndexWriterConfig indexWriterConfig =
        newIndexWriterConfig(new MockAnalyzer(random()))
            .setOpenMode(IndexWriterConfig.OpenMode.APPEND)
            .setIndexSort(sort)
            .setMergePolicy(newLogMergePolicy());
    if (this.version.onOrAfter(FIRST_PARENT_DOC_VERSION)) {
      indexWriterConfig.setParentField(PARENT_FIELD_NAME);
    }
    // open writer
    try (IndexWriter writer = new IndexWriter(directory, indexWriterConfig)) {
      // add 10 docs
      for (int i = 0; i < 10; i++) {
        Document child = new Document();
        child.add(new StringField("relation", "child", Field.Store.NO));
        child.add(new StringField("bid", "" + i, Field.Store.NO));
        if (version.onOrAfter(FIRST_PARENT_DOC_VERSION)
            == false) { // only add this to earlier versions
          child.add(new NumericDocValuesField("dateDV", i));
        }
        Document parent = new Document();
        parent.add(new StringField("relation", "parent", Field.Store.NO));
        parent.add(new StringField("bid", "" + i, Field.Store.NO));
        parent.add(new NumericDocValuesField("dateDV", i));
        writer.addDocuments(Arrays.asList(child, child, parent));
        if (random().nextBoolean()) {
          writer.flush();
        }
      }
      if (random().nextBoolean()) {
        writer.forceMerge(1);
      }
      writer.commit();
      try (IndexReader reader = DirectoryReader.open(directory)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        for (int i = 0; i < 10; i++) {
          TopDocs children =
              searcher.search(
                  new BooleanQuery.Builder()
                      .add(new TermQuery(new Term("relation", "child")), BooleanClause.Occur.MUST)
                      .add(new TermQuery(new Term("bid", "" + i)), BooleanClause.Occur.MUST)
                      .build(),
                  2);
          TopDocs parents =
              searcher.search(
                  new BooleanQuery.Builder()
                      .add(new TermQuery(new Term("relation", "parent")), BooleanClause.Occur.MUST)
                      .add(new TermQuery(new Term("bid", "" + i)), BooleanClause.Occur.MUST)
                      .build(),
                  2);
          assertEquals(2, children.totalHits.value);
          assertEquals(1, parents.totalHits.value);
          // make sure it's sorted
          assertEquals(children.scoreDocs[0].doc + 1, children.scoreDocs[1].doc);
          assertEquals(children.scoreDocs[1].doc + 1, parents.scoreDocs[0].doc);
        }
      }
    }
    // This will confirm the docs are really sorted
    TestUtil.checkIndex(directory);
  }

  public void testSortedIndex() throws Exception {
    try (DirectoryReader reader = DirectoryReader.open(directory)) {
      assertEquals(1, reader.leaves().size());
      Sort sort = reader.leaves().get(0).reader().getMetaData().getSort();
      assertNotNull(sort);
      assertEquals("<long: \"dateDV\">!", sort.toString());
      // This will confirm the docs are really sorted
      TestUtil.checkIndex(directory);
      searchExampleIndex(reader);
    }
  }

  @Override
  protected void createIndex(Directory directory) throws IOException {
    LogByteSizeMergePolicy mp = new LogByteSizeMergePolicy();
    mp.setNoCFSRatio(1.0);
    mp.setMaxCFSSegmentSizeMB(Double.POSITIVE_INFINITY);
    MockAnalyzer analyzer = new MockAnalyzer(random());
    analyzer.setMaxTokenLength(TestUtil.nextInt(random(), 1, IndexWriter.MAX_TERM_LENGTH));

    // TODO: remove randomness
    IndexWriterConfig conf = new IndexWriterConfig(analyzer);
    conf.setMergePolicy(mp);
    conf.setUseCompoundFile(false);
    conf.setCodec(TestUtil.getDefaultCodec());
    conf.setParentField(PARENT_FIELD_NAME);
    conf.setIndexSort(new Sort(new SortField("dateDV", SortField.Type.LONG, true)));
    IndexWriter writer = new IndexWriter(directory, conf);
    LineFileDocs docs = new LineFileDocs(new Random(0));

    for (int i = 0; i < 50; i++) {
      Document doc = TestUtil.cloneDocument(docs.nextDoc());
      String dateString = doc.get("date");
      LocalDateTime date = LineFileDocs.DATE_FIELD_VALUE_TO_LOCALDATETIME.apply(dateString);
      doc.add(
          new NumericDocValuesField(
              "docid_intDV", doc.getField("docid_int").numericValue().longValue()));
      doc.add(
          new SortedDocValuesField("titleDV", new BytesRef(doc.getField("title").stringValue())));
      doc.add(new NumericDocValuesField("dateDV", date.toInstant(ZoneOffset.UTC).toEpochMilli()));
      if (i % 10 == 0) { // commit every 10 documents
        writer.commit();
      }
      writer.addDocument(doc);
    }
    writer.forceMerge(1);
    writer.close();

    try (DirectoryReader reader = DirectoryReader.open(directory)) {
      searchExampleIndex(reader); // make sure we can search it
    }
  }

  public static void searchExampleIndex(DirectoryReader reader) throws IOException {
    IndexSearcher searcher = newSearcher(reader);

    TopDocs topDocs = searcher.search(new FieldExistsQuery("titleTokenized"), 10);
    assertEquals(50, topDocs.totalHits.value);

    topDocs = searcher.search(new FieldExistsQuery("titleDV"), 10);
    assertEquals(50, topDocs.totalHits.value);

    topDocs =
        searcher.search(
            IntPoint.newRangeQuery("docid_int", 42, 44),
            10,
            new Sort(new SortField("docid_intDV", SortField.Type.INT)));
    assertEquals(3, topDocs.totalHits.value);
    assertEquals(3, topDocs.scoreDocs.length);
    assertEquals(42, ((FieldDoc) topDocs.scoreDocs[0]).fields[0]);
    assertEquals(43, ((FieldDoc) topDocs.scoreDocs[1]).fields[0]);
    assertEquals(44, ((FieldDoc) topDocs.scoreDocs[2]).fields[0]);

    topDocs = searcher.search(new TermQuery(new Term("body", "the")), 5);
    assertTrue(topDocs.totalHits.value > 0);
    topDocs =
        searcher.search(
            new MatchAllDocsQuery(), 5, new Sort(new SortField("dateDV", SortField.Type.LONG)));
    assertEquals(50, topDocs.totalHits.value);
    assertEquals(5, topDocs.scoreDocs.length);
    long firstDate = (Long) ((FieldDoc) topDocs.scoreDocs[0]).fields[0];
    long lastDate = (Long) ((FieldDoc) topDocs.scoreDocs[4]).fields[0];
    assertTrue(firstDate <= lastDate);
  }
}
