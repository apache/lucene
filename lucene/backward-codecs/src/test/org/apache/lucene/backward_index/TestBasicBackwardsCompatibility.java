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

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.apache.lucene.util.Version.LUCENE_9_0_0;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.BinaryPoint;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.MultiBits;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StandardDirectoryReader;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermVectors;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;

public class TestBasicBackwardsCompatibility extends BackwardsCompatibilityTestBase {

  static final String INDEX_NAME = "index";
  static final String SUFFIX_CFS = "-cfs";
  static final String SUFFIX_NO_CFS = "-nocfs";

  private static final int DOCS_COUNT = 35;
  private static final int DELETED_ID = 7;

  private static final int KNN_VECTOR_MIN_SUPPORTED_VERSION = LUCENE_9_0_0.major;
  private static final String KNN_VECTOR_FIELD = "knn_field";
  private static final FieldType KNN_VECTOR_FIELD_TYPE =
      KnnFloatVectorField.createFieldType(3, VectorSimilarityFunction.COSINE);
  private static final float[] KNN_VECTOR = {0.2f, -0.1f, 0.1f};

  /**
   * A parameter constructor for {@link com.carrotsearch.randomizedtesting.RandomizedRunner}. See
   * {@link #testVersionsFactory()} for details on the values provided to the framework.
   */
  public TestBasicBackwardsCompatibility(Version version, String pattern) {
    super(version, pattern);
  }

  /** Provides all current version to the test-framework for each of the index suffixes. */
  @ParametersFactory(argumentFormatting = "Lucene-Version:%1$s; Pattern: %2$s")
  public static Iterable<Object[]> testVersionsFactory() throws IllegalAccessException {
    return allVersion(INDEX_NAME, SUFFIX_CFS, SUFFIX_NO_CFS);
  }

  @Override
  protected void createIndex(Directory directory) throws IOException {
    if (indexPattern.equals(createPattern(INDEX_NAME, SUFFIX_CFS))) {
      createIndex(directory, true, false);
    } else {
      createIndex(directory, false, false);
    }
  }

  static void createIndex(Directory dir, boolean doCFS, boolean fullyMerged) throws IOException {
    LogByteSizeMergePolicy mp = new LogByteSizeMergePolicy();
    mp.setNoCFSRatio(doCFS ? 1.0 : 0.0);
    mp.setMaxCFSSegmentSizeMB(Double.POSITIVE_INFINITY);
    // TODO: remove randomness
    IndexWriterConfig conf =
        new IndexWriterConfig(new MockAnalyzer(random()))
            .setMaxBufferedDocs(10)
            .setCodec(TestUtil.getDefaultCodec())
            .setMergePolicy(NoMergePolicy.INSTANCE);
    IndexWriter writer = new IndexWriter(dir, conf);

    for (int i = 0; i < DOCS_COUNT; i++) {
      addDoc(writer, i);
    }
    assertEquals("wrong doc count", DOCS_COUNT, writer.getDocStats().maxDoc);
    if (fullyMerged) {
      writer.forceMerge(1);
    }
    writer.close();

    if (!fullyMerged) {
      // open fresh writer so we get no prx file in the added segment
      mp = new LogByteSizeMergePolicy();
      mp.setNoCFSRatio(doCFS ? 1.0 : 0.0);
      // TODO: remove randomness
      conf =
          new IndexWriterConfig(new MockAnalyzer(random()))
              .setMaxBufferedDocs(10)
              .setCodec(TestUtil.getDefaultCodec())
              .setMergePolicy(NoMergePolicy.INSTANCE);
      writer = new IndexWriter(dir, conf);
      addNoProxDoc(writer);
      writer.close();

      conf =
          new IndexWriterConfig(new MockAnalyzer(random()))
              .setMaxBufferedDocs(10)
              .setCodec(TestUtil.getDefaultCodec())
              .setMergePolicy(NoMergePolicy.INSTANCE);
      writer = new IndexWriter(dir, conf);
      Term searchTerm = new Term("id", String.valueOf(DELETED_ID));
      writer.deleteDocuments(searchTerm);
      writer.close();
    }
  }

  static void addDoc(IndexWriter writer, int id) throws IOException {
    Document doc = new Document();
    doc.add(new TextField("content", "aaa", Field.Store.NO));
    doc.add(new StringField("id", Integer.toString(id), Field.Store.YES));
    FieldType customType2 = new FieldType(TextField.TYPE_STORED);
    customType2.setStoreTermVectors(true);
    customType2.setStoreTermVectorPositions(true);
    customType2.setStoreTermVectorOffsets(true);
    doc.add(
        new Field(
            "autf8", "Lu\uD834\uDD1Ece\uD834\uDD60ne \u0000 \u2620 ab\ud917\udc17cd", customType2));
    doc.add(
        new Field(
            "utf8", "Lu\uD834\uDD1Ece\uD834\uDD60ne \u0000 \u2620 ab\ud917\udc17cd", customType2));
    doc.add(new Field("content2", "here is more content with aaa aaa aaa", customType2));
    doc.add(new Field("fie\u2C77ld", "field with non-ascii name", customType2));

    // add docvalues fields
    doc.add(new NumericDocValuesField("dvByte", (byte) id));
    byte[] bytes =
        new byte[] {(byte) (id >>> 24), (byte) (id >>> 16), (byte) (id >>> 8), (byte) id};
    BytesRef ref = new BytesRef(bytes);
    doc.add(new BinaryDocValuesField("dvBytesDerefFixed", ref));
    doc.add(new BinaryDocValuesField("dvBytesDerefVar", ref));
    doc.add(new SortedDocValuesField("dvBytesSortedFixed", ref));
    doc.add(new SortedDocValuesField("dvBytesSortedVar", ref));
    doc.add(new BinaryDocValuesField("dvBytesStraightFixed", ref));
    doc.add(new BinaryDocValuesField("dvBytesStraightVar", ref));
    doc.add(new DoubleDocValuesField("dvDouble", id));
    doc.add(new FloatDocValuesField("dvFloat", (float) id));
    doc.add(new NumericDocValuesField("dvInt", id));
    doc.add(new NumericDocValuesField("dvLong", id));
    doc.add(new NumericDocValuesField("dvPacked", id));
    doc.add(new NumericDocValuesField("dvShort", (short) id));
    doc.add(new SortedSetDocValuesField("dvSortedSet", ref));
    doc.add(new SortedNumericDocValuesField("dvSortedNumeric", id));

    doc.add(new IntPoint("intPoint1d", id));
    doc.add(new IntPoint("intPoint2d", id, 2 * id));
    doc.add(new FloatPoint("floatPoint1d", (float) id));
    doc.add(new FloatPoint("floatPoint2d", (float) id, (float) 2 * id));
    doc.add(new LongPoint("longPoint1d", id));
    doc.add(new LongPoint("longPoint2d", id, 2 * id));
    doc.add(new DoublePoint("doublePoint1d", id));
    doc.add(new DoublePoint("doublePoint2d", id, (double) 2 * id));
    doc.add(new BinaryPoint("binaryPoint1d", bytes));
    doc.add(new BinaryPoint("binaryPoint2d", bytes, bytes));

    // a field with both offsets and term vectors for a cross-check
    FieldType customType3 = new FieldType(TextField.TYPE_STORED);
    customType3.setStoreTermVectors(true);
    customType3.setStoreTermVectorPositions(true);
    customType3.setStoreTermVectorOffsets(true);
    customType3.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    doc.add(new Field("content5", "here is more content with aaa aaa aaa", customType3));
    // a field that omits only positions
    FieldType customType4 = new FieldType(TextField.TYPE_STORED);
    customType4.setStoreTermVectors(true);
    customType4.setStoreTermVectorPositions(false);
    customType4.setStoreTermVectorOffsets(true);
    customType4.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
    doc.add(new Field("content6", "here is more content with aaa aaa aaa", customType4));

    float[] vector = {KNN_VECTOR[0], KNN_VECTOR[1], KNN_VECTOR[2] + 0.1f * id};
    doc.add(new KnnFloatVectorField(KNN_VECTOR_FIELD, vector, KNN_VECTOR_FIELD_TYPE));

    // TODO:
    //   index different norms types via similarity (we use a random one currently?!)
    //   remove any analyzer randomness, explicitly add payloads for certain fields.
    writer.addDocument(doc);
  }

  static void addNoProxDoc(IndexWriter writer) throws IOException {
    Document doc = new Document();
    FieldType customType = new FieldType(TextField.TYPE_STORED);
    customType.setIndexOptions(IndexOptions.DOCS);
    Field f = new Field("content3", "aaa", customType);
    doc.add(f);
    FieldType customType2 = new FieldType();
    customType2.setStored(true);
    customType2.setIndexOptions(IndexOptions.DOCS);
    f = new Field("content4", "aaa", customType2);
    doc.add(f);
    writer.addDocument(doc);
  }

  public static void searchIndex(
      Directory dir, String oldName, int minIndexMajorVersion, Version nameVersion)
      throws IOException {
    // QueryParser parser = new QueryParser("contents", new MockAnalyzer(random));
    // Query query = parser.parse("handle:1");
    IndexCommit indexCommit = DirectoryReader.listCommits(dir).get(0);
    IndexReader reader = DirectoryReader.open(indexCommit, minIndexMajorVersion, null);
    IndexSearcher searcher = newSearcher(reader);

    TestUtil.checkIndex(dir);

    final Bits liveDocs = MultiBits.getLiveDocs(reader);
    assertNotNull(liveDocs);

    StoredFields storedFields = reader.storedFields();
    TermVectors termVectors = reader.termVectors();

    for (int i = 0; i < DOCS_COUNT; i++) {
      if (liveDocs.get(i)) {
        Document d = storedFields.document(i);
        List<IndexableField> fields = d.getFields();
        boolean isProxDoc = d.getField("content3") == null;
        if (isProxDoc) {
          assertEquals(7, fields.size());
          IndexableField f = d.getField("id");
          assertEquals("" + i, f.stringValue());

          f = d.getField("utf8");
          assertEquals(
              "Lu\uD834\uDD1Ece\uD834\uDD60ne \u0000 \u2620 ab\ud917\udc17cd", f.stringValue());

          f = d.getField("autf8");
          assertEquals(
              "Lu\uD834\uDD1Ece\uD834\uDD60ne \u0000 \u2620 ab\ud917\udc17cd", f.stringValue());

          f = d.getField("content2");
          assertEquals("here is more content with aaa aaa aaa", f.stringValue());

          f = d.getField("fie\u2C77ld");
          assertEquals("field with non-ascii name", f.stringValue());
        }

        Fields tfvFields = termVectors.get(i);
        assertNotNull("i=" + i, tfvFields);
        Terms tfv = tfvFields.terms("utf8");
        assertNotNull("docID=" + i + " index=" + oldName, tfv);
      } else {
        assertEquals(DELETED_ID, i);
      }
    }

    // check docvalues fields
    NumericDocValues dvByte = MultiDocValues.getNumericValues(reader, "dvByte");
    BinaryDocValues dvBytesDerefFixed = MultiDocValues.getBinaryValues(reader, "dvBytesDerefFixed");
    BinaryDocValues dvBytesDerefVar = MultiDocValues.getBinaryValues(reader, "dvBytesDerefVar");
    SortedDocValues dvBytesSortedFixed =
        MultiDocValues.getSortedValues(reader, "dvBytesSortedFixed");
    SortedDocValues dvBytesSortedVar = MultiDocValues.getSortedValues(reader, "dvBytesSortedVar");
    BinaryDocValues dvBytesStraightFixed =
        MultiDocValues.getBinaryValues(reader, "dvBytesStraightFixed");
    BinaryDocValues dvBytesStraightVar =
        MultiDocValues.getBinaryValues(reader, "dvBytesStraightVar");
    NumericDocValues dvDouble = MultiDocValues.getNumericValues(reader, "dvDouble");
    NumericDocValues dvFloat = MultiDocValues.getNumericValues(reader, "dvFloat");
    NumericDocValues dvInt = MultiDocValues.getNumericValues(reader, "dvInt");
    NumericDocValues dvLong = MultiDocValues.getNumericValues(reader, "dvLong");
    NumericDocValues dvPacked = MultiDocValues.getNumericValues(reader, "dvPacked");
    NumericDocValues dvShort = MultiDocValues.getNumericValues(reader, "dvShort");

    SortedSetDocValues dvSortedSet = MultiDocValues.getSortedSetValues(reader, "dvSortedSet");
    SortedNumericDocValues dvSortedNumeric =
        MultiDocValues.getSortedNumericValues(reader, "dvSortedNumeric");

    for (int i = 0; i < DOCS_COUNT; i++) {
      int id = Integer.parseInt(storedFields.document(i).get("id"));
      assertEquals(i, dvByte.nextDoc());
      assertEquals(id, dvByte.longValue());

      byte[] bytes =
          new byte[] {(byte) (id >>> 24), (byte) (id >>> 16), (byte) (id >>> 8), (byte) id};
      BytesRef expectedRef = new BytesRef(bytes);

      assertEquals(i, dvBytesDerefFixed.nextDoc());
      BytesRef term = dvBytesDerefFixed.binaryValue();
      assertEquals(expectedRef, term);
      assertEquals(i, dvBytesDerefVar.nextDoc());
      term = dvBytesDerefVar.binaryValue();
      assertEquals(expectedRef, term);
      assertEquals(i, dvBytesSortedFixed.nextDoc());
      term = dvBytesSortedFixed.lookupOrd(dvBytesSortedFixed.ordValue());
      assertEquals(expectedRef, term);
      assertEquals(i, dvBytesSortedVar.nextDoc());
      term = dvBytesSortedVar.lookupOrd(dvBytesSortedVar.ordValue());
      assertEquals(expectedRef, term);
      assertEquals(i, dvBytesStraightFixed.nextDoc());
      term = dvBytesStraightFixed.binaryValue();
      assertEquals(expectedRef, term);
      assertEquals(i, dvBytesStraightVar.nextDoc());
      term = dvBytesStraightVar.binaryValue();
      assertEquals(expectedRef, term);

      assertEquals(i, dvDouble.nextDoc());
      assertEquals(id, Double.longBitsToDouble(dvDouble.longValue()), 0D);
      assertEquals(i, dvFloat.nextDoc());
      assertEquals((float) id, Float.intBitsToFloat((int) dvFloat.longValue()), 0F);
      assertEquals(i, dvInt.nextDoc());
      assertEquals(id, dvInt.longValue());
      assertEquals(i, dvLong.nextDoc());
      assertEquals(id, dvLong.longValue());
      assertEquals(i, dvPacked.nextDoc());
      assertEquals(id, dvPacked.longValue());
      assertEquals(i, dvShort.nextDoc());
      assertEquals(id, dvShort.longValue());

      assertEquals(i, dvSortedSet.nextDoc());
      assertEquals(1, dvSortedSet.docValueCount());
      long ord = dvSortedSet.nextOrd();
      term = dvSortedSet.lookupOrd(ord);
      assertEquals(expectedRef, term);

      assertEquals(i, dvSortedNumeric.nextDoc());
      assertEquals(1, dvSortedNumeric.docValueCount());
      assertEquals(id, dvSortedNumeric.nextValue());
    }

    ScoreDoc[] hits = searcher.search(new TermQuery(new Term("content", "aaa")), 1000).scoreDocs;

    // First document should be #0
    Document d = storedFields.document(hits[0].doc);
    assertEquals("didn't get the right document first", "0", d.get("id"));

    doTestHits(hits, 34, searcher.getIndexReader());

    hits = searcher.search(new TermQuery(new Term("content5", "aaa")), 1000).scoreDocs;

    doTestHits(hits, 34, searcher.getIndexReader());

    hits = searcher.search(new TermQuery(new Term("content6", "aaa")), 1000).scoreDocs;

    doTestHits(hits, 34, searcher.getIndexReader());

    hits = searcher.search(new TermQuery(new Term("utf8", "\u0000")), 1000).scoreDocs;
    assertEquals(34, hits.length);
    hits =
        searcher.search(new TermQuery(new Term("utf8", "lu\uD834\uDD1Ece\uD834\uDD60ne")), 1000)
            .scoreDocs;
    assertEquals(34, hits.length);
    hits = searcher.search(new TermQuery(new Term("utf8", "ab\ud917\udc17cd")), 1000).scoreDocs;
    assertEquals(34, hits.length);

    doTestHits(
        searcher.search(IntPoint.newRangeQuery("intPoint1d", 0, 34), 1000).scoreDocs,
        34,
        searcher.getIndexReader());
    doTestHits(
        searcher.search(
                IntPoint.newRangeQuery("intPoint2d", new int[] {0, 0}, new int[] {34, 68}), 1000)
            .scoreDocs,
        34,
        searcher.getIndexReader());
    doTestHits(
        searcher.search(FloatPoint.newRangeQuery("floatPoint1d", 0f, 34f), 1000).scoreDocs,
        34,
        searcher.getIndexReader());
    doTestHits(
        searcher.search(
                FloatPoint.newRangeQuery(
                    "floatPoint2d", new float[] {0f, 0f}, new float[] {34f, 68f}),
                1000)
            .scoreDocs,
        34,
        searcher.getIndexReader());
    doTestHits(
        searcher.search(LongPoint.newRangeQuery("longPoint1d", 0, 34), 1000).scoreDocs,
        34,
        searcher.getIndexReader());
    doTestHits(
        searcher.search(
                LongPoint.newRangeQuery("longPoint2d", new long[] {0, 0}, new long[] {34, 68}),
                1000)
            .scoreDocs,
        34,
        searcher.getIndexReader());
    doTestHits(
        searcher.search(DoublePoint.newRangeQuery("doublePoint1d", 0.0, 34.0), 1000).scoreDocs,
        34,
        searcher.getIndexReader());
    doTestHits(
        searcher.search(
                DoublePoint.newRangeQuery(
                    "doublePoint2d", new double[] {0.0, 0.0}, new double[] {34.0, 68.0}),
                1000)
            .scoreDocs,
        34,
        searcher.getIndexReader());

    byte[] bytes1 = new byte[4];
    byte[] bytes2 = new byte[] {0, 0, 0, (byte) 34};
    doTestHits(
        searcher.search(BinaryPoint.newRangeQuery("binaryPoint1d", bytes1, bytes2), 1000).scoreDocs,
        34,
        searcher.getIndexReader());
    byte[] bytes3 = new byte[] {0, 0, 0, (byte) 68};
    doTestHits(
        searcher.search(
                BinaryPoint.newRangeQuery(
                    "binaryPoint2d", new byte[][] {bytes1, bytes1}, new byte[][] {bytes2, bytes3}),
                1000)
            .scoreDocs,
        34,
        searcher.getIndexReader());

    // test vector values and KNN search
    if (nameVersion.major >= KNN_VECTOR_MIN_SUPPORTED_VERSION) {
      // test vector values
      int cnt = 0;
      for (LeafReaderContext ctx : reader.leaves()) {
        FloatVectorValues values = ctx.reader().getFloatVectorValues(KNN_VECTOR_FIELD);
        if (values != null) {
          assertEquals(KNN_VECTOR_FIELD_TYPE.vectorDimension(), values.dimension());
          for (int doc = values.nextDoc(); doc != NO_MORE_DOCS; doc = values.nextDoc()) {
            float[] expectedVector = {KNN_VECTOR[0], KNN_VECTOR[1], KNN_VECTOR[2] + 0.1f * cnt};
            assertArrayEquals(
                "vectors do not match for doc=" + cnt, expectedVector, values.vectorValue(), 0);
            cnt++;
          }
        }
      }
      assertEquals(DOCS_COUNT, cnt);

      // test KNN search
      ScoreDoc[] scoreDocs = assertKNNSearch(searcher, KNN_VECTOR, 10, 10, "0");
      for (int i = 0; i < scoreDocs.length; i++) {
        int id = Integer.parseInt(storedFields.document(scoreDocs[i].doc).get("id"));
        int expectedId = i < DELETED_ID ? i : i + 1;
        assertEquals(expectedId, id);
      }
    }

    reader.close();
  }

  private static void doTestHits(ScoreDoc[] hits, int expectedCount, IndexReader reader)
      throws IOException {
    final int hitCount = hits.length;
    assertEquals("wrong number of hits", expectedCount, hitCount);
    StoredFields storedFields = reader.storedFields();
    TermVectors termVectors = reader.termVectors();
    for (ScoreDoc hit : hits) {
      storedFields.document(hit.doc);
      termVectors.get(hit.doc);
    }
  }

  static ScoreDoc[] assertKNNSearch(
      IndexSearcher searcher,
      float[] queryVector,
      int k,
      int expectedHitsCount,
      String expectedFirstDocId)
      throws IOException {
    ScoreDoc[] hits =
        searcher.search(new KnnFloatVectorQuery(KNN_VECTOR_FIELD, queryVector, k), k).scoreDocs;
    assertEquals("wrong number of hits", expectedHitsCount, hits.length);
    Document d = searcher.storedFields().document(hits[0].doc);
    assertEquals("wrong first document", expectedFirstDocId, d.get("id"));
    return hits;
  }

  public void changeIndexWithAdds(Random random, Directory dir, Version nameVersion)
      throws IOException {
    SegmentInfos infos = SegmentInfos.readLatestCommit(dir);
    assertEquals(nameVersion, infos.getCommitLuceneVersion());
    assertEquals(nameVersion, infos.getMinSegmentLuceneVersion());

    // open writer
    IndexWriter writer =
        new IndexWriter(
            dir,
            newIndexWriterConfig(new MockAnalyzer(random))
                .setOpenMode(IndexWriterConfig.OpenMode.APPEND)
                .setMergePolicy(newLogMergePolicy()));
    // add 10 docs
    for (int i = 0; i < 10; i++) {
      addDoc(writer, DOCS_COUNT + i);
    }

    // make sure writer sees right total -- writer seems not to know about deletes in .del?
    final int expected = 45;
    assertEquals("wrong doc count", expected, writer.getDocStats().numDocs);
    writer.close();

    // make sure searching sees right # hits for term search
    IndexReader reader = DirectoryReader.open(dir);
    IndexSearcher searcher = newSearcher(reader);
    ScoreDoc[] hits = searcher.search(new TermQuery(new Term("content", "aaa")), 1000).scoreDocs;
    Document d = searcher.getIndexReader().storedFields().document(hits[0].doc);
    assertEquals("wrong first document", "0", d.get("id"));
    doTestHits(hits, 44, searcher.getIndexReader());

    if (nameVersion.major >= KNN_VECTOR_MIN_SUPPORTED_VERSION) {
      // make sure KNN search sees all hits (graph may not be used if k is big)
      assertKNNSearch(searcher, KNN_VECTOR, 1000, 44, "0");
      // make sure KNN search using HNSW graph sees newly added docs
      assertKNNSearch(
          searcher,
          new float[] {KNN_VECTOR[0], KNN_VECTOR[1], KNN_VECTOR[2] + 0.1f * 44},
          10,
          10,
          "44");
    }
    reader.close();

    // fully merge
    writer =
        new IndexWriter(
            dir,
            newIndexWriterConfig(new MockAnalyzer(random))
                .setOpenMode(IndexWriterConfig.OpenMode.APPEND)
                .setMergePolicy(newLogMergePolicy()));
    writer.forceMerge(1);
    writer.close();

    reader = DirectoryReader.open(dir);
    searcher = newSearcher(reader);
    // make sure searching sees right # hits fot term search
    hits = searcher.search(new TermQuery(new Term("content", "aaa")), 1000).scoreDocs;
    assertEquals("wrong number of hits", 44, hits.length);
    d = searcher.storedFields().document(hits[0].doc);
    doTestHits(hits, 44, searcher.getIndexReader());
    assertEquals("wrong first document", "0", d.get("id"));

    if (nameVersion.major >= KNN_VECTOR_MIN_SUPPORTED_VERSION) {
      // make sure KNN search sees all hits
      assertKNNSearch(searcher, KNN_VECTOR, 1000, 44, "0");
      // make sure KNN search using HNSW graph sees newly added docs
      assertKNNSearch(
          searcher,
          new float[] {KNN_VECTOR[0], KNN_VECTOR[1], KNN_VECTOR[2] + 0.1f * 44},
          10,
          10,
          "44");
    }
    reader.close();
  }

  public void changeIndexNoAdds(Random random, Directory dir, Version nameVersion)
      throws IOException {
    // make sure searching sees right # hits for term search
    DirectoryReader reader = DirectoryReader.open(dir);
    IndexSearcher searcher = newSearcher(reader);
    ScoreDoc[] hits = searcher.search(new TermQuery(new Term("content", "aaa")), 1000).scoreDocs;
    assertEquals("wrong number of hits", 34, hits.length);
    Document d = searcher.storedFields().document(hits[0].doc);
    assertEquals("wrong first document", "0", d.get("id"));

    if (nameVersion.major >= KNN_VECTOR_MIN_SUPPORTED_VERSION) {
      // make sure KNN search sees all hits
      assertKNNSearch(searcher, KNN_VECTOR, 1000, 34, "0");
      // make sure KNN search using HNSW graph retrieves correct results
      assertKNNSearch(searcher, KNN_VECTOR, 10, 10, "0");
    }
    reader.close();

    // fully merge
    IndexWriter writer =
        new IndexWriter(
            dir,
            newIndexWriterConfig(new MockAnalyzer(random))
                .setOpenMode(IndexWriterConfig.OpenMode.APPEND));
    writer.forceMerge(1);
    writer.close();

    reader = DirectoryReader.open(dir);
    searcher = newSearcher(reader);
    // make sure searching sees right # hits fot term search
    hits = searcher.search(new TermQuery(new Term("content", "aaa")), 1000).scoreDocs;
    assertEquals("wrong number of hits", 34, hits.length);
    doTestHits(hits, 34, searcher.getIndexReader());
    // make sure searching sees right # hits for KNN search
    if (nameVersion.major >= KNN_VECTOR_MIN_SUPPORTED_VERSION) {
      // make sure KNN search sees all hits
      assertKNNSearch(searcher, KNN_VECTOR, 1000, 34, "0");
      // make sure KNN search using HNSW graph retrieves correct results
      assertKNNSearch(searcher, KNN_VECTOR, 10, 10, "0");
    }
    reader.close();
  }

  // flex: test basics of TermsEnum api on non-flex index
  public void testNextIntoWrongField() throws Exception {
    IndexReader r = DirectoryReader.open(directory);
    TermsEnum terms = MultiTerms.getTerms(r, "content").iterator();
    BytesRef t = terms.next();
    assertNotNull(t);

    // content field only has term aaa:
    assertEquals("aaa", t.utf8ToString());
    assertNull(terms.next());

    BytesRef aaaTerm = new BytesRef("aaa");

    // should be found exactly
    assertEquals(TermsEnum.SeekStatus.FOUND, terms.seekCeil(aaaTerm));
    assertEquals(DOCS_COUNT, countDocs(TestUtil.docs(random(), terms, null, PostingsEnum.NONE)));
    assertNull(terms.next());

    // should hit end of field
    assertEquals(TermsEnum.SeekStatus.END, terms.seekCeil(new BytesRef("bbb")));
    assertNull(terms.next());

    // should seek to aaa
    assertEquals(TermsEnum.SeekStatus.NOT_FOUND, terms.seekCeil(new BytesRef("a")));
    assertTrue(terms.term().bytesEquals(aaaTerm));
    assertEquals(DOCS_COUNT, countDocs(TestUtil.docs(random(), terms, null, PostingsEnum.NONE)));
    assertNull(terms.next());

    assertEquals(TermsEnum.SeekStatus.FOUND, terms.seekCeil(aaaTerm));
    assertEquals(DOCS_COUNT, countDocs(TestUtil.docs(random(), terms, null, PostingsEnum.NONE)));
    assertNull(terms.next());

    r.close();
  }

  /**
   * Test that we didn't forget to bump the current Constants.LUCENE_MAIN_VERSION. This is important
   * so that we can determine which version of lucene wrote the segment.
   */
  public void testOldVersions() throws Exception {
    // first create a little index with the current code and get the version
    Directory currentDir = newDirectory();
    RandomIndexWriter riw = new RandomIndexWriter(random(), currentDir);
    riw.addDocument(new Document());
    riw.close();
    DirectoryReader ir = DirectoryReader.open(currentDir);
    SegmentReader air = (SegmentReader) ir.leaves().get(0).reader();
    Version currentVersion = air.getSegmentInfo().info.getVersion();
    assertNotNull(currentVersion); // only 3.0 segments can have a null version
    ir.close();
    currentDir.close();

    // now check all the old indexes, their version should be < the current version
    DirectoryReader r = DirectoryReader.open(directory);
    for (LeafReaderContext context : r.leaves()) {
      air = (SegmentReader) context.reader();
      Version oldVersion = air.getSegmentInfo().info.getVersion();
      assertNotNull(oldVersion); // only 3.0 segments can have a null version
      assertTrue(
          "current Version.LATEST is <= an old index: did you forget to bump it?!",
          currentVersion.onOrAfter(oldVersion));
    }
    r.close();
  }

  public void testIndexCreatedVersion() throws IOException {
    SegmentInfos infos = SegmentInfos.readLatestCommit(directory);
    // those indexes are created by a single version so we can
    // compare the commit version with the created version
    assertEquals(infos.getCommitLuceneVersion().major, infos.getIndexCreatedVersionMajor());
  }

  public void testSegmentCommitInfoId() throws IOException {
    Directory dir = this.directory;
    SegmentInfos infos = SegmentInfos.readLatestCommit(dir);
    for (SegmentCommitInfo info : infos) {
      if (info.info.getVersion().onOrAfter(Version.fromBits(8, 6, 0))) {
        assertNotNull(info.toString(), info.getId());
      } else {
        assertNull(info.toString(), info.getId());
      }
    }
  }

  private int countDocs(PostingsEnum docs) throws IOException {
    int count = 0;
    while ((docs.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      count++;
    }
    return count;
  }

  public void testIndexOldIndexNoAdds() throws Exception {
    try (Directory dir = newDirectory(directory)) {
      changeIndexNoAdds(random(), dir, version);
    }
  }

  public void testIndexOldIndex() throws Exception {
    try (Directory dir = newDirectory(directory)) {
      changeIndexWithAdds(random(), dir, version);
    }
  }

  public void testSearchOldIndex() throws Exception {
    searchIndex(directory, indexPattern, Version.MIN_SUPPORTED_MAJOR, version);
  }

  public void testFullyMergeOldIndex() throws Exception {
    try (Directory dir = newDirectory(this.directory)) {
      final SegmentInfos oldSegInfos = SegmentInfos.readLatestCommit(dir);

      IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(new MockAnalyzer(random())));
      w.forceMerge(1);
      w.close();

      final SegmentInfos segInfos = SegmentInfos.readLatestCommit(dir);
      assertEquals(
          oldSegInfos.getIndexCreatedVersionMajor(), segInfos.getIndexCreatedVersionMajor());
      assertEquals(Version.LATEST, segInfos.asList().get(0).info.getVersion());
      assertEquals(
          oldSegInfos.asList().get(0).info.getMinVersion(),
          segInfos.asList().get(0).info.getMinVersion());
    }
  }

  public void testAddOldIndexes() throws IOException {
    SegmentInfos infos = SegmentInfos.readLatestCommit(directory);

    Directory targetDir = newDirectory();
    if (infos.getCommitLuceneVersion().major != Version.LATEST.major) {
      // both indexes are not compatible
      Directory targetDir2 = newDirectory();
      IndexWriter w = new IndexWriter(targetDir2, newIndexWriterConfig(new MockAnalyzer(random())));
      IllegalArgumentException e =
          expectThrows(IllegalArgumentException.class, () -> w.addIndexes(directory));
      assertTrue(
          e.getMessage(),
          e.getMessage()
              .startsWith(
                  "Cannot use addIndexes(Directory) with indexes that have been created by a different Lucene version."));
      w.close();
      targetDir2.close();

      // for the next test, we simulate writing to an index that was created on the same major
      // version
      new SegmentInfos(infos.getIndexCreatedVersionMajor()).commit(targetDir);
    }

    IndexWriter w = new IndexWriter(targetDir, newIndexWriterConfig(new MockAnalyzer(random())));
    w.addIndexes(directory);
    w.close();

    SegmentInfos si = SegmentInfos.readLatestCommit(targetDir);
    assertNull(
        "none of the segments should have been upgraded",
        si.asList().stream()
            .filter( // depending on the MergePolicy we might see these segments merged away
                sci ->
                    sci.getId() != null
                        && sci.info.getVersion().onOrAfter(Version.fromBits(8, 6, 0)) == false)
            .findAny()
            .orElse(null));
    if (VERBOSE) {
      System.out.println("\nTEST: done adding indices; now close");
    }

    targetDir.close();
  }

  public void testAddOldIndexesReader() throws IOException {
    SegmentInfos infos = SegmentInfos.readLatestCommit(directory);
    DirectoryReader reader = DirectoryReader.open(directory);

    Directory targetDir = newDirectory();
    if (infos.getCommitLuceneVersion().major != Version.LATEST.major) {
      Directory targetDir2 = newDirectory();
      IndexWriter w = new IndexWriter(targetDir2, newIndexWriterConfig(new MockAnalyzer(random())));
      IllegalArgumentException e =
          expectThrows(IllegalArgumentException.class, () -> TestUtil.addIndexesSlowly(w, reader));
      assertEquals(
          e.getMessage(),
          "Cannot merge a segment that has been created with major version 8 into this index which has been created by major version 9");
      w.close();
      targetDir2.close();

      // for the next test, we simulate writing to an index that was created on the same major
      // version
      new SegmentInfos(infos.getIndexCreatedVersionMajor()).commit(targetDir);
    }
    IndexWriter w = new IndexWriter(targetDir, newIndexWriterConfig(new MockAnalyzer(random())));
    TestUtil.addIndexesSlowly(w, reader);
    w.close();
    reader.close();
    SegmentInfos si = SegmentInfos.readLatestCommit(targetDir);
    assertNull(
        "all SCIs should have an id now",
        si.asList().stream().filter(sci -> sci.getId() == null).findAny().orElse(null));
    targetDir.close();
  }

  public void testFailOpenOldIndex() throws IOException {
    assumeFalse("doesn't work on current index", version.major == Version.LATEST.major);
    IndexCommit commit = DirectoryReader.listCommits(directory).get(0);
    IndexFormatTooOldException ex =
        expectThrows(
            IndexFormatTooOldException.class,
            () -> StandardDirectoryReader.open(commit, Version.LATEST.major, null));
    assertTrue(
        ex.getMessage()
            .contains("only supports reading from version " + Version.LATEST.major + " upwards."));
    // now open with allowed min version
    StandardDirectoryReader.open(commit, Version.MIN_SUPPORTED_MAJOR, null).close();
  }

  public void testOpenModeAndCreatedVersion() throws IOException {
    Directory dir = newDirectory(directory);
    int majorVersion = SegmentInfos.readLatestCommit(dir).getIndexCreatedVersionMajor();
    if (majorVersion != Version.MIN_SUPPORTED_MAJOR && majorVersion != Version.LATEST.major) {
      fail(
          "expected one of: ["
              + Version.MIN_SUPPORTED_MAJOR
              + ", "
              + Version.LATEST.major
              + "] but got: "
              + majorVersion);
    }
    for (IndexWriterConfig.OpenMode openMode : IndexWriterConfig.OpenMode.values()) {
      Directory tmpDir = newDirectory(dir);
      IndexWriter w = new IndexWriter(tmpDir, newIndexWriterConfig().setOpenMode(openMode));
      w.commit();
      w.close();
      switch (openMode) {
        case CREATE:
          assertEquals(
              Version.LATEST.major,
              SegmentInfos.readLatestCommit(tmpDir).getIndexCreatedVersionMajor());
          break;
        case APPEND:
        case CREATE_OR_APPEND:
        default:
          assertEquals(
              majorVersion, SegmentInfos.readLatestCommit(tmpDir).getIndexCreatedVersionMajor());
      }
      tmpDir.close();
    }
    dir.close();
  }
}
