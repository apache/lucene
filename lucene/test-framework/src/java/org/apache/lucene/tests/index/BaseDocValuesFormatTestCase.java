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
package org.apache.lucene.tests.index;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.function.Supplier;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.CheckIndex.Status.DocValuesStatus;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.TermsEnum.SeekStatus;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

/**
 * Extends {@link LegacyBaseDocValuesFormatTestCase} and adds checks for {@link DocValuesSkipper}.
 */
public abstract class BaseDocValuesFormatTestCase extends LegacyBaseDocValuesFormatTestCase {

  /**
   * Override and return {@code false} if the {@link DocValuesSkipper} produced by this format
   * sometimes returns documents in {@link DocValuesSkipper#minDocID(int)} or {@link
   * DocValuesSkipper#maxDocID(int)} that may not have a value.
   */
  protected boolean skipperHasAccurateDocBounds() {
    return true;
  }

  /**
   * Override and return {@code false} if the {@link DocValuesSkipper} produced by this format
   * sometimes returns values in {@link DocValuesSkipper#minValue(int)} or {@link
   * DocValuesSkipper#maxValue(int)} that none of the documents in the range have.
   */
  protected boolean skipperHasAccurateValueBounds() {
    return true;
  }

  public void testSortedMergeAwayAllValuesWithSkipper() throws IOException {
    Directory directory = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriterConfig iwconfig = newIndexWriterConfig(analyzer);
    iwconfig.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, iwconfig);

    Document doc = new Document();
    doc.add(new StringField("id", "0", Field.Store.NO));
    iwriter.addDocument(doc);
    doc = new Document();
    doc.add(new StringField("id", "1", Field.Store.NO));
    doc.add(SortedDocValuesField.indexedField("field", newBytesRef("hello")));
    iwriter.addDocument(doc);
    iwriter.commit();
    iwriter.deleteDocuments(new Term("id", "1"));
    iwriter.forceMerge(1);

    DirectoryReader ireader = iwriter.getReader();
    iwriter.close();

    SortedDocValues dv = getOnlyLeafReader(ireader).getSortedDocValues("field");
    assertEquals(NO_MORE_DOCS, dv.nextDoc());

    DocValuesSkipper skipper = getOnlyLeafReader(ireader).getDocValuesSkipper("field");
    assertEquals(0, skipper.docCount());
    skipper.advance(0);
    assertEquals(NO_MORE_DOCS, skipper.minDocID(0));

    TermsEnum termsEnum = dv.termsEnum();
    assertFalse(termsEnum.seekExact(new BytesRef("lucene")));
    assertEquals(SeekStatus.END, termsEnum.seekCeil(new BytesRef("lucene")));
    assertEquals(-1, dv.lookupTerm(new BytesRef("lucene")));

    ireader.close();
    directory.close();
  }

  public void testSortedSetMergeAwayAllValuesWithSkipper() throws IOException {
    Directory directory = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriterConfig iwconfig = newIndexWriterConfig(analyzer);
    iwconfig.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, iwconfig);

    Document doc = new Document();
    doc.add(new StringField("id", "0", Field.Store.NO));
    iwriter.addDocument(doc);
    doc = new Document();
    doc.add(new StringField("id", "1", Field.Store.NO));
    doc.add(SortedSetDocValuesField.indexedField("field", newBytesRef("hello")));
    iwriter.addDocument(doc);
    iwriter.commit();
    iwriter.deleteDocuments(new Term("id", "1"));
    iwriter.forceMerge(1);

    DirectoryReader ireader = iwriter.getReader();
    iwriter.close();

    SortedSetDocValues dv = getOnlyLeafReader(ireader).getSortedSetDocValues("field");
    assertEquals(0, dv.getValueCount());

    DocValuesSkipper skipper = getOnlyLeafReader(ireader).getDocValuesSkipper("field");
    assertEquals(0, skipper.docCount());
    skipper.advance(0);
    assertEquals(NO_MORE_DOCS, skipper.minDocID(0));

    TermsEnum termsEnum = dv.termsEnum();
    assertFalse(termsEnum.seekExact(new BytesRef("lucene")));
    assertEquals(SeekStatus.END, termsEnum.seekCeil(new BytesRef("lucene")));
    assertEquals(-1, dv.lookupTerm(new BytesRef("lucene")));

    ireader.close();
    directory.close();
  }

  public void testNumberMergeAwayAllValuesWithSkipper() throws IOException {
    Directory directory = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriterConfig iwconfig = newIndexWriterConfig(analyzer);
    iwconfig.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, iwconfig);

    Document doc = new Document();
    doc.add(new StringField("id", "0", Field.Store.NO));
    iwriter.addDocument(doc);
    doc = new Document();
    doc.add(new StringField("id", "1", Field.Store.NO));
    doc.add(NumericDocValuesField.indexedField("field", 5));
    iwriter.addDocument(doc);
    iwriter.commit();
    iwriter.deleteDocuments(new Term("id", "1"));
    iwriter.forceMerge(1);

    DirectoryReader ireader = iwriter.getReader();
    iwriter.close();

    NumericDocValues dv = getOnlyLeafReader(ireader).getNumericDocValues("field");
    assertEquals(NO_MORE_DOCS, dv.nextDoc());

    DocValuesSkipper skipper = getOnlyLeafReader(ireader).getDocValuesSkipper("field");
    assertEquals(0, skipper.docCount());
    skipper.advance(0);
    assertEquals(NO_MORE_DOCS, skipper.minDocID(0));

    ireader.close();
    directory.close();
  }

  public void testSortedNumberMergeAwayAllValuesWithSkipper() throws IOException {
    Directory directory = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriterConfig iwconfig = newIndexWriterConfig(analyzer);
    iwconfig.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, iwconfig);

    Document doc = new Document();
    doc.add(new StringField("id", "0", Field.Store.NO));
    iwriter.addDocument(doc);
    doc = new Document();
    doc.add(new StringField("id", "1", Field.Store.NO));
    doc.add(SortedNumericDocValuesField.indexedField("field", 5));
    iwriter.addDocument(doc);
    iwriter.commit();
    iwriter.deleteDocuments(new Term("id", "1"));
    iwriter.forceMerge(1);

    DirectoryReader ireader = iwriter.getReader();
    iwriter.close();

    SortedNumericDocValues dv = getOnlyLeafReader(ireader).getSortedNumericDocValues("field");
    assertEquals(NO_MORE_DOCS, dv.nextDoc());

    DocValuesSkipper skipper = getOnlyLeafReader(ireader).getDocValuesSkipper("field");
    assertEquals(0, skipper.docCount());
    skipper.advance(0);
    assertEquals(NO_MORE_DOCS, skipper.minDocID(0));

    ireader.close();
    directory.close();
  }

  // same as testSortedMergeAwayAllValues but on more than 1024 docs to have sparse encoding on
  public void testSortedMergeAwayAllValuesLargeSegmentWithSkipper() throws IOException {
    Directory directory = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriterConfig iwconfig = newIndexWriterConfig(analyzer);
    iwconfig.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, iwconfig);

    Document doc = new Document();
    doc.add(new StringField("id", "1", Field.Store.NO));
    doc.add(SortedDocValuesField.indexedField("field", newBytesRef("hello")));
    iwriter.addDocument(doc);
    final int numEmptyDocs = atLeast(1024);
    for (int i = 0; i < numEmptyDocs; ++i) {
      iwriter.addDocument(new Document());
    }
    iwriter.commit();
    iwriter.deleteDocuments(new Term("id", "1"));
    iwriter.forceMerge(1);

    DirectoryReader ireader = iwriter.getReader();
    iwriter.close();

    SortedDocValues dv = getOnlyLeafReader(ireader).getSortedDocValues("field");
    assertEquals(NO_MORE_DOCS, dv.nextDoc());

    DocValuesSkipper skipper = getOnlyLeafReader(ireader).getDocValuesSkipper("field");
    assertEquals(0, skipper.docCount());
    skipper.advance(0);
    assertEquals(NO_MORE_DOCS, skipper.minDocID(0));

    TermsEnum termsEnum = dv.termsEnum();
    assertFalse(termsEnum.seekExact(new BytesRef("lucene")));
    assertEquals(SeekStatus.END, termsEnum.seekCeil(new BytesRef("lucene")));
    assertEquals(-1, dv.lookupTerm(new BytesRef("lucene")));

    ireader.close();
    directory.close();
  }

  // same as testSortedSetMergeAwayAllValues but on more than 1024 docs to have sparse encoding on
  public void testSortedSetMergeAwayAllValuesLargeSegmentWithSkipper() throws IOException {
    Directory directory = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriterConfig iwconfig = newIndexWriterConfig(analyzer);
    iwconfig.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, iwconfig);

    Document doc = new Document();
    doc.add(new StringField("id", "1", Field.Store.NO));
    doc.add(SortedSetDocValuesField.indexedField("field", newBytesRef("hello")));
    iwriter.addDocument(doc);
    final int numEmptyDocs = atLeast(1024);
    for (int i = 0; i < numEmptyDocs; ++i) {
      iwriter.addDocument(new Document());
    }
    iwriter.commit();
    iwriter.deleteDocuments(new Term("id", "1"));
    iwriter.forceMerge(1);

    DirectoryReader ireader = iwriter.getReader();
    iwriter.close();

    SortedSetDocValues dv = getOnlyLeafReader(ireader).getSortedSetDocValues("field");
    assertEquals(NO_MORE_DOCS, dv.nextDoc());

    DocValuesSkipper skipper = getOnlyLeafReader(ireader).getDocValuesSkipper("field");
    assertEquals(0, skipper.docCount());
    skipper.advance(0);
    assertEquals(NO_MORE_DOCS, skipper.minDocID(0));

    TermsEnum termsEnum = dv.termsEnum();
    assertFalse(termsEnum.seekExact(new BytesRef("lucene")));
    assertEquals(SeekStatus.END, termsEnum.seekCeil(new BytesRef("lucene")));
    assertEquals(-1, dv.lookupTerm(new BytesRef("lucene")));

    ireader.close();
    directory.close();
  }

  // same as testNumericMergeAwayAllValues but on more than 1024 docs to have sparse encoding on
  public void testNumericMergeAwayAllValuesLargeSegmentWithSkipper() throws IOException {
    Directory directory = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriterConfig iwconfig = newIndexWriterConfig(analyzer);
    iwconfig.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, iwconfig);

    Document doc = new Document();
    doc.add(new StringField("id", "1", Field.Store.NO));
    doc.add(NumericDocValuesField.indexedField("field", 42L));
    iwriter.addDocument(doc);
    final int numEmptyDocs = atLeast(1024);
    for (int i = 0; i < numEmptyDocs; ++i) {
      iwriter.addDocument(new Document());
    }
    iwriter.commit();
    iwriter.deleteDocuments(new Term("id", "1"));
    iwriter.forceMerge(1);

    DirectoryReader ireader = iwriter.getReader();
    iwriter.close();

    NumericDocValues dv = getOnlyLeafReader(ireader).getNumericDocValues("field");
    assertEquals(NO_MORE_DOCS, dv.nextDoc());

    DocValuesSkipper skipper = getOnlyLeafReader(ireader).getDocValuesSkipper("field");
    assertEquals(0, skipper.docCount());
    skipper.advance(0);
    assertEquals(NO_MORE_DOCS, skipper.minDocID(0));

    ireader.close();
    directory.close();
  }

  // same as testSortedNumericMergeAwayAllValues but on more than 1024 docs to have sparse encoding
  // on
  public void testSortedNumericMergeAwayAllValuesLargeSegmentWithSkipper() throws IOException {
    Directory directory = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriterConfig iwconfig = newIndexWriterConfig(analyzer);
    iwconfig.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, iwconfig);

    Document doc = new Document();
    doc.add(new StringField("id", "1", Field.Store.NO));
    doc.add(SortedNumericDocValuesField.indexedField("field", 42L));
    iwriter.addDocument(doc);
    final int numEmptyDocs = atLeast(1024);
    for (int i = 0; i < numEmptyDocs; ++i) {
      iwriter.addDocument(new Document());
    }
    iwriter.commit();
    iwriter.deleteDocuments(new Term("id", "1"));
    iwriter.forceMerge(1);

    DirectoryReader ireader = iwriter.getReader();
    iwriter.close();

    SortedNumericDocValues dv = getOnlyLeafReader(ireader).getSortedNumericDocValues("field");
    assertEquals(NO_MORE_DOCS, dv.nextDoc());

    DocValuesSkipper skipper = getOnlyLeafReader(ireader).getDocValuesSkipper("field");
    assertEquals(0, skipper.docCount());
    skipper.advance(0);
    assertEquals(NO_MORE_DOCS, skipper.minDocID(0));

    ireader.close();
    directory.close();
  }

  public void testNumericDocValuesWithSkipperSmall() throws Exception {
    doTestNumericDocValuesWithSkipper(random().nextInt(1, 1000));
  }

  public void testNumericDocValuesWithSkipperMedium() throws Exception {
    doTestNumericDocValuesWithSkipper(random().nextInt(1000, 20000));
  }

  @Nightly
  public void testNumericDocValuesWithSkipperBig() throws Exception {
    doTestNumericDocValuesWithSkipper(random().nextInt(50000, 100000));
  }

  private void doTestNumericDocValuesWithSkipper(int totalDocs) throws Exception {
    assertDocValuesWithSkipper(
        totalDocs,
        new TestDocValueSkipper() {
          @Override
          public void populateDoc(Document doc) {
            doc.add(NumericDocValuesField.indexedField("test", random().nextLong()));
          }

          @Override
          public DocValuesWrapper docValuesWrapper(LeafReader leafReader) throws IOException {
            NumericDocValues numericDocValues = leafReader.getNumericDocValues("test");
            return new DocValuesWrapper() {

              @Override
              public int advance(int target) throws IOException {
                return numericDocValues.advance(target);
              }

              @Override
              public boolean advanceExact(int target) throws IOException {
                return numericDocValues.advanceExact(target);
              }

              @Override
              public long maxValue() throws IOException {
                return numericDocValues.longValue();
              }

              @Override
              public long minValue() throws IOException {
                return numericDocValues.longValue();
              }

              @Override
              public int docID() {
                return numericDocValues.docID();
              }
            };
          }

          @Override
          public DocValuesSkipper docValuesSkipper(LeafReader leafReader) throws IOException {
            return leafReader.getDocValuesSkipper("test");
          }
        });
  }

  public void testSortedNumericDocValuesWithSkipperSmall() throws Exception {
    doTestSortedNumericDocValuesWithSkipper(random().nextInt(1, 1000));
  }

  public void testSortedNumericDocValuesWithSkipperMedium() throws Exception {
    doTestSortedNumericDocValuesWithSkipper(random().nextInt(1000, 20000));
  }

  @Nightly
  public void testSortedNumericDocValuesWithSkipperBig() throws Exception {
    doTestSortedNumericDocValuesWithSkipper(random().nextInt(50000, 100000));
  }

  private void doTestSortedNumericDocValuesWithSkipper(int totalDocs) throws Exception {
    assertDocValuesWithSkipper(
        totalDocs,
        new TestDocValueSkipper() {
          @Override
          public void populateDoc(Document doc) {
            for (int j = 0; j < random().nextInt(1, 5); j++) {
              doc.add(SortedNumericDocValuesField.indexedField("test", random().nextLong()));
            }
          }

          @Override
          public DocValuesWrapper docValuesWrapper(LeafReader leafReader) throws IOException {
            SortedNumericDocValues sortedNumericDocValues =
                leafReader.getSortedNumericDocValues("test");
            return new DocValuesWrapper() {
              long max;
              long min;

              @Override
              public int advance(int target) throws IOException {
                int doc = sortedNumericDocValues.advance(target);
                if (doc != NO_MORE_DOCS) {
                  readValues();
                }
                return doc;
              }

              @Override
              public boolean advanceExact(int target) throws IOException {
                if (sortedNumericDocValues.advanceExact(target)) {
                  readValues();
                  return true;
                }
                return false;
              }

              private void readValues() throws IOException {
                max = Long.MIN_VALUE;
                min = Long.MAX_VALUE;
                for (int i = 0; i < sortedNumericDocValues.docValueCount(); i++) {
                  long value = sortedNumericDocValues.nextValue();
                  max = Math.max(max, value);
                  min = Math.min(min, value);
                }
              }

              @Override
              public long maxValue() {
                return max;
              }

              @Override
              public long minValue() {
                return min;
              }

              @Override
              public int docID() {
                return sortedNumericDocValues.docID();
              }
            };
          }

          @Override
          public DocValuesSkipper docValuesSkipper(LeafReader leafReader) throws IOException {
            return leafReader.getDocValuesSkipper("test");
          }
        });
  }

  public void testSortedDocValuesWithSkipperSmall() throws Exception {
    doTestSortedDocValuesWithSkipper(random().nextInt(1, 1000));
  }

  public void testSortedDocValuesWithSkipperMedium() throws Exception {
    doTestSortedDocValuesWithSkipper(random().nextInt(1000, 20000));
  }

  @Nightly
  public void testSortedDocValuesWithSkipperBig() throws Exception {
    doTestSortedDocValuesWithSkipper(random().nextInt(50000, 100000));
  }

  private void doTestSortedDocValuesWithSkipper(int totalDocs) throws Exception {
    assertDocValuesWithSkipper(
        totalDocs,
        new TestDocValueSkipper() {
          @Override
          public void populateDoc(Document doc) {
            doc.add(SortedDocValuesField.indexedField("test", TestUtil.randomBinaryTerm(random())));
          }

          @Override
          public DocValuesWrapper docValuesWrapper(LeafReader leafReader) throws IOException {
            SortedDocValues sortedDocValues = leafReader.getSortedDocValues("test");
            return new DocValuesWrapper() {

              @Override
              public int advance(int target) throws IOException {
                return sortedDocValues.advance(target);
              }

              @Override
              public boolean advanceExact(int target) throws IOException {
                return sortedDocValues.advanceExact(target);
              }

              @Override
              public long maxValue() throws IOException {
                return sortedDocValues.ordValue();
              }

              @Override
              public long minValue() throws IOException {
                return sortedDocValues.ordValue();
              }

              @Override
              public int docID() {
                return sortedDocValues.docID();
              }
            };
          }

          @Override
          public DocValuesSkipper docValuesSkipper(LeafReader leafReader) throws IOException {
            return leafReader.getDocValuesSkipper("test");
          }
        });
  }

  public void testSortedSetDocValuesWithSkipperSmall() throws Exception {
    doTestSortedSetDocValuesWithSkipper(random().nextInt(1, 1000));
  }

  public void testSortedSetDocValuesWithSkipperMedium() throws Exception {
    doTestSortedSetDocValuesWithSkipper(random().nextInt(10000, 20000));
  }

  @Nightly
  public void testSortedSetDocValuesWithSkipperBig() throws Exception {
    doTestSortedSetDocValuesWithSkipper(random().nextInt(50000, 100000));
  }

  private void doTestSortedSetDocValuesWithSkipper(int totalDocs) throws Exception {
    assertDocValuesWithSkipper(
        totalDocs,
        new TestDocValueSkipper() {
          @Override
          public void populateDoc(Document doc) {
            for (int j = 0; j < random().nextInt(1, 5); j++) {
              doc.add(
                  SortedSetDocValuesField.indexedField(
                      "test", TestUtil.randomBinaryTerm(random())));
            }
          }

          @Override
          public DocValuesWrapper docValuesWrapper(LeafReader leafReader) throws IOException {
            SortedSetDocValues sortedSetDocValues = leafReader.getSortedSetDocValues("test");
            return new DocValuesWrapper() {
              long max;
              long min;

              @Override
              public int advance(int target) throws IOException {
                int doc = sortedSetDocValues.advance(target);
                if (doc != NO_MORE_DOCS) {
                  readValues();
                }
                return doc;
              }

              @Override
              public boolean advanceExact(int target) throws IOException {
                if (sortedSetDocValues.advanceExact(target)) {
                  readValues();
                  return true;
                }
                return false;
              }

              private void readValues() throws IOException {
                max = Long.MIN_VALUE;
                min = Long.MAX_VALUE;
                for (int i = 0; i < sortedSetDocValues.docValueCount(); i++) {
                  long value = sortedSetDocValues.nextOrd();
                  max = Math.max(max, value);
                  min = Math.min(min, value);
                }
              }

              @Override
              public long maxValue() {
                return max;
              }

              @Override
              public long minValue() {
                return min;
              }

              @Override
              public int docID() {
                return sortedSetDocValues.docID();
              }
            };
          }

          @Override
          public DocValuesSkipper docValuesSkipper(LeafReader leafReader) throws IOException {
            return leafReader.getDocValuesSkipper("test");
          }
        });
  }

  private void assertDocValuesWithSkipper(int totalDocs, TestDocValueSkipper testDocValueSkipper)
      throws Exception {
    Supplier<Boolean> booleanSupplier;
    switch (random().nextInt(3)) {
      case 0 -> booleanSupplier = () -> true;
      case 1 -> booleanSupplier = () -> random().nextBoolean();
      case 2 -> booleanSupplier = () -> random().nextBoolean() && random().nextBoolean();
      default -> throw new AssertionError();
    }
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory);
    int numDocs = 0;
    for (int i = 0; i < totalDocs; i++) {
      Document doc = new Document();
      if (booleanSupplier.get()) {
        testDocValueSkipper.populateDoc(doc);
        numDocs++;
      }
      writer.addDocument(doc);
      if (rarely()) {
        writer.commit();
      }
    }
    writer.flush();

    if (random().nextBoolean()) {
      writer.forceMerge(1);
    }

    IndexReader r = writer.getReader();
    int readDocs = 0;
    for (LeafReaderContext readerContext : r.leaves()) {
      LeafReader reader = readerContext.reader();
      ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
      PrintStream infoStream = new PrintStream(bos, false, UTF_8);
      DocValuesStatus status = CheckIndex.testDocValues((CodecReader) reader, infoStream, true);
      if (status.error != null) {
        throw new Exception(status.error);
      }
      readDocs +=
          assertDocValuesSkipSequential(
              testDocValueSkipper.docValuesWrapper(reader),
              testDocValueSkipper.docValuesSkipper(reader));
      for (int i = 0; i < 10; i++) {
        assertDocValuesSkipRandom(
            testDocValueSkipper.docValuesWrapper(reader),
            testDocValueSkipper.docValuesSkipper(reader),
            reader.maxDoc());
      }
    }
    assertEquals(numDocs, readDocs);
    IOUtils.close(r, writer, directory);
  }

  private int assertDocValuesSkipSequential(DocValuesWrapper iterator, DocValuesSkipper skipper)
      throws IOException {
    if (skipper == null) {
      return 0;
    }

    assertEquals(-1, iterator.docID());
    assertEquals(-1, skipper.minDocID(0));
    assertEquals(-1, skipper.maxDocID(0));

    iterator.advance(0);
    int docCount = 0;
    while (true) {
      int previousMaxDoc = skipper.maxDocID(0);
      skipper.advance(previousMaxDoc + 1);
      assertTrue(skipper.minDocID(0) > previousMaxDoc);
      if (skipperHasAccurateDocBounds()) {
        assertEquals(iterator.docID(), skipper.minDocID(0));
      } else {
        assertTrue(
            "Expected: " + iterator.docID() + " but got " + skipper.minDocID(0),
            skipper.minDocID(0) <= iterator.docID());
      }

      if (skipper.minDocID(0) == NO_MORE_DOCS) {
        assertEquals(NO_MORE_DOCS, skipper.maxDocID(0));
        break;
      }
      assertTrue(skipper.docCount(0) > 0);

      int maxDoc = -1;
      long minVal = Long.MAX_VALUE;
      long maxVal = Long.MIN_VALUE;
      for (int i = 0; i < skipper.docCount(0); ++i) {
        assertNotEquals(NO_MORE_DOCS, iterator.docID());
        maxDoc = Math.max(maxDoc, iterator.docID());
        minVal = Math.min(minVal, iterator.minValue());
        maxVal = Math.max(maxVal, iterator.maxValue());
        iterator.advance(iterator.docID() + 1);
      }
      if (skipperHasAccurateDocBounds()) {
        assertEquals(maxDoc, skipper.maxDocID(0));
      } else {
        assertTrue(
            "Expected: " + maxDoc + " but got " + skipper.maxDocID(0),
            skipper.maxDocID(0) >= maxDoc);
      }
      if (skipperHasAccurateValueBounds()) {
        assertEquals(minVal, skipper.minValue(0));
        assertEquals(maxVal, skipper.maxValue(0));
      } else {
        assertTrue(
            "Expected: " + minVal + " but got " + skipper.minValue(0),
            minVal >= skipper.minValue(0));
        assertTrue(
            "Expected: " + maxVal + " but got " + skipper.maxValue(0),
            maxVal <= skipper.maxValue(0));
      }
      docCount += skipper.docCount(0);
      for (int level = 1; level < skipper.numLevels(); level++) {
        assertTrue(skipper.minDocID(0) >= skipper.minDocID(level));
        assertTrue(skipper.maxDocID(0) <= skipper.maxDocID(level));
        assertTrue(skipper.minValue(0) >= skipper.minValue(level));
        assertTrue(skipper.maxValue(0) <= skipper.maxValue(level));
        assertTrue(skipper.docCount(0) < skipper.docCount(level));
      }
    }

    assertEquals(docCount, skipper.docCount());
    return docCount;
  }

  private static void assertDocValuesSkipRandom(
      DocValuesWrapper iterator, DocValuesSkipper skipper, int maxDoc) throws IOException {
    if (skipper == null) {
      return;
    }
    int nextLevel = 0;
    while (true) {
      int doc = random().nextInt(skipper.maxDocID(nextLevel), maxDoc + 1) + 1;
      skipper.advance(doc);
      if (skipper.minDocID(0) == NO_MORE_DOCS) {
        assertEquals(NO_MORE_DOCS, skipper.maxDocID(0));
        return;
      }
      if (iterator.advanceExact(doc)) {
        for (int level = 0; level < skipper.numLevels(); level++) {
          assertTrue(iterator.docID() >= skipper.minDocID(level));
          assertTrue(iterator.docID() <= skipper.maxDocID(level));
          assertTrue(iterator.minValue() >= skipper.minValue(level));
          assertTrue(iterator.maxValue() <= skipper.maxValue(level));
        }
      }
      nextLevel = random().nextInt(skipper.numLevels());
    }
  }

  private interface TestDocValueSkipper {

    void populateDoc(Document doc);

    DocValuesWrapper docValuesWrapper(LeafReader leafReader) throws IOException;

    DocValuesSkipper docValuesSkipper(LeafReader leafReader) throws IOException;
  }

  private interface DocValuesWrapper {

    int advance(int target) throws IOException;

    boolean advanceExact(int target) throws IOException;

    long maxValue() throws IOException;

    long minValue() throws IOException;

    int docID();
  }

  public void testMismatchedFields() throws Exception {
    Directory dir1 = newDirectory();
    IndexWriter w1 = new IndexWriter(dir1, newIndexWriterConfig());
    Document doc = new Document();
    doc.add(new BinaryDocValuesField("binary", new BytesRef("lucene")));
    doc.add(new NumericDocValuesField("numeric", 0L));
    doc.add(new SortedDocValuesField("sorted", new BytesRef("search")));
    doc.add(new SortedNumericDocValuesField("sorted_numeric", 1L));
    doc.add(new SortedSetDocValuesField("sorted_set", new BytesRef("engine")));
    w1.addDocument(doc);

    Directory dir2 = newDirectory();
    IndexWriter w2 =
        new IndexWriter(dir2, newIndexWriterConfig().setMergeScheduler(new SerialMergeScheduler()));
    w2.addDocument(doc);
    w2.commit();

    DirectoryReader reader = DirectoryReader.open(w1);
    w1.close();
    w2.addIndexes(new MismatchedCodecReader((CodecReader) getOnlyLeafReader(reader), random()));
    reader.close();
    w2.forceMerge(1);
    reader = DirectoryReader.open(w2);
    w2.close();

    LeafReader leafReader = getOnlyLeafReader(reader);

    BinaryDocValues bdv = leafReader.getBinaryDocValues("binary");
    assertNotNull(bdv);
    assertEquals(0, bdv.nextDoc());
    assertEquals(new BytesRef("lucene"), bdv.binaryValue());
    assertEquals(1, bdv.nextDoc());
    assertEquals(new BytesRef("lucene"), bdv.binaryValue());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, bdv.nextDoc());

    NumericDocValues ndv = leafReader.getNumericDocValues("numeric");
    assertNotNull(ndv);
    assertEquals(0, ndv.nextDoc());
    assertEquals(0, ndv.longValue());
    assertEquals(1, ndv.nextDoc());
    assertEquals(0, ndv.longValue());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, ndv.nextDoc());

    SortedDocValues sdv = leafReader.getSortedDocValues("sorted");
    assertNotNull(sdv);
    assertEquals(0, sdv.nextDoc());
    assertEquals(new BytesRef("search"), sdv.lookupOrd(sdv.ordValue()));
    assertEquals(1, sdv.nextDoc());
    assertEquals(new BytesRef("search"), sdv.lookupOrd(sdv.ordValue()));
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, sdv.nextDoc());

    SortedNumericDocValues sndv = leafReader.getSortedNumericDocValues("sorted_numeric");
    assertNotNull(sndv);
    assertEquals(0, sndv.nextDoc());
    assertEquals(1, sndv.nextValue());
    assertEquals(1, sndv.nextDoc());
    assertEquals(1, sndv.nextValue());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, sndv.nextDoc());

    SortedSetDocValues ssdv = leafReader.getSortedSetDocValues("sorted_set");
    assertNotNull(ssdv);
    assertEquals(0, ssdv.nextDoc());
    assertEquals(new BytesRef("engine"), ssdv.lookupOrd(ssdv.nextOrd()));
    assertEquals(1, ssdv.nextDoc());
    assertEquals(new BytesRef("engine"), ssdv.lookupOrd(ssdv.nextOrd()));
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, ssdv.nextDoc());

    IOUtils.close(reader, w2, dir1, dir2);
  }
}
