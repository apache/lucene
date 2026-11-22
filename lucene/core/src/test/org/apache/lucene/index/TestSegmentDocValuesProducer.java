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

import java.io.IOException;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.NullInfoStream;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

public class TestSegmentDocValuesProducer extends LuceneTestCase {

  private static final int NUM_DOCS = 20;
  private static final String SOFT_DELETES = "__soft_deletes";

  public void testOnlyUpdatedFieldHasEntryWithoutOverlays() throws Exception {
    doTestOnlyUpdatedFieldHasEntry(0);
  }

  public void testOnlyUpdatedFieldHasEntryWithOverlays() throws Exception {
    doTestOnlyUpdatedFieldHasEntry(IndexWriterConfig.DEFAULT_MAX_DOC_VALUES_OVERLAYS);
  }

  private void doTestOnlyUpdatedFieldHasEntry(int maxDocValuesOverlays) throws Exception {
    IndexWriterConfig conf =
        new IndexWriterConfig(new MockAnalyzer(random()))
            .setMaxDocValuesOverlays(maxDocValuesOverlays)
            .setMergePolicy(NoMergePolicy.INSTANCE);
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, conf)) {
      addDocs(w);
      w.commit();
      SkipperSnapshot coreSkipper;
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        coreSkipper = SkipperSnapshot.of(getOnlyLeafReader(reader));
      }

      int updatedDoc = random().nextInt(NUM_DOCS);
      w.updateNumericDocValue(new Term("id", id(updatedDoc)), "num", 1000L);
      w.commit();

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        LeafReader leaf = getOnlyLeafReader(reader);
        SegmentDocValuesProducer producer = segmentDocValuesProducer(leaf);
        assertEquals(1, producer.dvProducersByField.size());
        assertTrue(producer.dvProducersByField.containsKey(fieldNumber(leaf, "num")));
        assertEquals(coreSkipper, SkipperSnapshot.of(leaf));
        assertAllValues(leaf, updatedDoc, 1000L);
      }
      TestUtil.checkIndex(dir);
    }
  }

  public void testSoftDeletesOnlyMapSoftDeletesField() throws Exception {
    IndexWriterConfig conf =
        new IndexWriterConfig(new MockAnalyzer(random()))
            .setSoftDeletesField(SOFT_DELETES)
            .setMergePolicy(NoMergePolicy.INSTANCE);
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, conf)) {
      addDocs(w);
      w.commit();
      int deletedDoc = random().nextInt(NUM_DOCS);
      w.updateDocValues(
          new Term("id", id(deletedDoc)), new NumericDocValuesField(SOFT_DELETES, 1L));
      w.commit();

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        LeafReader leaf = getOnlyLeafReader(reader);
        SegmentDocValuesProducer producer = segmentDocValuesProducer(leaf);
        assertEquals(1, producer.dvProducersByField.size());
        assertTrue(producer.dvProducersByField.containsKey(fieldNumber(leaf, SOFT_DELETES)));
        assertAllValues(leaf, -1, 0L);
        NumericDocValues softDeletes = leaf.getNumericDocValues(SOFT_DELETES);
        assertEquals(docId(leaf, deletedDoc), softDeletes.nextDoc());
        assertEquals(1L, softDeletes.longValue());
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, softDeletes.nextDoc());
      }
      TestUtil.checkIndex(dir);
    }
  }

  public void testEveryDocValuesFieldUpdated() throws Exception {
    int maxDocValuesOverlays =
        random().nextBoolean() ? 0 : IndexWriterConfig.DEFAULT_MAX_DOC_VALUES_OVERLAYS;
    IndexWriterConfig conf =
        new IndexWriterConfig(new MockAnalyzer(random()))
            .setMaxDocValuesOverlays(maxDocValuesOverlays)
            .setMergePolicy(NoMergePolicy.INSTANCE);
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, conf)) {
      for (int i = 0; i < NUM_DOCS; i++) {
        Document doc = new Document();
        doc.add(new StringField("id", id(i), StringField.Store.NO));
        doc.add(new NumericDocValuesField("num", i));
        doc.add(new BinaryDocValuesField("bin", new BytesRef("b" + i)));
        w.addDocument(doc);
      }
      w.commit();
      w.updateNumericDocValue(new Term("id", id(3)), "num", 1000L);
      w.updateBinaryDocValue(new Term("id", id(5)), "bin", new BytesRef("updated"));
      w.commit();

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        LeafReader leaf = getOnlyLeafReader(reader);
        SegmentDocValuesProducer producer = segmentDocValuesProducer(leaf);
        assertEquals(2, producer.dvProducersByField.size());
        if (maxDocValuesOverlays == 0) {
          assertFalse(producer.dvGens.contains(-1));
        }
        NumericDocValues num = leaf.getNumericDocValues("num");
        BinaryDocValues bin = leaf.getBinaryDocValues("bin");
        for (int i = 0; i < NUM_DOCS; i++) {
          int doc = docId(leaf, i);
          assertTrue(num.advanceExact(doc));
          assertEquals(i == 3 ? 1000L : i, num.longValue());
          assertTrue(bin.advanceExact(doc));
          assertEquals(new BytesRef(i == 5 ? "updated" : "b" + i), bin.binaryValue());
        }
      }
      TestUtil.checkIndex(dir);
    }
  }

  public void testPendingFieldWithoutCoreColumnReadsFromBase() throws Exception {
    try (Directory dir = newDirectory()) {
      FieldInfos.FieldNumbers fieldNumbers;
      IndexWriterConfig conf =
          new IndexWriterConfig(new MockAnalyzer(random()))
              .setMaxDocValuesOverlays(0)
              .setMergePolicy(NoMergePolicy.INSTANCE);
      try (IndexWriter w = new IndexWriter(dir, conf)) {
        Document doc = new Document();
        doc.add(new NumericDocValuesField("num", 1));
        w.addDocument(doc);
        w.commit();
        Document other = new Document();
        other.add(new NumericDocValuesField("extra", 2));
        w.addDocument(other);
        w.commit();
        fieldNumbers = w.globalFieldNumberMap;
      }

      StandardDirectoryReader reader = (StandardDirectoryReader) DirectoryReader.open(dir);
      assertNull(reader.leaves().get(0).reader().getFieldInfos().fieldInfo("extra"));
      SegmentInfos segmentInfos = reader.segmentInfos.clone();
      SegmentCommitInfo info = segmentInfos.info(0);
      ReaderPool pool =
          new ReaderPool(
              dir,
              dir,
              segmentInfos,
              fieldNumbers,
              () -> 0L,
              new NullInfoStream(),
              null,
              null,
              new IndexWriterConfig().setMaxDocValuesOverlays(0));
      try {
        ReadersAndUpdates rld = pool.get(info, true);
        NumericDocValuesFieldUpdates num =
            new NumericDocValuesFieldUpdates(1, "num", info.info.maxDoc());
        num.add(0, 10L);
        num.finish();
        rld.addDVUpdate(num);
        NumericDocValuesFieldUpdates extra =
            new NumericDocValuesFieldUpdates(3, "extra", info.info.maxDoc());
        extra.add(0, 20L);
        extra.finish();
        rld.addDVUpdate(extra);

        assertTrue(rld.writeFieldUpdates(dir, fieldNumbers, 1, new NullInfoStream()));
        assertTrue(rld.writeFieldUpdates(dir, fieldNumbers, 3, new NullInfoStream()));

        SegmentReader updated = rld.getReadOnlyClone(IOContext.DEFAULT);
        try {
          NumericDocValues numValues = updated.getNumericDocValues("num");
          assertTrue(numValues.advanceExact(0));
          assertEquals(10L, numValues.longValue());
          NumericDocValues extraValues = updated.getNumericDocValues("extra");
          assertTrue(extraValues.advanceExact(0));
          assertEquals(20L, extraValues.longValue());
        } finally {
          updated.close();
        }
        pool.release(rld, false);
      } finally {
        IOUtils.close(pool, reader);
      }
    }
  }

  private static void addDocs(IndexWriter w) throws IOException {
    for (int i = 0; i < NUM_DOCS; i++) {
      Document doc = new Document();
      doc.add(new StringField("id", id(i), StringField.Store.NO));
      doc.add(new NumericDocValuesField("num", i));
      doc.add(new NumericDocValuesField("num2", -i));
      doc.add(new BinaryDocValuesField("bin", new BytesRef("b" + i)));
      doc.add(new SortedDocValuesField("sorted", new BytesRef("s" + i)));
      doc.add(new SortedSetDocValuesField("sset", new BytesRef("a" + i)));
      doc.add(new SortedSetDocValuesField("sset", new BytesRef("z" + i)));
      doc.add(new SortedNumericDocValuesField("snum", i));
      doc.add(new SortedNumericDocValuesField("snum", i + 100));
      doc.add(NumericDocValuesField.indexedField("skip", i));
      w.addDocument(doc);
    }
  }

  private static void assertAllValues(LeafReader leaf, int updatedDoc, long updatedValue)
      throws IOException {
    NumericDocValues num = leaf.getNumericDocValues("num");
    NumericDocValues num2 = leaf.getNumericDocValues("num2");
    BinaryDocValues bin = leaf.getBinaryDocValues("bin");
    SortedDocValues sorted = leaf.getSortedDocValues("sorted");
    SortedSetDocValues sset = leaf.getSortedSetDocValues("sset");
    SortedNumericDocValues snum = leaf.getSortedNumericDocValues("snum");
    NumericDocValues skip = leaf.getNumericDocValues("skip");
    for (int i = 0; i < NUM_DOCS; i++) {
      int doc = docId(leaf, i);
      assertTrue(num.advanceExact(doc));
      assertEquals(i == updatedDoc ? updatedValue : i, num.longValue());
      assertTrue(num2.advanceExact(doc));
      assertEquals(-i, num2.longValue());
      assertTrue(bin.advanceExact(doc));
      assertEquals(new BytesRef("b" + i), bin.binaryValue());
      assertTrue(sorted.advanceExact(doc));
      assertEquals(new BytesRef("s" + i), sorted.lookupOrd(sorted.ordValue()));
      assertTrue(sset.advanceExact(doc));
      assertEquals(2, sset.docValueCount());
      assertEquals(new BytesRef("a" + i), sset.lookupOrd(sset.nextOrd()));
      assertEquals(new BytesRef("z" + i), sset.lookupOrd(sset.nextOrd()));
      assertTrue(snum.advanceExact(doc));
      assertEquals(2, snum.docValueCount());
      assertEquals(i, snum.nextValue());
      assertEquals(i + 100, snum.nextValue());
      assertTrue(skip.advanceExact(doc));
      assertEquals(i, skip.longValue());
    }
  }

  private static SegmentDocValuesProducer segmentDocValuesProducer(LeafReader leaf) {
    SegmentReader segmentReader = (SegmentReader) leaf;
    return (SegmentDocValuesProducer) segmentReader.getDocValuesReader();
  }

  private static int fieldNumber(LeafReader leaf, String field) {
    return leaf.getFieldInfos().fieldInfo(field).number;
  }

  private static int docId(LeafReader leaf, int i) throws IOException {
    PostingsEnum postings = leaf.postings(new Term("id", id(i)));
    assertNotNull(postings);
    return postings.nextDoc();
  }

  private static String id(int i) {
    return "d" + i;
  }

  private record SkipperSnapshot(long minValue, long maxValue, int docCount) {
    static SkipperSnapshot of(LeafReader leaf) throws IOException {
      DocValuesSkipper skipper = leaf.getDocValuesSkipper("skip");
      if (skipper == null) {
        return null;
      }
      return new SkipperSnapshot(skipper.minValue(), skipper.maxValue(), skipper.docCount());
    }
  }
}
