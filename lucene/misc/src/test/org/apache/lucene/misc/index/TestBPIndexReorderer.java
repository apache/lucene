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
package org.apache.lucene.misc.index;

import static org.apache.lucene.misc.index.BPIndexReorderer.fastLog2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SlowCodecReaderWrapper;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.ArrayUtil;

public class TestBPIndexReorderer extends LuceneTestCase {

  public void testSingleTerm() throws IOException {
    doTestSingleTerm(null);
  }

  public void testSingleTermWithForkJoinPool() throws IOException {
    int concurrency = TestUtil.nextInt(random(), 1, 8);
    // The default ForkJoinPool implementation uses a thread factory that removes all permissions on
    // threads, so we need to create our own to avoid tests failing with FS-based directories.
    ForkJoinPool pool =
        new ForkJoinPool(
            concurrency, p -> new ForkJoinWorkerThread(p) {}, null, random().nextBoolean());
    try {
      doTestSingleTerm(pool);
    } finally {
      pool.shutdown();
    }
  }

  public void doTestSingleTerm(ForkJoinPool pool) throws IOException {
    Directory dir = newDirectory();
    IndexWriter w =
        new IndexWriter(
            dir, newIndexWriterConfig().setMergePolicy(newLogMergePolicy(random().nextBoolean())));
    Document doc = new Document();
    StoredField idField = new StoredField("id", "");
    doc.add(idField);
    StringField bodyField = new StringField("body", "", Store.NO);
    doc.add(bodyField);

    idField.setStringValue("1");
    bodyField.setStringValue("lucene");
    w.addDocument(doc);

    idField.setStringValue("2");
    bodyField.setStringValue("lucene");
    w.addDocument(doc);

    idField.setStringValue("3");
    bodyField.setStringValue("search");
    w.addDocument(doc);

    idField.setStringValue("4");
    bodyField.setStringValue("lucene");
    w.addDocument(doc);

    idField.setStringValue("5");
    bodyField.setStringValue("search");
    w.addDocument(doc);

    idField.setStringValue("6");
    bodyField.setStringValue("lucene");
    w.addDocument(doc);

    idField.setStringValue("7");
    bodyField.setStringValue("search");
    w.addDocument(doc);

    idField.setStringValue("8");
    bodyField.setStringValue("search");
    w.addDocument(doc);

    w.forceMerge(1);

    DirectoryReader reader = DirectoryReader.open(w);
    LeafReader leafRealer = getOnlyLeafReader(reader);
    CodecReader codecReader = SlowCodecReaderWrapper.wrap(leafRealer);

    BPIndexReorderer reorderer = new BPIndexReorderer();
    reorderer.setForkJoinPool(pool);
    reorderer.setMinDocFreq(2);
    reorderer.setMinPartitionSize(1);
    reorderer.setMaxIters(10);
    CodecReader reordered = reorderer.reorder(codecReader, dir);
    String[] ids = new String[codecReader.maxDoc()];
    StoredFields storedFields = reordered.storedFields();
    for (int i = 0; i < codecReader.maxDoc(); ++i) {
      ids[i] = storedFields.document(i).get("id");
    }

    assertArrayEquals(
        // All "lucene" docs, then all "search" docs, preserving the existing doc ID order in case
        // of tie
        new String[] {"1", "2", "4", "6", "3", "5", "7", "8"}, ids);

    reader.close();
    w.close();
    dir.close();
  }

  public void testSingleTermWithBlocks() throws IOException {
    doTestSingleTermWithBlocks(null);
  }

  public void testSingleTermWithBlocksAndForkJoinPool() throws IOException {
    int concurrency = TestUtil.nextInt(random(), 1, 8);
    // The default ForkJoinPool implementation uses a thread factory that removes all permissions on
    // threads, so we need to create our own to avoid tests failing with FS-based directories.
    ForkJoinPool pool =
        new ForkJoinPool(
            concurrency, p -> new ForkJoinWorkerThread(p) {}, null, random().nextBoolean());
    try {
      doTestSingleTermWithBlocks(pool);
    } finally {
      pool.shutdown();
    }
  }

  private void doTestSingleTermWithBlocks(ForkJoinPool pool) throws IOException {
    Directory dir = newDirectory();
    IndexWriter w =
        new IndexWriter(
            dir,
            newIndexWriterConfig()
                .setParentField("parent")
                .setMergePolicy(newLogMergePolicy(random().nextBoolean())));

    w.addDocuments(createBlock("1", "lucene", "search", "lucene")); // 0-2
    w.addDocuments(createBlock("2", "lucene")); // 3
    w.addDocuments(createBlock("3", "search", "lucene", "search")); // 4-6
    w.addDocuments(createBlock("4", "lucene", "lucene", "search")); // 7-9
    w.addDocuments(createBlock("5", "lucene", "lucene", "lucene", "lucene")); // 10-13
    w.addDocuments(createBlock("6", "search", "search", "search")); // 14-16
    w.addDocuments(createBlock("7", "search", "lucene", "search")); // 17-19
    w.addDocuments(createBlock("8", "search", "search")); // 20-21

    w.forceMerge(1);

    DirectoryReader reader = DirectoryReader.open(w);
    LeafReader leafRealer = getOnlyLeafReader(reader);
    CodecReader codecReader = SlowCodecReaderWrapper.wrap(leafRealer);

    BPIndexReorderer reorderer = new BPIndexReorderer();
    reorderer.setForkJoinPool(pool);
    reorderer.setMinDocFreq(2);
    reorderer.setMinPartitionSize(1);
    reorderer.setMaxIters(10);
    CodecReader reordered = reorderer.reorder(codecReader, dir);
    StoredFields storedFields = reordered.storedFields();

    assertEquals("2", storedFields.document(0).get("id"));

    assertEquals("1", storedFields.document(1).get("parent_id"));
    assertEquals("0", storedFields.document(1).get("child_id"));
    assertEquals("1", storedFields.document(2).get("parent_id"));
    assertEquals("1", storedFields.document(2).get("child_id"));
    assertEquals("1", storedFields.document(3).get("id"));

    assertEquals("4", storedFields.document(4).get("parent_id"));
    assertEquals("0", storedFields.document(4).get("child_id"));
    assertEquals("4", storedFields.document(5).get("parent_id"));
    assertEquals("1", storedFields.document(5).get("child_id"));
    assertEquals("4", storedFields.document(6).get("id"));

    assertEquals("5", storedFields.document(7).get("parent_id"));
    assertEquals("0", storedFields.document(7).get("child_id"));
    assertEquals("5", storedFields.document(8).get("parent_id"));
    assertEquals("1", storedFields.document(8).get("child_id"));
    assertEquals("5", storedFields.document(9).get("parent_id"));
    assertEquals("2", storedFields.document(9).get("child_id"));
    assertEquals("5", storedFields.document(10).get("id"));

    assertEquals("3", storedFields.document(11).get("parent_id"));
    assertEquals("0", storedFields.document(11).get("child_id"));
    assertEquals("3", storedFields.document(12).get("parent_id"));
    assertEquals("1", storedFields.document(12).get("child_id"));
    assertEquals("3", storedFields.document(13).get("id"));

    assertEquals("7", storedFields.document(14).get("parent_id"));
    assertEquals("0", storedFields.document(14).get("child_id"));
    assertEquals("7", storedFields.document(15).get("parent_id"));
    assertEquals("1", storedFields.document(15).get("child_id"));
    assertEquals("7", storedFields.document(16).get("id"));

    assertEquals("8", storedFields.document(17).get("parent_id"));
    assertEquals("0", storedFields.document(17).get("child_id"));
    assertEquals("8", storedFields.document(18).get("id"));

    assertEquals("6", storedFields.document(19).get("parent_id"));
    assertEquals("0", storedFields.document(19).get("child_id"));
    assertEquals("6", storedFields.document(20).get("parent_id"));
    assertEquals("1", storedFields.document(20).get("child_id"));
    assertEquals("6", storedFields.document(21).get("id"));

    reader.close();
    w.close();
    dir.close();
  }

  private List<Document> createBlock(String parentID, String... values) {
    List<Document> docs = new ArrayList<>();
    for (int i = 0; i < values.length - 1; ++i) {
      Document doc = new Document();
      doc.add(new StoredField("parent_id", parentID));
      doc.add(new StoredField("child_id", Integer.toString(i)));
      doc.add(new StringField("body", values[i], Store.NO));
      docs.add(doc);
    }

    Document parentDoc = new Document();
    parentDoc.add(new StoredField("id", parentID));
    parentDoc.add(new StringField("body", values[values.length - 1], Store.NO));
    docs.add(parentDoc);

    return docs;
  }

  public void testMultiTerm() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w =
        new IndexWriter(
            dir, newIndexWriterConfig().setMergePolicy(newLogMergePolicy(random().nextBoolean())));
    Document doc = new Document();
    StoredField idField = new StoredField("id", "");
    doc.add(idField);
    TextField bodyField = new TextField("body", "", Store.NO);
    doc.add(bodyField);

    idField.setStringValue("1");
    bodyField.setStringValue("apache lucene");
    w.addDocument(doc);

    idField.setStringValue("2");
    bodyField.setStringValue("lucene");
    w.addDocument(doc);

    idField.setStringValue("3");
    bodyField.setStringValue("apache tomcat");
    w.addDocument(doc);

    idField.setStringValue("4");
    bodyField.setStringValue("apache lucene");
    w.addDocument(doc);

    idField.setStringValue("5");
    bodyField.setStringValue("tomcat");
    w.addDocument(doc);

    idField.setStringValue("6");
    bodyField.setStringValue("apache lucene");
    w.addDocument(doc);

    idField.setStringValue("7");
    bodyField.setStringValue("tomcat");
    w.addDocument(doc);

    idField.setStringValue("8");
    bodyField.setStringValue("apache tomcat");
    w.addDocument(doc);

    w.forceMerge(1);

    DirectoryReader reader = DirectoryReader.open(w);
    LeafReader leafRealer = getOnlyLeafReader(reader);
    CodecReader codecReader = SlowCodecReaderWrapper.wrap(leafRealer);

    BPIndexReorderer reorderer = new BPIndexReorderer();
    reorderer.setMinDocFreq(2);
    reorderer.setMinPartitionSize(1);
    reorderer.setMaxIters(10);
    CodecReader reordered = reorderer.reorder(codecReader, dir);
    String[] ids = new String[codecReader.maxDoc()];
    StoredFields storedFields = reordered.storedFields();
    for (int i = 0; i < codecReader.maxDoc(); ++i) {
      ids[i] = storedFields.document(i).get("id");
    }

    assertArrayEquals(
        // All "lucene" docs, then all "tomcat" docs, preserving the existing doc ID order in case
        // of tie
        new String[] {"1", "2", "4", "6", "3", "5", "7", "8"}, ids);

    reader.close();
    w.close();
    dir.close();
  }

  public void testFastLog2() {
    // Test powers of 2
    for (int i = 0; i < 31; ++i) {
      assertEquals(i, fastLog2(1 << i), 0f);
    }

    // Test non powers of 2
    for (int i = 3; i < 100_000; ++i) {
      assertEquals("" + i, (float) (Math.log(i) / Math.log(2)), fastLog2(i), 0.01f);
    }
  }

  public void testReadWriteInts() throws IOException {
    int[] ints = new int[17];

    for (int len = 1; len <= 17; ++len) {
      // random
      for (int i = 0; i < len; ++i) {
        ints[i] = random().nextInt(Integer.MAX_VALUE);
      }
      Arrays.sort(ints, 0, len);
      doTestReadWriteInts(ints, len);

      // incremental
      for (int i = 0; i < len; ++i) {
        ints[i] = i;
      }
      doTestReadWriteInts(ints, len);

      // incremental with offset
      for (int i = 0; i < len; ++i) {
        ints[i] = 100_000 + i;
      }
      doTestReadWriteInts(ints, len);

      // irregular deltas
      for (int i = 0; i < len; ++i) {
        ints[i] = 100_000 + (i * 31) & 0x07;
      }
      doTestReadWriteInts(ints, len);
    }
  }

  private void doTestReadWriteInts(int[] ints, int len) throws IOException {
    byte[] outBytes = new byte[len * Integer.BYTES + 1];
    ByteArrayDataOutput out = new ByteArrayDataOutput(outBytes);
    BPIndexReorderer.writeMonotonicInts(ArrayUtil.copyOfSubArray(ints, 0, len), len, out);
    ByteArrayDataInput in = new ByteArrayDataInput(outBytes, 0, out.getPosition());
    int[] restored = new int[17];
    final int restoredLen = BPIndexReorderer.readMonotonicInts(in, restored);
    assertArrayEquals(
        ArrayUtil.copyOfSubArray(ints, 0, len), ArrayUtil.copyOfSubArray(restored, 0, restoredLen));
  }

  public void testForwardIndexSorter() throws IOException {
    class Entry implements Comparable<Entry> {
      final int docId;
      final int termId;

      Entry(int docId, int termId) {
        this.docId = docId;
        this.termId = termId;
      }

      @Override
      public int compareTo(Entry o) {
        if (docId == o.docId) {
          return Integer.compare(termId, o.termId);
        } else {
          return Integer.compare(docId, o.docId);
        }
      }
    }

    try (Directory directory = newDirectory()) {
      for (int bits = 2; bits < 32; bits++) {
        int maxDoc = (1 << bits) - 1;
        int termNum = atLeast(100);
        List<Entry> entryList = new ArrayList<>();
        String fileName;
        try (IndexOutput out =
            directory.createTempOutput("testForwardIndexSorter", "sort", IOContext.DEFAULT)) {
          for (int termId = 0; termId < termNum; termId++) {
            int docNum = 0;
            int doc = 0;
            while (docNum < 100 && doc < maxDoc - 1) {
              doc = random().nextInt(maxDoc - (doc + 1)) + doc + 1;
              assertTrue(doc >= 0);
              docNum++;
              entryList.add(new Entry(doc, termId));
              out.writeLong((Integer.toUnsignedLong(termId) << 32) | Integer.toUnsignedLong(doc));
            }
          }
          CodecUtil.writeFooter(out);
          fileName = out.getName();
        }
        Collections.sort(entryList);
        new BPIndexReorderer.ForwardIndexSorter(directory)
            .sortAndConsume(
                fileName,
                maxDoc,
                new BPIndexReorderer.LongConsumer() {

                  int total = 0;

                  @Override
                  public void accept(long value) {
                    int doc = (int) value;
                    int term = (int) (value >>> 32);
                    Entry entry = entryList.get(total);
                    assertEquals(entry.docId, doc);
                    assertEquals(entry.termId, term);
                    total++;
                  }

                  @Override
                  public void onFinish() {
                    assertEquals(entryList.size(), total);
                  }
                });
      }
    }
  }
}
