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
import java.util.Arrays;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
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
}
