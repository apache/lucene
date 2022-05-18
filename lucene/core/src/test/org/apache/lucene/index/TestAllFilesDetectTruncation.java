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
import java.util.Arrays;
import java.util.Collections;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.KnnVectorField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.store.BaseDirectoryWrapper;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;

/** Test that a plain default detects index file truncation early (on opening a reader). */
@SuppressFileSystems("ExtrasFS")
public class TestAllFilesDetectTruncation extends LuceneTestCase {

  public void test() throws Exception {
    doTest(false);
  }

  public void testCFS() throws Exception {
    doTest(true);
  }

  private void doTest(boolean cfs) throws Exception {
    Directory dir = newDirectory();

    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    conf.setCodec(TestUtil.getDefaultCodec());

    // Disable CFS 80% of the time so we can truncate individual files, but the other 20% of the
    // time we test truncation of .cfs/.cfe too:
    if (cfs == false) {
      conf.setUseCompoundFile(false);
      conf.getMergePolicy().setNoCFSRatio(0.0);
    }

    RandomIndexWriter riw = new RandomIndexWriter(random(), dir, conf);
    Document doc = new Document();
    FieldType textWithTermVectorsType = new FieldType(TextField.TYPE_STORED);
    textWithTermVectorsType.setStoreTermVectors(true);
    Field text = new Field("text", "", textWithTermVectorsType);
    doc.add(text);
    Field termString = new StringField("string", "", Store.YES);
    doc.add(termString);
    Field dvString = new SortedDocValuesField("string", new BytesRef());
    doc.add(dvString);
    Field pointNumber = new LongPoint("long", 0L);
    doc.add(pointNumber);
    Field dvNumber = new NumericDocValuesField("long", 0L);
    doc.add(dvNumber);
    KnnVectorField vector = new KnnVectorField("vector", new float[16]);
    doc.add(vector);

    for (int i = 0; i < 100; i++) {
      text.setStringValue(TestUtil.randomAnalysisString(random(), 20, true));
      String randomString = TestUtil.randomSimpleString(random(), 5);
      termString.setStringValue(randomString);
      dvString.setBytesValue(new BytesRef(randomString));
      long number = random().nextInt(10);
      pointNumber.setLongValue(number);
      dvNumber.setLongValue(number);
      Arrays.fill(vector.vectorValue(), i % 4);
      riw.addDocument(doc);
    }

    if (TEST_NIGHTLY == false) {
      riw.forceMerge(1);
    }

    riw.deleteDocuments(LongPoint.newRangeQuery("long", 0, 2));

    riw.close();
    checkTruncation(dir);
    dir.close();
  }

  private void checkTruncation(Directory dir) throws IOException {
    for (String name : dir.listAll()) {
      if (name.equals(IndexWriter.WRITE_LOCK_NAME) == false) {
        truncateOneFile(dir, name);
      }
    }
  }

  private void truncateOneFile(Directory dir, String victim) throws IOException {
    try (BaseDirectoryWrapper dirCopy = newDirectory()) {
      dirCopy.setCheckIndexOnClose(false);
      long victimLength = dir.fileLength(victim);
      int lostBytes = TestUtil.nextInt(random(), 1, (int) Math.min(100, victimLength));
      assert victimLength > 0;

      if (VERBOSE) {
        System.out.println(
            "TEST: now truncate file "
                + victim
                + " by removing "
                + lostBytes
                + " of "
                + victimLength
                + " bytes");
      }

      for (String name : dir.listAll()) {
        if (name.equals(victim) == false) {
          dirCopy.copyFrom(dir, name, name, IOContext.DEFAULT);
        } else {
          try (ChecksumIndexInput in = dir.openChecksumInput(name, IOContext.DEFAULT)) {
            try {
              CodecUtil.checkFooter(in);
              // In some rare cases, the codec footer would still appear as correct even though the
              // file has been truncated. We just skip the test is this rare case.
              return;
            } catch (
                @SuppressWarnings("unused")
                CorruptIndexException e) {
              // expected
            }
          }

          try (IndexOutput out = dirCopy.createOutput(name, IOContext.DEFAULT);
              IndexInput in = dir.openInput(name, IOContext.DEFAULT)) {
            out.copyBytes(in, victimLength - lostBytes);
          }
        }
        dirCopy.sync(Collections.singleton(name));
      }

      // There needs to be an exception thrown, but we don't care about its type, it's too heroic to
      // ensure that a specific exception type gets throws upon opening an index.
      // NOTE: we .close so that if the test fails (truncation not detected) we don't also get all
      // these confusing errors about open files:
      expectThrows(Exception.class, () -> DirectoryReader.open(dirCopy).close());

      // CheckIndex should also fail:
      expectThrows(Exception.class, () -> TestUtil.checkIndex(dirCopy, true, true, true, null));
    }
  }
}
