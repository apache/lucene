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

/** Test that the default codec detects mismatched checksums at open or checkIntegrity time. */
@SuppressFileSystems("ExtrasFS")
public class TestAllFilesDetectMismatchedChecksum extends LuceneTestCase {

  public void test() throws Exception {
    Directory dir = newDirectory();

    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    conf.setCodec(TestUtil.getDefaultCodec());
    // Disable CFS, which makes it harder to test due to its double checksumming
    conf.setUseCompoundFile(false);
    conf.getMergePolicy().setNoCFSRatio(0.0);

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
    riw.deleteDocuments(LongPoint.newRangeQuery("long", 0, 2));
    riw.close();
    checkMismatchedChecksum(dir);
    dir.close();
  }

  private void checkMismatchedChecksum(Directory dir) throws IOException {
    for (String name : dir.listAll()) {
      if (name.equals(IndexWriter.WRITE_LOCK_NAME) == false) {
        corruptFile(dir, name);
      }
    }
  }

  private void corruptFile(Directory dir, String victim) throws IOException {
    try (BaseDirectoryWrapper dirCopy = newDirectory()) {
      dirCopy.setCheckIndexOnClose(false);

      long victimLength = dir.fileLength(victim);
      long flipOffset =
          TestUtil.nextLong(
              random(), Math.max(0, victimLength - CodecUtil.footerLength()), victimLength - 1);

      if (VERBOSE) {
        System.out.println(
            "TEST: now corrupt file "
                + victim
                + " by changing byte at offset "
                + flipOffset
                + " (length= "
                + victimLength
                + ")");
      }

      for (String name : dir.listAll()) {
        if (name.equals(victim) == false) {
          dirCopy.copyFrom(dir, name, name, IOContext.DEFAULT);
        } else {
          try (IndexOutput out = dirCopy.createOutput(name, IOContext.DEFAULT);
              IndexInput in = dir.openInput(name, IOContext.DEFAULT)) {
            out.copyBytes(in, flipOffset);
            out.writeByte((byte) (in.readByte() + TestUtil.nextInt(random(), 0x01, 0xFF)));
            out.copyBytes(in, victimLength - flipOffset - 1);
          }
        }
        dirCopy.sync(Collections.singleton(name));
      }

      // corruption must be detected
      expectThrows(
          CorruptIndexException.class,
          () -> {
            try (IndexReader reader = DirectoryReader.open(dirCopy)) {
              for (LeafReaderContext context : reader.leaves()) {
                context.reader().checkIntegrity();
              }
            }
          });
    }
  }
}
