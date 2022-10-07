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

import java.io.IOException;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

/**
 * This test creates an index with 100 documents and 10 segments where all the documents have a
 * binary doc value, "textOrd", which is filled with consecutive integers. The documents with even
 * "textOrd" are marked for deletion. The index is then rearranged into 4 segments using {@link
 * BinaryDocValueSelector}.
 */
public class TestBinaryDocValueSelector extends LuceneTestCase {
  public void testRearrangeUsingBinaryDocValueSelector() throws Exception {
    Directory srcDir = newDirectory();
    createIndex(100, 10, srcDir);
    assertSequentialIndex(srcDir, 100, 10);

    Directory inputDir = newDirectory();
    createIndex(100, 4, inputDir);
    assertSequentialIndex(inputDir, 100, 4);

    Directory outputDir = newDirectory();
    IndexRearranger rearranger =
        new IndexRearranger(
            inputDir,
            outputDir,
            getIndexWriterConfig(),
            BinaryDocValueSelector.createLiveSelectorsFromIndex("textOrd", srcDir),
            BinaryDocValueSelector.createDeleteSelectorFromIndex("textOrd", srcDir));
    rearranger.execute();
    assertSequentialIndex(outputDir, 100, 10);

    outputDir.close();
    inputDir.close();
    srcDir.close();
  }

  private static void assertSequentialIndex(Directory dir, int docNum, int segNum)
      throws IOException {
    IndexReader reader = DirectoryReader.open(dir);
    long lastOrd = -1;
    for (int i = 0; i < segNum; i++) {
      LeafReader leafReader = reader.leaves().get(i).reader();
      NumericDocValues numericDocValues = leafReader.getNumericDocValues("ord");

      for (int doc = 0; doc < leafReader.numDocs(); doc++) {
        assertTrue(numericDocValues.advanceExact(doc));
        assertEquals(numericDocValues.longValue(), lastOrd + 1);
        lastOrd = numericDocValues.longValue();
      }
    }
    assertEquals(docNum, lastOrd + 1);
    reader.close();
  }

  private static IndexWriterConfig getIndexWriterConfig() {
    return new IndexWriterConfig(null)
        .setOpenMode(IndexWriterConfig.OpenMode.CREATE)
        .setMergePolicy(NoMergePolicy.INSTANCE)
        .setIndexSort(new Sort(new SortField("ord", SortField.Type.INT)));
  }

  private static void createIndex(int docNum, int segNum, Directory dir) throws IOException {
    IndexWriter w = new IndexWriter(dir, getIndexWriterConfig());
    int docPerSeg = (int) Math.ceil((double) docNum / segNum);
    for (int i = 0; i < docNum; i++) {
      Document doc = new Document();
      doc.add(new NumericDocValuesField("ord", i));
      doc.add(new BinaryDocValuesField("textOrd", new BytesRef(Integer.toString(i))));
      if (i % 2 == 0) {
        doc.add(new BinaryDocValuesField("delete", new BytesRef("yes")));
      }
      w.addDocument(doc);
      if (i % docPerSeg == docPerSeg - 1) {
        w.deleteDocuments(new Term("delete", "yes"));
        w.commit();
      }
    }
    IndexReader reader = DirectoryReader.open(w);
    assertEquals(segNum, reader.leaves().size());
    reader.close();
    w.close();
  }
}
