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

import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestMockRandomMergePolicy extends LuceneTestCase {

  public void testReverseWithParents() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, new IndexWriterConfig().setParentField("_parent"));
    List<Document> docs = new ArrayList<>();
    for (int i = 0; i < 5; ++i) {
      docs.add(new Document());
    }
    w.addDocuments(docs.subList(0, 2));
    w.addDocuments(docs.subList(0, 4));
    w.addDocuments(docs.subList(0, 3));
    w.forceMerge(1);
    w.close();
    DirectoryReader reader = DirectoryReader.open(dir);
    CodecReader codecReader = (CodecReader) getOnlyLeafReader(reader);
    Sorter.DocMap docMap = MockRandomMergePolicy.reverse(codecReader);

    assertEquals(7, docMap.oldToNew(0));
    assertEquals(8, docMap.oldToNew(1));
    assertEquals(3, docMap.oldToNew(2));
    assertEquals(4, docMap.oldToNew(3));
    assertEquals(5, docMap.oldToNew(4));
    assertEquals(6, docMap.oldToNew(5));
    assertEquals(0, docMap.oldToNew(6));
    assertEquals(1, docMap.oldToNew(7));
    assertEquals(2, docMap.oldToNew(8));

    assertEquals(6, docMap.newToOld(0));
    assertEquals(7, docMap.newToOld(1));
    assertEquals(8, docMap.newToOld(2));
    assertEquals(2, docMap.newToOld(3));
    assertEquals(3, docMap.newToOld(4));
    assertEquals(4, docMap.newToOld(5));
    assertEquals(5, docMap.newToOld(6));
    assertEquals(0, docMap.newToOld(7));
    assertEquals(1, docMap.newToOld(8));

    reader.close();
    dir.close();
  }
}
