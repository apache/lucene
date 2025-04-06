/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import java.nio.file.Path;
import org.apache.lucene.codecs.lucene101.Lucene101PostingsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.junit.After;
import org.junit.Before;

public class TestBinScoreUtil extends LuceneTestCase {

  private static final String FIELD = "field";

  private Directory dir;
  private IndexWriter writer;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    Path path = createTempDir("binutil");
    dir = new MMapDirectory(path);
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(TestUtil.alwaysPostingsFormat(new Lucene101PostingsFormat()));
    iwc.setUseCompoundFile(false);
    writer = new IndexWriter(dir, iwc);
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();

    writer.close();
    dir.close();
  }

  public void testSingleSegmentWrapping() throws Exception {
    for (int i = 0; i < 5; i++) {
      Document doc = new Document();
      doc.add(newBinningField(FIELD, "term " + i));
      writer.addDocument(doc);
    }
    writer.commit();

    try (DirectoryReader reader = DirectoryReader.open(dir)) {
      IndexReader wrapped = BinScoreUtil.wrap(reader);
      for (LeafReaderContext ctx : wrapped.leaves()) {
        LeafReader leaf = ctx.reader();
        assertTrue(leaf instanceof BinScoreLeafReader);
        BinScoreReader bin = ((BinScoreLeafReader) leaf).getBinScoreReader();
        assertTrue(bin.getBinCount() > 0);
        for (int docID = 0; docID < leaf.maxDoc(); docID++) {
          int b = bin.getBinForDoc(docID);
          assertTrue("bin must be non-negative", b >= 0);
        }
      }
    }
  }

  public void testMultiSegmentWrapping() throws Exception {
    for (int i = 0; i < 3; i++) {
      Document doc = new Document();
      doc.add(newBinningField(FIELD, "segment " + i));
      writer.addDocument(doc);
      writer.commit();
    }

    try (DirectoryReader reader = DirectoryReader.open(dir)) {
      IndexReader wrapped = BinScoreUtil.wrap(reader);
      for (LeafReaderContext ctx : wrapped.leaves()) {
        LeafReader leaf = ctx.reader();
        assertTrue(leaf instanceof BinScoreLeafReader);
      }
    }
  }

  public void testCompoundFileWrapping() throws Exception {
    writer.getConfig().setUseCompoundFile(true);
    for (int i = 0; i < 4; i++) {
      Document doc = new Document();
      doc.add(newBinningField(FIELD, "compound " + i));
      writer.addDocument(doc);
    }
    writer.commit();

    try (DirectoryReader reader = DirectoryReader.open(dir)) {
      IndexReader wrapped = BinScoreUtil.wrap(reader);
      for (LeafReaderContext ctx : wrapped.leaves()) {
        LeafReader leaf = ctx.reader();
        assertTrue(leaf instanceof BinScoreLeafReader);
      }
    }
  }

  private static Field newBinningField(String name, String value) {
    FieldType type = new FieldType(TextField.TYPE_NOT_STORED);
    type.setStoreTermVectors(true);
    type.putAttribute("doBinning", "true");
    type.freeze();
    return new Field(name, value, type);
  }
}
