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
package org.apache.lucene.search;

import java.io.IOException;
import java.nio.file.Path;
import org.apache.lucene.codecs.lucene103.blocktree.FieldReader;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.junit.Test;

public class TestIndexingMode extends LuceneTestCase {
  @Test
  public void testMMapDirectoryWithHotMode() throws IOException {
    testIndexingMode(IndexingMode.HOT);
  }

  @Test
  public void testMMapDirectoryWithColdMode() throws IOException {
    testIndexingMode(IndexingMode.COLD);
  }

  @Test
  public void testMMapDirectoryWithAdaptiveMode() throws IOException {
    testIndexingMode(IndexingMode.ADAPTIVE);
  }

  @Test
  public void testMMapDirectoryWithoutIndexingMode() throws IOException {
    Path tempDir = createTempDir();
    MMapDirectory dir = new MMapDirectory(tempDir, FSLockFactory.getDefault());
    IndexWriterConfig config = new IndexWriterConfig();
    config.setCodec(TestUtil.getDefaultCodec());
    // config.setUseCompoundFile(false); // Disable compound files
    IndexWriter writer = new IndexWriter(dir, config);

    Document doc = new Document();
    doc.add(new TextField("field", "test content", Field.Store.YES));
    writer.addDocument(doc);
    writer.close();

    DirectoryReader reader = DirectoryReader.open(dir);
    IndexSearcher searcher = new IndexSearcher(reader);

    TermQuery query = new TermQuery(new Term("field", "test"));
    TopDocs results = searcher.search(query, 10);
    assertEquals(1, results.totalHits.value());

    reader.close();
    dir.close();
  }

  @Test
  public void testMMapDirectoryIndexingModeFlow() throws IOException {
    for (IndexingMode mode : IndexingMode.values()) {
      testIndexingMode(mode);
    }
  }

  private void testIndexingMode(IndexingMode mode) throws IOException {
    Path tempDir = createTempDir();
    MMapDirectory dir = new MMapDirectory(tempDir, FSLockFactory.getDefault());
    dir.setIndexingMode(mode);
    IndexWriterConfig config = new IndexWriterConfig();
    config.setCodec(TestUtil.getDefaultCodec());
    IndexWriter writer = new IndexWriter(dir, config);

    Document doc = new Document();
    doc.add(new TextField("field", "test content", Field.Store.YES));
    writer.addDocument(doc);
    writer.close();

    DirectoryReader reader = DirectoryReader.open(dir);
    IndexSearcher searcher = new IndexSearcher(reader);

    TermQuery query = new TermQuery(new Term("field", "test"));
    TopDocs results = searcher.search(query, 10);
    assertEquals(1, results.totalHits.value());

    LeafReader leafReader = reader.leaves().get(0).reader();
    Terms terms = leafReader.terms("field");
    assertTrue(terms instanceof FieldReader);
    assertEquals(mode, ((FieldReader) terms).getIndexingMode());

    reader.close();
    dir.close();
  }
}
