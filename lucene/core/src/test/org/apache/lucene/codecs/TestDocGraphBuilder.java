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
package org.apache.lucene.codecs;

import java.io.IOException;
import java.nio.file.Path;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.Test;

/** Unit test for {@link DocGraphBuilder} without external analyzer dependencies. */
public class TestDocGraphBuilder extends LuceneTestCase {

  @Test
  public void testSparseEdgeGraphConstruction() throws Exception {
    Path indexPath = createTempDir("doc-graph-builder");
    try (Directory dir = FSDirectory.open(indexPath)) {
      IndexWriterConfig iwc =
          new IndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.SIMPLE, true));
      iwc.setUseCompoundFile(false);
      iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

      try (IndexWriter writer = new IndexWriter(dir, iwc)) {
        addDoc(writer, "alpha beta gamma");
        addDoc(writer, "beta gamma delta");
        addDoc(writer, "alpha delta epsilon");
        addDoc(writer, "zeta eta theta");
        writer.commit();
      }

      try (IndexReader reader = DirectoryReader.open(dir)) {
        LeafReader leaf = reader.leaves().get(0).reader();

        DocGraphBuilder builder = new DocGraphBuilder("content", 2);
        SparseEdgeGraph graph = builder.build(leaf);

        assertEquals("Graph should contain one node per doc", leaf.maxDoc(), graph.size());

        for (int docID = 0; docID < leaf.maxDoc(); docID++) {
          int[] neighbors = graph.getNeighbors(docID);
          float[] weights = graph.getWeights(docID);

          assertEquals(neighbors.length, weights.length);
          for (int i = 0; i < neighbors.length; i++) {
            assertNotEquals("Self-loops are not expected", docID, neighbors[i]);
            assertTrue("Edge weight should be positive", weights[i] > 0);
          }
        }
      }
    }
  }

  private void addDoc(IndexWriter writer, String content) throws IOException {
    Document doc = new Document();

    FieldType tvType = new FieldType(TextField.TYPE_STORED);
    tvType.setStoreTermVectors(true);
    tvType.setStoreTermVectorPositions(true);
    tvType.setStoreTermVectorOffsets(true);
    tvType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);

    Field field = new Field("content", content, tvType);
    doc.add(field);
    writer.addDocument(doc);
  }
}
