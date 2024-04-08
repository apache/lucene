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
package org.apache.lucene.sandbox.codecs.bitvectors;

import java.io.IOException;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopKnnCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.BaseIndexFileFormatTestCase;

public class TestHnswBitVectorsFormat extends BaseIndexFileFormatTestCase {
  @Override
  protected Codec getCodec() {
    return new Lucene99Codec() {
      @Override
      public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
        return new HnswBitVectorsFormat();
      }
    };
  }

  @Override
  protected void addRandomFields(Document doc) {
    doc.add(new KnnByteVectorField("v2", randomVector8(30), VectorSimilarityFunction.DOT_PRODUCT));
  }

  static byte[] randomVector8(int dim) {
    byte[] vector = new byte[dim];
    random().nextBytes(vector);
    return vector;
  }

  public void testFloatVectorFails() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      Document doc = new Document();
      doc.add(new KnnFloatVectorField("f", new float[4], VectorSimilarityFunction.DOT_PRODUCT));
      IllegalArgumentException e =
          expectThrows(IllegalArgumentException.class, () -> w.addDocument(doc));
      e.getMessage().contains("Lucene99HnswBitVectorsFormat only supports BYTE encoding");
    }
  }

  public void testIndexAndSearchBitVectors() throws IOException {
    byte[][] vectors =
        new byte[][] {
          new byte[] {(byte) 0b10101110, (byte) 0b01010111}, // 16
          new byte[] {(byte) 0b11110000, (byte) 0b00001111}, // 8
          new byte[] {(byte) 0b11001100, (byte) 0b00110011}, // 6
          new byte[] {(byte) 0b11111111, (byte) 0b00000000}, // 8
          new byte[] {(byte) 0b00000000, (byte) 0b00000000} // 8
        };
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      int id = 0;
      for (byte[] vector : vectors) {
        Document doc = new Document();
        doc.add(new KnnByteVectorField("v1", vector, VectorSimilarityFunction.DOT_PRODUCT));
        doc.add(new StringField("id", Integer.toString(id++), Field.Store.YES));
        w.addDocument(doc);
      }
      w.commit();
      w.forceMerge(1);
      try (IndexReader reader = DirectoryReader.open(w)) {
        LeafReader r = getOnlyLeafReader(reader);
        TopKnnCollector collector = new TopKnnCollector(3, Integer.MAX_VALUE);
        r.searchNearestVectors("v1", vectors[0], collector, null);
        TopDocs topDocs = collector.topDocs();
        assertEquals(3, topDocs.scoreDocs.length);

        StoredFields fields = r.storedFields();
        assertEquals("0", fields.document(topDocs.scoreDocs[0].doc).get("id"));
        assertEquals(1.0, topDocs.scoreDocs[0].score, 1e-12);
        assertEquals("2", fields.document(topDocs.scoreDocs[1].doc).get("id"));
        assertEquals(0.625, topDocs.scoreDocs[1].score, 1e-12);
        assertEquals("1", fields.document(topDocs.scoreDocs[2].doc).get("id"));
        assertEquals(0.5, topDocs.scoreDocs[2].score, 1e-12);
      }
    }
  }
}
