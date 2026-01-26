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
package org.apache.lucene.codecs.spann;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopKnnCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.VectorUtil;

public class TestLucene99SpannVectorsFormat extends LuceneTestCase {

  public void testDefaultName() {
    assertEquals("Lucene99SpannVectors", Lucene99SpannVectorsFormat.FORMAT_NAME);
  }

  public void testMaxDimensions() {
    Lucene99SpannVectorsFormat format = new Lucene99SpannVectorsFormat();
    assertEquals(1024, format.getMaxDimensions("any_field"));
  }

  public void testToString() {
    Lucene99SpannVectorsFormat format = new Lucene99SpannVectorsFormat(20, 200, 4096);
    assertEquals(
        "Lucene99SpannVectorsFormat(name=Lucene99SpannVectors, nprobe=20, numPartitions=200, clusteringSample=4096)",
        format.toString());
  }

  public void testIntegration() throws Exception {
    try (Directory dir = newDirectory()) {
      Codec codec =
          new Lucene104Codec() {
            @Override
            public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
              return new Lucene99SpannVectorsFormat();
            }
          };

      IndexWriterConfig iwc = newIndexWriterConfig().setCodec(codec);
      try (IndexWriter writer = new IndexWriter(dir, iwc)) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("vec", new float[] {1f, 2f, 3f}));
        writer.addDocument(doc);
        writer.commit();
      }

      try (IndexReader reader = DirectoryReader.open(dir)) {
        assertEquals(1, reader.numDocs());
        LeafReader leaf = reader.leaves().get(0).reader();
        leaf.checkIntegrity();
      }
    }
  }

  public void testNProbeImpact() throws Exception {
    int dim = 16;
    int numDocs = 1000;
    try (Directory dir = newDirectory()) {
      // Indexing with fixed parameters
      IndexWriterConfig iwc =
          newIndexWriterConfig()
              .setCodec(
                  new Lucene104Codec() {
                    @Override
                    public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                      return new Lucene99SpannVectorsFormat(10, 20, 256);
                    }
                  })
              .setUseCompoundFile(false);

      float[][] vectors = new float[numDocs][];
      try (IndexWriter writer = new IndexWriter(dir, iwc)) {
        for (int i = 0; i < numDocs; i++) {
          vectors[i] = new float[dim];
          for (int j = 0; j < dim; j++) {
            vectors[i][j] = random().nextFloat();
          }
          VectorUtil.l2normalize(vectors[i]);
          Document doc = new Document();
          doc.add(new KnnFloatVectorField("vec", vectors[i]));
          writer.addDocument(doc);
        }
        writer.commit();
      }

      // Search with Low N-Probe (1)
      int hitsLow = countHits(dir, vectors[0], 1);

      // Search with High N-Probe (20)
      int hitsHigh = countHits(dir, vectors[0], 20);

      // High nprobe should find at least as many
      assertTrue(
          "High nprobe (" + hitsHigh + ") should be >= Low nprobe (" + hitsLow + ")",
          hitsHigh >= hitsLow);
    }
  }

  private int countHits(Directory dir, float[] query, int nprobe) throws Exception {
    try (IndexReader reader = DirectoryReader.open(dir)) {
      LeafReader leaf = reader.leaves().get(0).reader();

      String segmentName = ((SegmentReader) leaf).getSegmentInfo().info.name;
      String segmentSuffix = "";
      for (String file : dir.listAll()) {
        if (file.startsWith(segmentName + "_") && file.endsWith(".vemf")) {
          segmentSuffix = file.substring(segmentName.length() + 1, file.length() - 5);
          break;
        }
      }

      SegmentReadState state =
          new SegmentReadState(
              ((SegmentReader) leaf).directory(),
              ((SegmentReader) leaf).getSegmentInfo().info,
              leaf.getFieldInfos(),
              IOContext.DEFAULT,
              segmentSuffix);
      KnnVectorsReader vecReader =
          new Lucene99SpannVectorsFormat(nprobe, 20, 256).fieldsReader(state);
      TopKnnCollector collector = new TopKnnCollector(10, Integer.MAX_VALUE);

      AcceptDocs acceptDocs =
          new AcceptDocs() {
            @Override
            public int cost() {
              return ((SegmentReader) leaf).maxDoc();
            }

            @Override
            public Bits bits() {
              return ((SegmentReader) leaf).getLiveDocs();
            }

            @Override
            public DocIdSetIterator iterator() throws java.io.IOException {
              return DocIdSetIterator.all(((SegmentReader) leaf).maxDoc());
            }
          };

      vecReader.search("vec", query, collector, acceptDocs);
      TopDocs docs = collector.topDocs();
      vecReader.close();
      return docs.scoreDocs.length;
    }
  }
}
