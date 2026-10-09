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
import org.apache.lucene.search.IndexSearcher;
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

      int hitsLow = countHits(dir, vectors[0], 1);

      int hitsHigh = countHits(dir, vectors[0], 20);

      assertTrue(
          "High nprobe (" + hitsHigh + ") should be >= Low nprobe (" + hitsLow + ")",
          hitsHigh >= hitsLow);
    }
  }

  public void testDeletedDocs() throws Exception {
    int dim = 8;
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc =
          newIndexWriterConfig()
              .setCodec(
                  new Lucene104Codec() {
                    @Override
                    public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                      return new Lucene99SpannVectorsFormat(20, 5, 256);
                    }
                  });

      try (IndexWriter writer = new IndexWriter(dir, iwc)) {
        for (int i = 0; i < 10; i++) {
          Document doc = new Document();
          doc.add(new KnnFloatVectorField("vec", new float[dim]));
          doc.add(
              new org.apache.lucene.document.StringField(
                  "id", Integer.toString(i), org.apache.lucene.document.Field.Store.YES));
          writer.addDocument(doc);
        }
        writer.commit();
        writer.deleteDocuments(new org.apache.lucene.index.Term("id", "0"));
        writer.commit();
      }

      try (IndexReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs results =
            searcher.search(
                new org.apache.lucene.search.KnnFloatVectorQuery("vec", new float[dim], 10), 10);
        for (org.apache.lucene.search.ScoreDoc sd : results.scoreDocs) {
          assertNotEquals("Deleted doc 0 should not be found", 0, sd.doc);
        }
      }
    }
  }

  public void testMixedCodecs() throws Exception {
    int dim = 8;
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc =
          newIndexWriterConfig()
              .setCodec(
                  new Lucene104Codec() {
                    @Override
                    public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                      if (field.equals("spann")) return new Lucene99SpannVectorsFormat();
                      return new org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat();
                    }
                  });

      try (IndexWriter writer = new IndexWriter(dir, iwc)) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("spann", new float[dim]));
        doc.add(new KnnFloatVectorField("hnsw", new float[dim]));
        writer.addDocument(doc);
        writer.commit();
      }

      try (IndexReader reader = DirectoryReader.open(dir)) {
        LeafReader leaf = reader.leaves().get(0).reader();
        leaf.checkIntegrity();
        IndexSearcher searcher = new IndexSearcher(reader);
        assertNotNull(
            searcher.search(
                new org.apache.lucene.search.KnnFloatVectorQuery("spann", new float[dim], 1), 1));
        assertNotNull(
            searcher.search(
                new org.apache.lucene.search.KnnFloatVectorQuery("hnsw", new float[dim], 1), 1));
      }
    }
  }

  public void testByteVectors() throws Exception {
    int dim = 8;
    int numDocs = 100;
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc =
          newIndexWriterConfig()
              .setCodec(
                  new Lucene104Codec() {
                    @Override
                    public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                      return new Lucene99SpannVectorsFormat(10, 5, 256);
                    }
                  });

      byte[][] vectors = new byte[numDocs][dim];
      try (IndexWriter writer = new IndexWriter(dir, iwc)) {
        for (int i = 0; i < numDocs; i++) {
          for (int j = 0; j < dim; j++) {
            vectors[i][j] = (byte) random().nextInt(128);
          }
          Document doc = new Document();
          doc.add(new org.apache.lucene.document.KnnByteVectorField("vec", vectors[i]));
          writer.addDocument(doc);
        }
        writer.commit();
      }

      try (IndexReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs results =
            searcher.search(
                new org.apache.lucene.search.KnnByteVectorQuery("vec", vectors[0], 1), 1);
        assertEquals(1, results.scoreDocs.length);
        assertEquals(0, results.scoreDocs[0].doc);
      }
    }
  }

  public void testAllSimilarities() throws Exception {
    for (org.apache.lucene.index.VectorSimilarityFunction sim :
        org.apache.lucene.index.VectorSimilarityFunction.values()) {
      doTestParity(sim);
    }
  }

  private void doTestParity(org.apache.lucene.index.VectorSimilarityFunction sim) throws Exception {
    int dim = 8;
    int numDocs = 50;
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc =
          newIndexWriterConfig()
              .setCodec(
                  new Lucene104Codec() {
                    @Override
                    public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                      return new Lucene99SpannVectorsFormat(20, 5, 128);
                    }
                  });

      float[][] vectors = new float[numDocs][];
      try (IndexWriter writer = new IndexWriter(dir, iwc)) {
        for (int i = 0; i < numDocs; i++) {
          vectors[i] = new float[dim];
          for (int j = 0; j < dim; j++) {
            vectors[i][j] = random().nextFloat();
          }
          if (sim == org.apache.lucene.index.VectorSimilarityFunction.COSINE) {
            VectorUtil.l2normalize(vectors[i]);
          }
          Document doc = new Document();
          doc.add(new KnnFloatVectorField("vec", vectors[i], sim));
          writer.addDocument(doc);
        }
        writer.commit();
      }

      try (IndexReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        for (int i = 0; i < 5; i++) {
          float[] query = new float[dim];
          for (int j = 0; j < dim; j++) query[j] = random().nextFloat();
          if (sim == org.apache.lucene.index.VectorSimilarityFunction.COSINE) {
            VectorUtil.l2normalize(query);
          }

          float bestScore = Float.NEGATIVE_INFINITY;
          for (int d = 0; d < numDocs; d++) {
            bestScore = Math.max(bestScore, sim.compare(query, vectors[d]));
          }

          TopDocs results =
              searcher.search(new org.apache.lucene.search.KnnFloatVectorQuery("vec", query, 1), 1);
          if (results.scoreDocs.length > 0) {
            assertEquals(
                "Score mismatch for similarity: " + sim,
                bestScore,
                results.scoreDocs[0].score,
                0.001f);
          }
        }
      }
    }
  }

  public void testParityWithExactSearch() throws Exception {
    int dim = 16;
    int numDocs = 200;
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc =
          newIndexWriterConfig()
              .setCodec(
                  new Lucene104Codec() {
                    @Override
                    public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                      return new Lucene99SpannVectorsFormat(20, 10, 256, 2);
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
          doc.add(
              new KnnFloatVectorField(
                  "vec", vectors[i], org.apache.lucene.index.VectorSimilarityFunction.COSINE));
          writer.addDocument(doc);
        }
        writer.commit();
      }

      try (IndexReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = new IndexSearcher(reader);

        for (int i = 0; i < 10; i++) {
          float[] query = new float[dim];
          for (int j = 0; j < dim; j++) {
            query[j] = random().nextFloat();
          }
          VectorUtil.l2normalize(query);

          float bestScore = Float.NEGATIVE_INFINITY;
          for (int d = 0; d < numDocs; d++) {
            float cosine = VectorUtil.cosine(query, vectors[d]);
            float score = (1 + cosine) / 2;
            if (score > bestScore) {
              bestScore = score;
            }
          }

          TopDocs spannResults =
              searcher.search(
                  new org.apache.lucene.search.KnnFloatVectorQuery("vec", query, 10), 10);

          if (spannResults.scoreDocs.length > 0) {
            float spannScore = spannResults.scoreDocs[0].score;
            assertEquals(
                "SPANN should find top-1 recall on small dataset data",
                bestScore,
                spannScore,
                0.001f);
          }
        }
      }
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

  public void testMergeRecall() throws Exception {
    int dim = 16;
    int numDocs = 400;
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc =
          newIndexWriterConfig()
              .setCodec(
                  new Lucene104Codec() {
                    @Override
                    public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                      return new Lucene99SpannVectorsFormat(20, 10, 256, 2);
                    }
                  })
              .setUseCompoundFile(false);

      float[][] vectors = new float[numDocs][];
      java.util.Random rnd = new java.util.Random(42);
      try (IndexWriter writer = new IndexWriter(dir, iwc)) {
        for (int i = 0; i < numDocs; i++) {
          vectors[i] = new float[dim];
          for (int j = 0; j < dim; j++) {
            vectors[i][j] = rnd.nextFloat();
          }
          VectorUtil.l2normalize(vectors[i]);
          Document doc = new Document();
          doc.add(
              new KnnFloatVectorField(
                  "vec", vectors[i], org.apache.lucene.index.VectorSimilarityFunction.COSINE));
          writer.addDocument(doc);
          if (i % 100 == 99) {
            writer.commit();
          }
        }
        writer.forceMerge(1);
      }

      try (IndexReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = new IndexSearcher(reader);

        int numQueries = 20;
        int successfulQueries = 0;
        float tolerance = 0.01f;

        for (int i = 0; i < numQueries; i++) {
          float[] query = new float[dim];
          for (int j = 0; j < dim; j++) {
            query[j] = rnd.nextFloat();
          }
          VectorUtil.l2normalize(query);

          float bestScore = Float.NEGATIVE_INFINITY;
          for (int d = 0; d < numDocs; d++) {
            float cosine = VectorUtil.cosine(query, vectors[d]);
            float score = (1 + cosine) / 2;
            bestScore = Math.max(bestScore, score);
          }

          TopDocs results =
              searcher.search(
                  new org.apache.lucene.search.KnnFloatVectorQuery("vec", query, 50), 1);

          if (results.scoreDocs.length > 0) {
            float actualScore = results.scoreDocs[0].score;
            if (actualScore >= bestScore - tolerance) {
              successfulQueries++;
            }
          }
        }

        assertTrue(
            "Recall after merge was too low: " + successfulQueries + "/" + numQueries,
            successfulQueries >= (numQueries * 0.9));
      }
    }
  }

  public void testDeletedDocsFilteringCentroids() throws Exception {
    int dim = 16;
    int numDocs = 50;

    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc =
          newIndexWriterConfig()
              .setCodec(
                  new Lucene104Codec() {
                    @Override
                    public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                      return new Lucene99SpannVectorsFormat(10, 1, 256);
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
          doc.add(
              new org.apache.lucene.document.StringField(
                  "id", String.valueOf(i), org.apache.lucene.document.Field.Store.NO));
          writer.addDocument(doc);
        }
        writer.commit();

        writer.deleteDocuments(new org.apache.lucene.index.Term("id", "0"));
        writer.commit();
      }

      try (IndexReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = new IndexSearcher(reader);

        TopDocs results =
            searcher.search(
                new org.apache.lucene.search.KnnFloatVectorQuery("vec", vectors[1], 10), 10);

        assertTrue("Should find results even if Doc 0 is deleted", results.scoreDocs.length > 0);
        boolean foundDoc1 = false;
        for (var sd : results.scoreDocs) {
          if (sd.doc == 0) fail("Doc 0 is deleted!");

          if (sd.doc == 1) foundDoc1 = true;
        }
        assertTrue("Doc 1 should be found", foundDoc1);
      }
    }
  }
}
