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
package org.apache.lucene.document;

import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;
import java.nio.file.Path;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.tests.codecs.vector.ConfigurableMCodec;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.LuceneTestCase.Monster;
import org.junit.BeforeClass;

@TimeoutSuite(millis = 86_400_000) // 24 hour timeout
@Monster("takes ~10 minutes and needs extra heap, disk space, file handles")
public class TestManyKnnDocs extends LuceneTestCase {
  // gradlew -p lucene/core test --tests TestManyKnnDocs -Ptests.heapsize=16g -Dtests.monster=true

  private static Path testDir;

  @BeforeClass
  public static void init_index() throws Exception {
    IndexWriterConfig iwc = new IndexWriterConfig();
    iwc.setCodec(
        new ConfigurableMCodec(
            128)); // Make sure to use the ConfigurableMCodec instead of a random one
    iwc.setRAMBufferSizeMB(64); // Use a 64MB buffer to create larger initial segments
    TieredMergePolicy mp = new TieredMergePolicy();
    mp.setMaxMergeAtOnce(256); // avoid intermediate merges (waste of time with HNSW?)
    mp.setSegmentsPerTier(256); // only merge once at the end when we ask
    iwc.setMergePolicy(mp);
    VectorSimilarityFunction similarityFunction = VectorSimilarityFunction.DOT_PRODUCT;

    try (Directory dir = FSDirectory.open(testDir = createTempDir("ManyKnnVectorDocs"));
        IndexWriter iw = new IndexWriter(dir, iwc)) {

      int numVectors = 2088992;
      for (int i = 0; i < numVectors; i++) {
        float[] vector = new float[1];
        Document doc = new Document();
        vector[0] = (i % 256);
        doc.add(new KnnFloatVectorField("field", vector, similarityFunction));
        doc.add(new KeywordField("int", "" + i, org.apache.lucene.document.Field.Store.YES));
        doc.add(new StoredField("intValue", i));
        iw.addDocument(doc);
      }

      // merge to single segment and then verify
      iw.forceMerge(1);
      iw.commit();
    }
  }

  public void testLargeSegmentKnn() throws Exception {
    try (Directory dir = FSDirectory.open(testDir)) {
      IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(dir));
      for (int i = 0; i < 256; i++) {
        Query filterQuery = new MatchAllDocsQuery();
        float[] vector = new float[128];
        vector[0] = i;
        vector[1] = 1;
        TopDocs docs =
            searcher.search(new KnnFloatVectorQuery("field", vector, 10, filterQuery), 5);
        assertEquals(5, docs.scoreDocs.length);
        Document d = searcher.storedFields().document(docs.scoreDocs[0].doc);
        String s = "";
        for (int j = 0; j < docs.scoreDocs.length - 1; j++) {
          s += docs.scoreDocs[j].doc + " " + docs.scoreDocs[j].score + "\n";
        }
        assertEquals(s, i + 256, d.getField("intValue").numericValue());
      }
    }
  }

  public void testLargeSegmentSeededExact() throws Exception {
    try (Directory dir = FSDirectory.open(testDir)) {
      IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(dir));
      for (int i = 0; i < 256; i++) {
        Query seedQuery = KeywordField.newExactQuery("int", "" + (i + 256));
        Query filterQuery = new MatchAllDocsQuery();
        float[] vector = new float[128];
        vector[0] = i;
        vector[1] = 1;
        TopDocs docs =
            searcher.search(
                new KnnFloatVectorQuery("field", vector, 10, filterQuery, seedQuery), 5);
        assertEquals(5, docs.scoreDocs.length);
        String s = "";
        for (int j = 0; j < docs.scoreDocs.length - 1; j++) {
          s += docs.scoreDocs[j].doc + " " + docs.scoreDocs[j].score + "\n";
        }
        Document d = searcher.storedFields().document(docs.scoreDocs[0].doc);
        assertEquals(s, i + 256, d.getField("intValue").numericValue());
      }
    }
  }

  public void testLargeSegmentSeededNearby() throws Exception {
    try (Directory dir = FSDirectory.open(testDir)) {
      IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(dir));
      for (int i = 0; i < 256; i++) {
        Query seedQuery = KeywordField.newExactQuery("int", "" + i);
        Query filterQuery = new MatchAllDocsQuery();
        float[] vector = new float[128];
        vector[0] = i;
        vector[1] = 1;
        TopDocs docs =
            searcher.search(
                new KnnFloatVectorQuery("field", vector, 10, filterQuery, seedQuery), 5);
        assertEquals(5, docs.scoreDocs.length);
        String s = "";
        for (int j = 0; j < docs.scoreDocs.length - 1; j++) {
          s += docs.scoreDocs[j].doc + " " + docs.scoreDocs[j].score + "\n";
        }
        Document d = searcher.storedFields().document(docs.scoreDocs[0].doc);
        assertEquals(s, i + 256, d.getField("intValue").numericValue());
      }
    }
  }

  public void testLargeSegmentSeededDistant() throws Exception {
    try (Directory dir = FSDirectory.open(testDir)) {
      IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(dir));
      for (int i = 0; i < 256; i++) {
        Query seedQuery = KeywordField.newExactQuery("int", "" + (i + 128));
        Query filterQuery = new MatchAllDocsQuery();
        float[] vector = new float[128];
        vector[0] = i;
        vector[1] = 1;
        TopDocs docs =
            searcher.search(
                new KnnFloatVectorQuery("field", vector, 10, filterQuery, seedQuery), 5);
        assertEquals(5, docs.scoreDocs.length);
        Document d = searcher.storedFields().document(docs.scoreDocs[0].doc);
        String s = "";
        for (int j = 0; j < docs.scoreDocs.length - 1; j++) {
          s += docs.scoreDocs[j].doc + " " + docs.scoreDocs[j].score + "\n";
        }
        assertEquals(s, i + 256, d.getField("intValue").numericValue());
      }
    }
  }

  public void testLargeSegmentSeededNone() throws Exception {
    try (Directory dir = FSDirectory.open(testDir)) {
      IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(dir));
      for (int i = 0; i < 256; i++) {
        Query seedQuery = new MatchNoDocsQuery();
        Query filterQuery = new MatchAllDocsQuery();
        float[] vector = new float[128];
        vector[0] = i;
        vector[1] = 1;
        TopDocs docs =
            searcher.search(
                new KnnFloatVectorQuery("field", vector, 10, filterQuery, seedQuery), 5);
        assertEquals(5, docs.scoreDocs.length);
        Document d = searcher.storedFields().document(docs.scoreDocs[0].doc);
        String s = "";
        for (int j = 0; j < docs.scoreDocs.length - 1; j++) {
          s += docs.scoreDocs[j].doc + " " + docs.scoreDocs[j].score + "\n";
        }
        assertEquals(s, i + 256, d.getField("intValue").numericValue());
      }
    }
  }
}
