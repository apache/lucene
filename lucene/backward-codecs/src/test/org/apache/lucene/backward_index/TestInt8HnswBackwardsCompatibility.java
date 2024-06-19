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
package org.apache.lucene.backward_index;

import static org.apache.lucene.backward_index.TestBasicBackwardsCompatibility.assertKNNSearch;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import java.io.IOException;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99Codec;
import org.apache.lucene.codecs.lucene99.Lucene99HnswScalarQuantizedVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.Version;

public class TestInt8HnswBackwardsCompatibility extends BackwardsCompatibilityTestBase {

  static final String INDEX_NAME = "int8_hnsw";
  static final String SUFFIX = "";
  private static final Version FIRST_INT8_HNSW_VERSION = Version.LUCENE_9_10_0;
  private static final String KNN_VECTOR_FIELD = "knn_field";
  private static final int DOC_COUNT = 30;
  private static final FieldType KNN_VECTOR_FIELD_TYPE =
      KnnFloatVectorField.createFieldType(3, VectorSimilarityFunction.COSINE);
  private static final float[] KNN_VECTOR = {0.2f, -0.1f, 0.1f};

  public TestInt8HnswBackwardsCompatibility(Version version, String pattern) {
    super(version, pattern);
  }

  /** Provides all sorted versions to the test-framework */
  @ParametersFactory(argumentFormatting = "Lucene-Version:%1$s; Pattern: %2$s")
  public static Iterable<Object[]> testVersionsFactory() throws IllegalAccessException {
    return allVersion(INDEX_NAME, SUFFIX);
  }

  protected Codec getCodec() {
    return new Lucene99Codec() {
      @Override
      public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
        return new Lucene99HnswScalarQuantizedVectorsFormat(
            Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN,
            Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH);
      }
    };
  }

  @Override
  protected boolean supportsVersion(Version version) {
    return version.onOrAfter(FIRST_INT8_HNSW_VERSION);
  }

  @Override
  void verifyUsesDefaultCodec(Directory dir, String name) throws IOException {
    // We don't use the default codec
  }

  public void testInt8HnswIndexAndSearch() throws Exception {
    IndexWriterConfig indexWriterConfig =
        newIndexWriterConfig(new MockAnalyzer(random()))
            .setOpenMode(IndexWriterConfig.OpenMode.APPEND)
            .setCodec(getCodec())
            .setMergePolicy(newLogMergePolicy());
    try (IndexWriter writer = new IndexWriter(directory, indexWriterConfig)) {
      // add 10 docs
      for (int i = 0; i < 10; i++) {
        writer.addDocument(knnDocument(i + DOC_COUNT));
        if (random().nextBoolean()) {
          writer.flush();
        }
      }
      if (random().nextBoolean()) {
        writer.forceMerge(1);
      }
      writer.commit();
      try (IndexReader reader = DirectoryReader.open(directory)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        assertKNNSearch(searcher, KNN_VECTOR, 1000, DOC_COUNT + 10, "0");
        assertKNNSearch(searcher, KNN_VECTOR, 10, 10, "0");
      }
    }
    // This will confirm the docs are really sorted
    TestUtil.checkIndex(directory);
  }

  @Override
  protected void createIndex(Directory dir) throws IOException {
    IndexWriterConfig conf =
        new IndexWriterConfig(new MockAnalyzer(random()))
            .setMaxBufferedDocs(10)
            .setCodec(TestUtil.getDefaultCodec())
            .setMergePolicy(NoMergePolicy.INSTANCE);
    try (IndexWriter writer = new IndexWriter(dir, conf)) {
      for (int i = 0; i < DOC_COUNT; i++) {
        writer.addDocument(knnDocument(i));
      }
      writer.forceMerge(1);
    }
    try (DirectoryReader reader = DirectoryReader.open(dir)) {
      IndexSearcher searcher = new IndexSearcher(reader);
      assertKNNSearch(searcher, KNN_VECTOR, 1000, DOC_COUNT, "0");
      assertKNNSearch(searcher, KNN_VECTOR, 10, 10, "0");
    }
  }

  private static Document knnDocument(int id) {
    Document doc = new Document();
    float[] vector = {KNN_VECTOR[0], KNN_VECTOR[1], KNN_VECTOR[2] + 0.1f * id};
    doc.add(new KnnFloatVectorField(KNN_VECTOR_FIELD, vector, KNN_VECTOR_FIELD_TYPE));
    doc.add(new StringField("id", Integer.toString(id), Field.Store.YES));
    return doc;
  }

  public void testReadOldIndices() throws Exception {
    try (DirectoryReader reader = DirectoryReader.open(directory)) {
      IndexSearcher searcher = new IndexSearcher(reader);
      assertKNNSearch(searcher, KNN_VECTOR, 1000, DOC_COUNT, "0");
      assertKNNSearch(searcher, KNN_VECTOR, 10, 10, "0");
    }
  }
}
