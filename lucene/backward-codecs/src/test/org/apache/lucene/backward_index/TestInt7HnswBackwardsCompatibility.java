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
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.lucene104.Lucene104HnswScalarQuantizedVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.Version;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;

public class TestInt7HnswBackwardsCompatibility extends BackwardsCompatibilityTestBase {

  static final String INDEX_NAME = "int7_hnsw";
  static final String SUFFIX = "";
  private static final Version FIRST_INT7_HNSW_VERSION = Version.fromBits(9, 10, 0);
  private static final String KNN_VECTOR_FIELD = "knn_field";
  private static final int DOC_COUNT = 30;
  private static final FieldType KNN_VECTOR_FIELD_TYPE =
      KnnFloatVectorField.createFieldType(3, VectorSimilarityFunction.COSINE);
  private static final float[] KNN_VECTOR = {0.2f, -0.1f, 0.1f};

  public TestInt7HnswBackwardsCompatibility(Version version, String pattern) {
    super(version, pattern);
  }

  @ParametersFactory(argumentFormatting = "Lucene-Version:%1$s; Pattern: %2$s")
  public static Iterable<Object[]> testVersionsFactory() throws IllegalAccessException {
    return allVersion(INDEX_NAME, SUFFIX);
  }

  protected Codec getCodec() {
    return TestUtil.alwaysKnnVectorsFormat(
        new Lucene104HnswScalarQuantizedVectorsFormat(
            Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN,
            Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH));
  }

  @Override
  protected boolean supportsVersion(Version version) {
    return version.onOrAfter(FIRST_INT7_HNSW_VERSION);
  }

  @Override
  void verifyUsesDefaultCodec(Directory dir, String name) throws IOException {
    // We don't use the default codec
  }

  public void testInt7HnswIndexAndSearch() throws Exception {
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
    TestUtil.checkIndex(directory);
  }

  @Override
  protected void createIndex(Directory dir) throws IOException {
    IndexWriterConfig conf =
        new IndexWriterConfig(new MockAnalyzer(random()))
            .setMaxBufferedDocs(10)
            .setCodec(getCodec())
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

  // #13880: make sure the BWC index really contains quantized HNSW not float32
  public void testIndexIsReallyQuantized() throws Exception {
    try (DirectoryReader reader = DirectoryReader.open(directory)) {
      for (LeafReaderContext leafContext : reader.leaves()) {
        KnnVectorsReader knnVectorsReader = ((CodecReader) leafContext.reader()).getVectorReader();

        KnnVectorsReader forField = knnVectorsReader.unwrapReaderForField(KNN_VECTOR_FIELD);
        assertNotSame(
            "Expected unwrapped field but got: " + knnVectorsReader, knnVectorsReader, forField);

        assertTrue(forField instanceof Lucene99HnswVectorsReader);

        QuantizedByteVectorValues quantized =
            ((Lucene99HnswVectorsReader) forField).getQuantizedVectorValues(KNN_VECTOR_FIELD);

        assertNotNull(
            "KnnVectorsReader should have quantized interface for field " + KNN_VECTOR_FIELD,
            quantized);
      }
    }
  }
}
