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

package org.apache.lucene.codecs.lucene99;

import java.io.IOException;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.DefaultFlatVectorScorer;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorer;

public class TestLucene99ScalarQuantizedVectorScorer extends LuceneTestCase {

  private static Codec getCodec(int bits, boolean compress) {
    return new Lucene99Codec() {
      @Override
      public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
        return new Lucene99HnswScalarQuantizedVectorsFormat(
            Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN,
            Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH,
            1,
            bits,
            compress,
            null,
            null);
      }
    };
  }

  public void testScoringCompressedInt4() throws Exception {
    vectorScoringTest(4, true);
  }

  public void testScoringUncompressedInt4() throws Exception {
    vectorScoringTest(4, false);
  }

  public void testScoringInt7() throws Exception {
    vectorScoringTest(7, random().nextBoolean());
  }

  private void vectorScoringTest(int bits, boolean compress) throws IOException {
    float[][] storedVectors = new float[10][];
    int numVectors = 10;
    int vectorDimensions = random().nextInt(10) + 4;
    if (bits == 4 && vectorDimensions % 2 == 1) {
      vectorDimensions++;
    }
    for (int i = 0; i < numVectors; i++) {
      float[] vector = new float[vectorDimensions];
      for (int j = 0; j < vectorDimensions; j++) {
        vector[j] = i + j;
      }
      VectorUtil.l2normalize(vector);
      storedVectors[i] = vector;
    }

    // create lucene directory with codec
    for (VectorSimilarityFunction similarityFunction :
        new VectorSimilarityFunction[] {
          VectorSimilarityFunction.DOT_PRODUCT,
          VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT,
          VectorSimilarityFunction.EUCLIDEAN
        }) {
      try (Directory dir = newDirectory()) {
        indexVectors(dir, storedVectors, similarityFunction, bits, compress);
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
          LeafReader leafReader = reader.leaves().get(0).reader();
          float[] vector = new float[vectorDimensions];
          for (int i = 0; i < vectorDimensions; i++) {
            vector[i] = i + 1;
          }
          VectorUtil.l2normalize(vector);
          RandomVectorScorer randomScorer =
              getRandomVectorScorer(similarityFunction, leafReader, vector);
          float[] rawScores = new float[10];
          for (int i = 0; i < 10; i++) {
            rawScores[i] = similarityFunction.compare(vector, storedVectors[i]);
          }
          for (int i = 0; i < 10; i++) {
            assertEquals(similarityFunction.toString(), rawScores[i], randomScorer.score(i), 0.05f);
          }
        }
      }
    }
  }

  private RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction function, LeafReader leafReader, float[] vector) throws IOException {
    if (leafReader instanceof CodecReader codecReader) {
      KnnVectorsReader format = codecReader.getVectorReader();
      if (format instanceof PerFieldKnnVectorsFormat.FieldsReader perFieldFormat) {
        format = perFieldFormat.getFieldReader("field");
      }
      if (format instanceof Lucene99HnswVectorsReader hnswFormat) {
        OffHeapQuantizedByteVectorValues quantizedByteVectorReader =
            (OffHeapQuantizedByteVectorValues) hnswFormat.getQuantizedVectorValues("field");
        return new Lucene99ScalarQuantizedVectorScorer(new DefaultFlatVectorScorer())
            .getRandomVectorScorer(function, quantizedByteVectorReader, vector);
      }
    }
    throw new IllegalArgumentException("Unsupported reader");
  }

  private static void indexVectors(
      Directory dir,
      float[][] vectors,
      VectorSimilarityFunction function,
      int bits,
      boolean compress)
      throws IOException {
    try (IndexWriter writer =
        new IndexWriter(dir, new IndexWriterConfig().setCodec(getCodec(bits, compress)))) {
      for (int i = 0; i < vectors.length; i++) {
        Document doc = new Document();
        if (random().nextBoolean()) {
          // index a document without a vector
          writer.addDocument(doc);
        }
        writer.addDocument(doc);
        doc.add(new KnnFloatVectorField("field", vectors[i], function));
        writer.addDocument(doc);
      }
      writer.commit();
      writer.forceMerge(1);
    }
  }
}
