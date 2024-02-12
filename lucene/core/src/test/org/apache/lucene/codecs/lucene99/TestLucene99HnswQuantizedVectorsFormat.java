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

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.util.SameThreadExecutorService;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.apache.lucene.util.quantization.ScalarQuantizer;

public class TestLucene99HnswQuantizedVectorsFormat extends BaseKnnVectorsFormatTestCase {

  @Override
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

  public void testQuantizedVectorsWriteAndRead() throws Exception {
    // create lucene directory with codec
    int numVectors = 1 + random().nextInt(50);
    VectorSimilarityFunction similarityFunction = randomSimilarity();
    boolean normalize = similarityFunction == VectorSimilarityFunction.COSINE;
    int dim = random().nextInt(64) + 1;
    List<float[]> vectors = new ArrayList<>(numVectors);
    for (int i = 0; i < numVectors; i++) {
      vectors.add(randomVector(dim));
    }
    float confidenceInterval =
        Lucene99ScalarQuantizedVectorsFormat.calculateDefaultConfidenceInterval(dim);
    ScalarQuantizer scalarQuantizer =
        ScalarQuantizer.fromVectors(
            new Lucene99ScalarQuantizedVectorsWriter.FloatVectorWrapper(vectors, normalize),
            confidenceInterval,
            numVectors);
    float[] expectedCorrections = new float[numVectors];
    byte[][] expectedVectors = new byte[numVectors][];
    for (int i = 0; i < numVectors; i++) {
      float[] vector = vectors.get(i);
      if (normalize) {
        float[] copy = new float[vector.length];
        System.arraycopy(vector, 0, copy, 0, copy.length);
        VectorUtil.l2normalize(copy);
        vector = copy;
      }

      expectedVectors[i] = new byte[dim];
      expectedCorrections[i] =
          scalarQuantizer.quantize(vector, expectedVectors[i], similarityFunction);
    }
    float[] randomlyReusedVector = new float[dim];

    try (Directory dir = newDirectory();
        IndexWriter w =
            new IndexWriter(
                dir,
                new IndexWriterConfig()
                    .setMaxBufferedDocs(numVectors + 1)
                    .setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH)
                    .setMergePolicy(NoMergePolicy.INSTANCE))) {
      for (int i = 0; i < numVectors; i++) {
        Document doc = new Document();
        // randomly reuse a vector, this ensures the underlying codec doesn't rely on the array
        // reference
        final float[] v;
        if (random().nextBoolean()) {
          System.arraycopy(vectors.get(i), 0, randomlyReusedVector, 0, dim);
          v = randomlyReusedVector;
        } else {
          v = vectors.get(i);
        }
        doc.add(new KnnFloatVectorField("f", v, similarityFunction));
        w.addDocument(doc);
      }
      w.commit();
      try (IndexReader reader = DirectoryReader.open(w)) {
        LeafReader r = getOnlyLeafReader(reader);
        if (r instanceof CodecReader) {
          KnnVectorsReader knnVectorsReader = ((CodecReader) r).getVectorReader();
          if (knnVectorsReader instanceof PerFieldKnnVectorsFormat.FieldsReader) {
            knnVectorsReader =
                ((PerFieldKnnVectorsFormat.FieldsReader) knnVectorsReader).getFieldReader("f");
          }
          if (knnVectorsReader instanceof Lucene99HnswVectorsReader) {
            Lucene99HnswVectorsReader hnswReader = (Lucene99HnswVectorsReader) knnVectorsReader;
            assertNotNull(hnswReader.getQuantizationState("f"));
            QuantizedByteVectorValues quantizedByteVectorValues =
                hnswReader.getQuantizedVectorValues("f");
            int docId = -1;
            while ((docId = quantizedByteVectorValues.nextDoc()) != NO_MORE_DOCS) {
              byte[] vector = quantizedByteVectorValues.vectorValue();
              float offset = quantizedByteVectorValues.getScoreCorrectionConstant();
              for (int i = 0; i < dim; i++) {
                assertEquals(vector[i], expectedVectors[docId][i]);
              }
              assertEquals(offset, expectedCorrections[docId], 0.00001f);
            }
          } else {
            fail("reader is not Lucene99HnswVectorsReader");
          }
        } else {
          fail("reader is not CodecReader");
        }
      }
    }
  }

  public void testToString() {
    FilterCodec customCodec =
        new FilterCodec("foo", Codec.getDefault()) {
          @Override
          public KnnVectorsFormat knnVectorsFormat() {
            return new Lucene99HnswScalarQuantizedVectorsFormat(10, 20, 1, 0.9f, null);
          }
        };
    String expectedString =
        "Lucene99HnswScalarQuantizedVectorsFormat(name=Lucene99HnswScalarQuantizedVectorsFormat, maxConn=10, beamWidth=20, flatVectorFormat=Lucene99ScalarQuantizedVectorsFormat(name=Lucene99ScalarQuantizedVectorsFormat, confidenceInterval=0.9, rawVectorFormat=Lucene99FlatVectorsFormat()))";
    assertEquals(expectedString, customCodec.knnVectorsFormat().toString());
  }

  public void testLimits() {
    expectThrows(
        IllegalArgumentException.class, () -> new Lucene99HnswScalarQuantizedVectorsFormat(-1, 20));
    expectThrows(
        IllegalArgumentException.class, () -> new Lucene99HnswScalarQuantizedVectorsFormat(0, 20));
    expectThrows(
        IllegalArgumentException.class, () -> new Lucene99HnswScalarQuantizedVectorsFormat(20, 0));
    expectThrows(
        IllegalArgumentException.class, () -> new Lucene99HnswScalarQuantizedVectorsFormat(20, -1));
    expectThrows(
        IllegalArgumentException.class,
        () -> new Lucene99HnswScalarQuantizedVectorsFormat(512 + 1, 20));
    expectThrows(
        IllegalArgumentException.class,
        () -> new Lucene99HnswScalarQuantizedVectorsFormat(20, 3201));
    expectThrows(
        IllegalArgumentException.class,
        () -> new Lucene99HnswScalarQuantizedVectorsFormat(20, 100, 0, 1.1f, null));
    expectThrows(
        IllegalArgumentException.class,
        () -> new Lucene99HnswScalarQuantizedVectorsFormat(20, 100, 0, 0.8f, null));
    expectThrows(
        IllegalArgumentException.class,
        () -> new Lucene99HnswScalarQuantizedVectorsFormat(20, 100, 100, null, null));
    expectThrows(
        IllegalArgumentException.class,
        () ->
            new Lucene99HnswScalarQuantizedVectorsFormat(
                20, 100, 1, null, new SameThreadExecutorService()));
  }
}
