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
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.SameThreadExecutorService;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.apache.lucene.util.quantization.ScalarQuantizer;

public class TestLucene99HnswScalarInt4QuantizedVectorsFormat extends BaseKnnVectorsFormatTestCase {

  @Override
  protected Codec getCodec() {
    return new Lucene99Codec() {
      @Override
      public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
        return new Lucene99HnswScalarInt4QuantizedVectorsFormat(
            Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN,
            Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH);
      }
    };
  }

  @Override
  public void testSparseVectors() throws Exception {
    int numDocs = atLeast(1000);
    int numFields = TestUtil.nextInt(random(), 1, 10);
    int[] fieldDocCounts = new int[numFields];
    double[] fieldTotals = new double[numFields];
    int[] fieldDims = new int[numFields];
    VectorSimilarityFunction[] fieldSimilarityFunctions = new VectorSimilarityFunction[numFields];
    VectorEncoding[] fieldVectorEncodings = new VectorEncoding[numFields];
    for (int i = 0; i < numFields; i++) {
      fieldDims[i] = random().nextInt(20) + 1;
      fieldSimilarityFunctions[i] = randomSimilarity();
      fieldVectorEncodings[i] = randomVectorEncoding();
    }
    try (Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir, newIndexWriterConfig())) {
      for (int i = 0; i < numDocs; i++) {
        Document doc = new Document();
        for (int field = 0; field < numFields; field++) {
          String fieldName = "int" + field;
          if (random().nextInt(100) == 17) {
            switch (fieldVectorEncodings[field]) {
              case BYTE -> {
                byte[] b = randomVector8(fieldDims[field]);
                doc.add(new KnnByteVectorField(fieldName, b, fieldSimilarityFunctions[field]));
                fieldTotals[field] += b[0];
              }
              case FLOAT32 -> {
                float[] v = randomNormalizedVector(fieldDims[field]);
                doc.add(new KnnFloatVectorField(fieldName, v, fieldSimilarityFunctions[field]));
                fieldTotals[field] += v[0];
              }
            }
            fieldDocCounts[field]++;
          }
        }
        w.addDocument(doc);
      }
      try (IndexReader r = w.getReader()) {
        for (int field = 0; field < numFields; field++) {
          int docCount = 0;
          double checksum = 0;
          String fieldName = "int" + field;
          switch (fieldVectorEncodings[field]) {
            case BYTE -> {
              for (LeafReaderContext ctx : r.leaves()) {
                ByteVectorValues byteVectorValues = ctx.reader().getByteVectorValues(fieldName);
                if (byteVectorValues != null) {
                  docCount += byteVectorValues.size();
                  while (byteVectorValues.nextDoc() != NO_MORE_DOCS) {
                    checksum += byteVectorValues.vectorValue()[0];
                  }
                }
              }
            }
            case FLOAT32 -> {
              for (LeafReaderContext ctx : r.leaves()) {
                FloatVectorValues vectorValues = ctx.reader().getFloatVectorValues(fieldName);
                if (vectorValues != null) {
                  docCount += vectorValues.size();
                  while (vectorValues.nextDoc() != NO_MORE_DOCS) {
                    checksum += vectorValues.vectorValue()[0];
                  }
                }
              }
            }
          }
          assertEquals(fieldDocCounts[field], docCount);
          // Account for quantization done when indexing fields w/BYTE encoding
          double delta = fieldVectorEncodings[field] == VectorEncoding.BYTE ? numDocs * 0.01 : 1e-5;
          assertEquals(fieldTotals[field], checksum, delta);
        }
      }
    }
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
    ScalarQuantizer scalarQuantizer =
        ScalarQuantizer.fromVectorsAutoInterval(
            new Lucene99ScalarQuantizedVectorsWriter.FloatVectorWrapper(vectors, normalize),
            similarityFunction,
            vectors.size(),
            4);
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
        if (r instanceof CodecReader codecReader) {
          KnnVectorsReader knnVectorsReader = codecReader.getVectorReader();
          if (knnVectorsReader instanceof PerFieldKnnVectorsFormat.FieldsReader fieldsReader) {
            knnVectorsReader = fieldsReader.getFieldReader("f");
          }
          if (knnVectorsReader instanceof Lucene99HnswVectorsReader hnswReader) {
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
            return new Lucene99HnswScalarInt4QuantizedVectorsFormat(10, 20, 1, null);
          }
        };
    String expectedString =
        "Lucene99HnswScalarInt4QuantizedVectorsFormat(name=Lucene99HnswScalarInt4QuantizedVectorsFormat, maxConn=10, beamWidth=20, flatVectorFormat=Lucene99ScalarQuantizedVectorsFormat(name=Lucene99ScalarQuantizedVectorsFormat, confidenceInterval=0.9, rawVectorFormat=Lucene99FlatVectorsFormat()))";
    assertEquals(expectedString, customCodec.knnVectorsFormat().toString());
  }

  public void testLimits() {
    expectThrows(
        IllegalArgumentException.class,
        () -> new Lucene99HnswScalarInt4QuantizedVectorsFormat(-1, 20));
    expectThrows(
        IllegalArgumentException.class,
        () -> new Lucene99HnswScalarInt4QuantizedVectorsFormat(0, 20));
    expectThrows(
        IllegalArgumentException.class,
        () -> new Lucene99HnswScalarInt4QuantizedVectorsFormat(20, 0));
    expectThrows(
        IllegalArgumentException.class,
        () -> new Lucene99HnswScalarInt4QuantizedVectorsFormat(20, -1));
    expectThrows(
        IllegalArgumentException.class,
        () -> new Lucene99HnswScalarInt4QuantizedVectorsFormat(512 + 1, 20));
    expectThrows(
        IllegalArgumentException.class,
        () -> new Lucene99HnswScalarInt4QuantizedVectorsFormat(20, 3201));
    expectThrows(
        IllegalArgumentException.class,
        () ->
            new Lucene99HnswScalarInt4QuantizedVectorsFormat(
                20, 100, 1, new SameThreadExecutorService()));
  }
}
