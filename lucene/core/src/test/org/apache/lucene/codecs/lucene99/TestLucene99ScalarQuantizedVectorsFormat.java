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

import static java.lang.String.format;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
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
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.apache.lucene.util.quantization.ScalarQuantizer;
import org.junit.Before;

public class TestLucene99ScalarQuantizedVectorsFormat extends BaseKnnVectorsFormatTestCase {

  KnnVectorsFormat format;
  Float confidenceInterval;
  int bits;

  @Before
  @Override
  public void setUp() throws Exception {
    bits = random().nextBoolean() ? 4 : 7;
    confidenceInterval = random().nextBoolean() ? random().nextFloat(0.90f, 1.0f) : null;
    if (random().nextBoolean()) {
      confidenceInterval = 0f;
    }
    format =
        new Lucene99ScalarQuantizedVectorsFormat(confidenceInterval, bits, random().nextBoolean());
    super.setUp();
  }

  @Override
  protected Codec getCodec() {
    return new Lucene99Codec() {
      @Override
      public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
        return format;
      }
    };
  }

  public void testSearch() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      Document doc = new Document();
      // randomly reuse a vector, this ensures the underlying codec doesn't rely on the array
      // reference
      doc.add(
          new KnnFloatVectorField("f", new float[] {0, 1}, VectorSimilarityFunction.DOT_PRODUCT));
      w.addDocument(doc);
      w.commit();
      try (IndexReader reader = DirectoryReader.open(w)) {
        LeafReader r = getOnlyLeafReader(reader);
        if (r instanceof CodecReader codecReader) {
          KnnVectorsReader knnVectorsReader = codecReader.getVectorReader();
          // if this search found any results it would raise NPE attempting to collect them in our
          // null collector
          knnVectorsReader.search("f", new float[] {1, 0}, null, null);
        } else {
          fail("reader is not CodecReader");
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
    if (dim % 2 == 1) {
      dim++;
    }
    List<float[]> vectors = new ArrayList<>(numVectors);
    for (int i = 0; i < numVectors; i++) {
      vectors.add(randomVector(dim));
    }
    ScalarQuantizer scalarQuantizer =
        confidenceInterval != null && confidenceInterval == 0f
            ? ScalarQuantizer.fromVectorsAutoInterval(
                new Lucene99ScalarQuantizedVectorsWriter.FloatVectorWrapper(vectors, normalize),
                similarityFunction,
                numVectors,
                (byte) bits)
            : ScalarQuantizer.fromVectors(
                new Lucene99ScalarQuantizedVectorsWriter.FloatVectorWrapper(vectors, normalize),
                confidenceInterval == null
                    ? Lucene99ScalarQuantizedVectorsFormat.calculateDefaultConfidenceInterval(dim)
                    : confidenceInterval,
                numVectors,
                (byte) bits);
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
          if (knnVectorsReader instanceof Lucene99ScalarQuantizedVectorsReader quantizedReader) {
            assertNotNull(quantizedReader.getQuantizationState("f"));
            QuantizedByteVectorValues quantizedByteVectorValues =
                quantizedReader.getQuantizedVectorValues("f");
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
            fail("reader is not Lucene99ScalarQuantizedVectorsReader");
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
            return new Lucene99ScalarQuantizedVectorsFormat(0.9f, (byte) 4, false);
          }
        };
    String expectedPattern =
        "Lucene99ScalarQuantizedVectorsFormat(name=Lucene99ScalarQuantizedVectorsFormat, confidenceInterval=0.9, bits=4, compress=false, flatVectorScorer=ScalarQuantizedVectorScorer(nonQuantizedDelegate=DefaultFlatVectorScorer()), rawVectorFormat=Lucene99FlatVectorsFormat(vectorsScorer=%s()))";
    var defaultScorer = format(Locale.ROOT, expectedPattern, "DefaultFlatVectorScorer");
    var memSegScorer =
        format(Locale.ROOT, expectedPattern, "Lucene99MemorySegmentFlatVectorsScorer");
    assertThat(customCodec.knnVectorsFormat().toString(), is(oneOf(defaultScorer, memSegScorer)));
  }

  public void testLimits() {
    expectThrows(
        IllegalArgumentException.class,
        () -> new Lucene99ScalarQuantizedVectorsFormat(1.1f, 7, false));
    expectThrows(
        IllegalArgumentException.class,
        () -> new Lucene99ScalarQuantizedVectorsFormat(null, -1, false));
    expectThrows(
        IllegalArgumentException.class,
        () -> new Lucene99ScalarQuantizedVectorsFormat(null, 5, false));
    expectThrows(
        IllegalArgumentException.class,
        () -> new Lucene99ScalarQuantizedVectorsFormat(null, 9, false));
  }

  @Override
  public void testRandomWithUpdatesAndGraph() {
    // graph not supported
  }

  @Override
  public void testSearchWithVisitedLimit() {
    // search not supported
  }
}
