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
package org.apache.lucene.codecs.lucene912;

import static java.lang.String.format;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;

import java.io.IOException;
import java.util.Locale;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.util.quantization.BQVectorUtils;
import org.apache.lucene.util.quantization.BinaryQuantizer;

public class TestLucene912BinaryQuantizedVectorsFormat extends BaseKnnVectorsFormatTestCase {

  @Override
  protected Codec getCodec() {
    return new Lucene912Codec() {
      @Override
      public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
        return new Lucene912BinaryQuantizedVectorsFormat();
      }
    };
  }

  public void testSearch() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      Document doc = new Document();
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

  public void testToString() {
    FilterCodec customCodec =
        new FilterCodec("foo", Codec.getDefault()) {
          @Override
          public KnnVectorsFormat knnVectorsFormat() {
            return new Lucene912BinaryQuantizedVectorsFormat(90_000_000);
          }
        };
    String expectedPattern =
        "Lucene912BinaryQuantizedVectorsFormat(name=Lucene912BinaryQuantizedVectorsFormat, numVectorsPerCluster=90000000, flatVectorScorer=Lucene912BinaryFlatVectorsScorer(nonQuantizedDelegate=DefaultFlatVectorScorer()))";
    var defaultScorer = format(Locale.ROOT, expectedPattern, "DefaultFlatVectorScorer");
    var memSegScorer =
        format(Locale.ROOT, expectedPattern, "Lucene99MemorySegmentFlatVectorsScorer");
    assertThat(customCodec.knnVectorsFormat().toString(), is(oneOf(defaultScorer, memSegScorer)));
  }

  public void testLimits() {
    expectThrows(
        IllegalArgumentException.class, () -> new Lucene912BinaryQuantizedVectorsFormat(12));
  }

  @Override
  public void testRandomWithUpdatesAndGraph() {
    // graph not supported
  }

  @Override
  public void testSearchWithVisitedLimit() {
    // search not supported
  }

  public void testQuantizedVectorsWriteAndRead() throws IOException {
    int numberOfVectorsPerCluster = 100;
    String fieldName = "field";
    int numVectors = random().nextInt(99, 500);
    int dims = random().nextInt(4, 65);
    IndexWriterConfig conf =
        newIndexWriterConfig()
            .setCodec(
                new Lucene912Codec() {
                  @Override
                  public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                    return new Lucene912BinaryQuantizedVectorsFormat(
                        Lucene912BinaryQuantizedVectorsFormat.NAME, numberOfVectorsPerCluster);
                  }
                })
            .setMaxBufferedDocs(numVectors + 1)
            .setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH)
            .setMergePolicy(NoMergePolicy.INSTANCE);

    float[] vector = randomVector(dims);
    VectorSimilarityFunction similarityFunction = randomSimilarity();
    KnnFloatVectorField knnField = new KnnFloatVectorField(fieldName, vector, similarityFunction);
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, conf)) {
        for (int i = 0; i < numVectors; i++) {
          Document doc = new Document();
          knnField.setVectorValue(randomVector(dims));
          doc.add(knnField);
          w.addDocument(doc);
        }
        w.commit();

        try (IndexReader reader = DirectoryReader.open(w)) {
          LeafReader r = getOnlyLeafReader(reader);
          FloatVectorValues vectorValues = r.getFloatVectorValues(fieldName);
          assert (vectorValues.size() == numVectors);
          OffHeapBinarizedVectorValues qvectorValues =
              ((Lucene912BinaryQuantizedVectorsReader.BinarizedVectorValues) vectorValues)
                  .getQuantizedVectorValues();
          float[][] centroids = qvectorValues.getCentroids();
          assert centroids[0].length == dims;
          int expectedNumCentroids = Math.max(1, numVectors / numberOfVectorsPerCluster);
          assert centroids.length == expectedNumCentroids;

          BinaryQuantizer quantizer = new BinaryQuantizer(dims, similarityFunction);
          byte[] expectedVector = new byte[BQVectorUtils.discretize(dims, 64) / 8];
          while (vectorValues.nextDoc() != NO_MORE_DOCS) {
            float[] centroid = centroids[qvectorValues.clusterId()];
            float[] corrections =
                quantizer.quantizeForIndex(vectorValues.vectorValue(), expectedVector, centroid);
            assertArrayEquals(expectedVector, qvectorValues.vectorValue());
            assertEquals(corrections[0], qvectorValues.getOOQ(), 0.00001f);
            assertEquals(corrections[1], qvectorValues.getNormOC(), 0.00001f);
            if (corrections.length == 3) {
              assertEquals(corrections[2], qvectorValues.getODotC(), 0.00001f);
            } else {
              assertEquals(0f, qvectorValues.getODotC(), 0.00001f);
            }
          }
        }
      }
    }
  }
}