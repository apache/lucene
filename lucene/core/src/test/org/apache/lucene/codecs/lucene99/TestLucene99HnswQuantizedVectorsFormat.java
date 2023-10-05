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
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.util.ScalarQuantizer;

public class TestLucene99HnswQuantizedVectorsFormat extends BaseKnnVectorsFormatTestCase {
  @Override
  protected Codec getCodec() {
    return new Lucene99Codec() {
      @Override
      public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
        return new Lucene99HnswVectorsFormat(
            Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN,
            Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH,
            new Lucene99ScalarQuantizedVectorsFormat());
      }
    };
  }

  public void testQuantizedVectorsWriteAndRead() throws Exception {
    // create lucene directory with codec
    int numVectors = 1 + random().nextInt(100);
    VectorSimilarityFunction similarityFunction = randomSimilarity();
    int dim = random().nextInt(100) + 1;
    List<float[]> vectors = new ArrayList<>(numVectors);
    for (int i = 0; i < numVectors; i++) {
      vectors.add(randomVector(dim));
    }
    float quantile = Lucene99ScalarQuantizedVectorsFormat.calculateDefaultQuantile(dim);
    ScalarQuantizer scalarQuantizer =
        ScalarQuantizer.fromVectors(
            new Lucene99ScalarQuantizedVectorsWriter.FloatVectorWrapper(vectors, false), quantile);
    float[] expectedCorrections = new float[numVectors];
    byte[][] expectedVectors = new byte[numVectors][];
    for (int i = 0; i < numVectors; i++) {
      expectedVectors[i] = new byte[dim];
      expectedCorrections[i] =
          scalarQuantizer.quantize(vectors.get(i), expectedVectors[i], similarityFunction);
    }

    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      for (int i = 0; i < numVectors; i++) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("f", vectors.get(i), similarityFunction));
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
    Lucene99Codec customCodec =
        new Lucene99Codec() {
          @Override
          public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
            return new Lucene99HnswVectorsFormat(
                10, 20, new Lucene99ScalarQuantizedVectorsFormat(0.9f));
          }
        };
    String expectedString =
        "Lucene99HnswVectorsFormat(name=Lucene99HnswVectorsFormat, maxConn=10, beamWidth=20, quantizer=Lucene99ScalarQuantizedVectorsFormat(name=Lucene99ScalarQuantizedVectorsFormat, quantile=0.9))";
    assertEquals(expectedString, customCodec.getKnnVectorsFormatForField("bogus_field").toString());
  }
}
