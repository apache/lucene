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
import static org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsFormat.DIRECT_MONOTONIC_BLOCK_SHIFT;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.lucene95.OrdToDocDISIReaderConfiguration;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.tests.store.BaseDirectoryWrapper;
import org.apache.lucene.tests.util.TestUtil;
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
        new Lucene99ScalarQuantizedVectorsFormat(
            confidenceInterval, bits, bits == 4 ? random().nextBoolean() : false);
    super.setUp();
  }

  @Override
  protected Codec getCodec() {
    return TestUtil.alwaysKnnVectorsFormat(format);
  }

  protected List<float[]> getRandomFloatVector(int numVectors, int dim, boolean normalize) {
    List<float[]> vectors = new ArrayList<>(numVectors);
    for (int i = 0; i < numVectors; i++) {
      float[] vec = randomVector(dim);
      if (normalize) {
        float[] copy = new float[vec.length];
        System.arraycopy(vec, 0, copy, 0, copy.length);
        VectorUtil.l2normalize(copy);
        vec = copy;
      }
      vectors.add(vec);
    }
    return vectors;
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

  @Override
  public void testRecall() {
    // ignore this test since this class always returns no results from search
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
        Lucene99ScalarQuantizedVectorsWriter.buildScalarQuantizer(
            new Lucene99ScalarQuantizedVectorsWriter.FloatVectorWrapper(vectors),
            numVectors,
            similarityFunction,
            confidenceInterval,
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
            KnnVectorValues.DocIndexIterator iter = quantizedByteVectorValues.iterator();
            for (docId = iter.nextDoc(); docId != NO_MORE_DOCS; docId = iter.nextDoc()) {
              byte[] vector = quantizedByteVectorValues.vectorValue(iter.index());
              float offset = quantizedByteVectorValues.getScoreCorrectionConstant(iter.index());
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

  public void testReadQuantizedVectorWithEmptyRawVectors() throws Exception {
    String vectorFieldName = "vec1";
    int numVectors = 1 + random().nextInt(50);
    int dim = random().nextInt(64) + 1;
    if (dim % 2 == 1) {
      dim++;
    }
    VectorSimilarityFunction similarityFunction = randomSimilarity();
    List<float[]> vectors =
        getRandomFloatVector(
            numVectors, dim, similarityFunction == VectorSimilarityFunction.COSINE);

    try (BaseDirectoryWrapper dir = newDirectory();
        IndexWriter w =
            new IndexWriter(
                dir,
                new IndexWriterConfig()
                    .setMaxBufferedDocs(numVectors + 1)
                    .setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH)
                    .setMergePolicy(NoMergePolicy.INSTANCE)
                    .setUseCompoundFile(false)
                    .setCodec(getCodec()))) {
      dir.setCheckIndexOnClose(false);

      for (int i = 0; i < numVectors; i++) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField(vectorFieldName, vectors.get(i), similarityFunction));
        w.addDocument(doc);
      }
      w.commit();

      simulateEmptyRawVectors(dir);

      try (IndexReader reader = DirectoryReader.open(w)) {
        LeafReader r = getOnlyLeafReader(reader);
        if (r instanceof CodecReader codecReader) {
          KnnVectorsReader knnVectorsReader = codecReader.getVectorReader();
          if (knnVectorsReader instanceof PerFieldKnnVectorsFormat.FieldsReader fieldsReader) {
            knnVectorsReader = fieldsReader.getFieldReader(vectorFieldName);
          }
          if (knnVectorsReader instanceof Lucene99ScalarQuantizedVectorsReader quantizedReader) {
            FloatVectorValues floatVectorValues =
                quantizedReader.getFloatVectorValues(vectorFieldName);
            if (floatVectorValues instanceof OffHeapQuantizedFloatVectorValues) {
              KnnVectorValues.DocIndexIterator iter = floatVectorValues.iterator();
              for (int docId = iter.nextDoc(); docId != NO_MORE_DOCS; docId = iter.nextDoc()) {
                float[] dequantizedVector = floatVectorValues.vectorValue(iter.index());
                for (int i = 0; i < dim; i++) {
                  assertEquals(dequantizedVector[i], vectors.get(docId)[i], 0.2f);
                }
              }
            } else {
              fail("floatVectorValues is not OffHeapQuantizedFloatVectorValues");
            }
          } else {
            System.out.println("Vector READER:: " + knnVectorsReader.toString());
            fail("reader is not Lucene99ScalarQuantizedVectorsReader");
          }
        } else {
          fail("reader is not CodecReader");
        }
      }
    }
  }

  /** Simulates empty raw vectors by modifying index files. */
  private void simulateEmptyRawVectors(Directory dir) throws Exception {
    final String[] indexFiles = dir.listAll();
    final String RAW_VECTOR_EXTENSION = "vec";
    final String VECTOR_META_EXTENSION = "vemf";

    for (String file : indexFiles) {
      if (file.endsWith("." + RAW_VECTOR_EXTENSION)) {
        replaceWithEmptyVectorFile(dir, file);
      } else if (file.endsWith("." + VECTOR_META_EXTENSION)) {
        updateVectorMetadataFile(dir, file);
      }
    }
  }

  /** Replaces a raw vector file with an empty one that has valid header/footer. */
  private void replaceWithEmptyVectorFile(Directory dir, String fileName) throws Exception {
    byte[] indexHeader;
    try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
      indexHeader = CodecUtil.readIndexHeader(in);
    }
    dir.deleteFile(fileName);
    try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
      // Write header
      out.writeBytes(indexHeader, 0, indexHeader.length);
      // Write footer (no content in between)
      CodecUtil.writeFooter(out);
    }
  }

  /** Updates vector metadata file to indicate zero vector length. */
  private void updateVectorMetadataFile(Directory dir, String fileName) throws Exception {
    // Read original metadata
    byte[] indexHeader;
    int fieldNumber, vectorEncoding, vectorSimilarityFunction, dimension;
    long vectorStartPos;

    try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
      indexHeader = CodecUtil.readIndexHeader(in);
      fieldNumber = in.readInt();
      vectorEncoding = in.readInt();
      vectorSimilarityFunction = in.readInt();
      vectorStartPos = in.readVLong();
      in.readVLong(); // Skip original vector length
      dimension = in.readVInt();
    }

    // Create updated metadata file
    dir.deleteFile(fileName);
    try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
      // Write header
      out.writeBytes(indexHeader, 0, indexHeader.length);

      // Write metadata with zero vector length
      out.writeInt(fieldNumber);
      out.writeInt(vectorEncoding);
      out.writeInt(vectorSimilarityFunction);
      out.writeVLong(vectorStartPos);
      out.writeVLong(0); // Set vector length to 0
      out.writeVInt(dimension);
      out.writeInt(0);

      // Write configuration
      OrdToDocDISIReaderConfiguration.writeStoredMeta(
          DIRECT_MONOTONIC_BLOCK_SHIFT, out, null, 0, 0, null);

      // Mark end of fields and write footer
      out.writeInt(-1);
      CodecUtil.writeFooter(out);
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
