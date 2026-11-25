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
package org.apache.lucene.codecs.lucene104;

import java.io.IOException;
import org.apache.lucene.codecs.lucene95.OrdToDocDISIReaderConfiguration;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.quantization.OptimizedScalarQuantizer.QuantizationResult;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues.ScalarEncoding;

/**
 * The off-heap scalar-quantized values cache the record of the last ordinal read by {@code
 * vectorValue()}. Reading the corrective terms of another ordinal must not overwrite that cache.
 */
public class TestOffHeapScalarQuantizedVectorValues extends LuceneTestCase {

  private static final int DIMS = 4;
  private static final int NUM_VECTORS = 2;
  private static final ScalarEncoding ENCODING = ScalarEncoding.UNSIGNED_BYTE;
  private static final VectorSimilarityFunction SIMILARITY = VectorSimilarityFunction.DOT_PRODUCT;

  public void testQuantizedValuesKeepCachedCorrectiveTerms() throws IOException {
    try (Directory dir = newDirectory();
        IndexInput data = writeRecords(dir)) {
      // the quantizer and the scorer are not used by the methods under test
      OffHeapScalarQuantizedVectorValues values =
          new OffHeapScalarQuantizedVectorValues.DenseOffHeapVectorValues(
              DIMS, NUM_VECTORS, new float[DIMS], 0f, null, ENCODING, SIMILARITY, null, data);
      values.vectorValue(0);
      assertEquals(corrections(1), values.getCorrectiveTerms(1));
      assertEquals(corrections(0), values.getCorrectiveTerms(0));
    }
  }

  public void testFloatValuesKeepCachedCorrectiveTerms() throws IOException {
    try (Directory dir = newDirectory();
        IndexInput data = writeRecords(dir)) {
      OffHeapScalarQuantizedFloatVectorValues values =
          OffHeapScalarQuantizedFloatVectorValues.load(
              denseConfiguration(dir),
              DIMS,
              NUM_VECTORS,
              ENCODING,
              SIMILARITY,
              null,
              new float[DIMS],
              0,
              data.length(),
              data);
      values.vectorValue(0);
      assertEquals(corrections(1), values.getCorrectiveTerms(1));
      assertEquals(corrections(0), values.getCorrectiveTerms(0));
    }
  }

  public void testFloat16ValuesKeepCachedCorrectiveTerms() throws IOException {
    try (Directory dir = newDirectory();
        IndexInput data = writeRecords(dir)) {
      OffHeapScalarQuantizedFloat16VectorValues values =
          OffHeapScalarQuantizedFloat16VectorValues.load(
              denseConfiguration(dir),
              DIMS,
              NUM_VECTORS,
              ENCODING,
              SIMILARITY,
              null,
              new float[DIMS],
              0,
              data.length(),
              data);
      values.vectorValue(0);
      assertEquals(corrections(1), values.getCorrectiveTerms(1));
      assertEquals(corrections(0), values.getCorrectiveTerms(0));
    }
  }

  /** Distinct corrective terms for each ordinal. */
  private static QuantizationResult corrections(int ord) {
    return new QuantizationResult(ord + 1, 10 * (ord + 1), 100 * (ord + 1), 1000 * (ord + 1));
  }

  /** Writes one record per ordinal: the packed vector, then its corrective terms. */
  private static IndexInput writeRecords(Directory dir) throws IOException {
    try (IndexOutput out = dir.createOutput("vectors", IOContext.DEFAULT)) {
      for (int ord = 0; ord < NUM_VECTORS; ord++) {
        for (int i = 0; i < ENCODING.getDocPackedLength(DIMS); i++) {
          out.writeByte((byte) (ord + 1));
        }
        QuantizationResult c = corrections(ord);
        out.writeInt(Float.floatToIntBits(c.lowerInterval()));
        out.writeInt(Float.floatToIntBits(c.upperInterval()));
        out.writeInt(Float.floatToIntBits(c.additionalCorrection()));
        out.writeInt(c.quantizedComponentSum());
      }
    }
    return dir.openInput("vectors", IOContext.DEFAULT);
  }

  /** A configuration for a field where every document has a vector. */
  private static OrdToDocDISIReaderConfiguration denseConfiguration(Directory dir)
      throws IOException {
    try (IndexOutput meta = dir.createOutput("meta", IOContext.DEFAULT)) {
      OrdToDocDISIReaderConfiguration.writeStoredMeta(
          16, meta, null, NUM_VECTORS, NUM_VECTORS, null);
    }
    try (IndexInput meta = dir.openInput("meta", IOContext.DEFAULT)) {
      return OrdToDocDISIReaderConfiguration.fromStoredMeta(meta, NUM_VECTORS);
    }
  }
}
