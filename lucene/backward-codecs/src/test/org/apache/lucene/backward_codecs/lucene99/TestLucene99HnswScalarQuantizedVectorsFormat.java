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

package org.apache.lucene.backward_codecs.lucene99;

import static org.apache.lucene.index.VectorSimilarityFunction.DOT_PRODUCT;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.codecs.Codec;
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
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;

public class TestLucene99HnswScalarQuantizedVectorsFormat extends BaseKnnVectorsFormatTestCase {
  @Override
  protected Codec getCodec() {
    return TestUtil.alwaysKnnVectorsFormat(new Lucene99RWV0HnswScalarQuantizationVectorsFormat());
  }

  public void testSimpleOffHeapSize() throws IOException {
    float[] vector = randomVector(random().nextInt(12, 500));
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      Document doc = new Document();
      doc.add(new KnnFloatVectorField("f", vector, DOT_PRODUCT));
      w.addDocument(doc);
      w.commit();
      try (IndexReader reader = DirectoryReader.open(w)) {
        LeafReader r = getOnlyLeafReader(reader);
        if (r instanceof CodecReader codecReader) {
          KnnVectorsReader knnVectorsReader = codecReader.getVectorReader();
          knnVectorsReader = knnVectorsReader.unwrapReaderForField("f");
          var fieldInfo = r.getFieldInfos().fieldInfo("f");
          var offHeap = knnVectorsReader.getOffHeapByteSize(fieldInfo);
          assertEquals(vector.length * Float.BYTES, (long) offHeap.get("vec"));
          assertEquals(1L, (long) offHeap.get("vex"));
          long corrections = Float.BYTES;
          long expected = fieldInfo.getVectorDimension() + corrections;
          assertEquals(expected, (long) offHeap.get("veq"));
          assertEquals(3, offHeap.size());
        }
      }
    }
  }

  @Override
  protected boolean supportsFloatVectorFallback() {
    return false;
  }

  @Override
  protected VectorEncoding randomVectorEncoding() {
    return random().nextBoolean() ? VectorEncoding.BYTE : VectorEncoding.FLOAT32;
  }

  /**
   * A merge instance reaches the raw vectors reader, which opens the raw vectors again for the
   * merge, and finishing the merge closes them.
   */
  public void testMergeInstanceOpensTheRawVectors() throws IOException {
    List<AtomicBoolean> mergeOpens = new CopyOnWriteArrayList<>();
    AtomicInteger mergeSlices = new AtomicInteger();
    try (Directory dir =
        new FilterDirectory(newDirectory()) {
          @Override
          public IndexInput openInput(String name, IOContext context) throws IOException {
            IndexInput in = super.openInput(name, context);
            if (name.endsWith(".vec") == false || context.context() != IOContext.Context.MERGE) {
              return in;
            }
            AtomicBoolean closed = new AtomicBoolean();
            mergeOpens.add(closed);
            return new FilterIndexInput(name, in) {
              @Override
              public IndexInput slice(String sliceDescription, long offset, long length)
                  throws IOException {
                mergeSlices.incrementAndGet(); // the vectors are read through slices
                return super.slice(sliceDescription, offset, length);
              }

              @Override
              public void close() throws IOException {
                closed.set(true);
                super.close();
              }
            };
          }
        }) {
      int dims = random().nextInt(4, 65);
      try (IndexWriter w =
          new IndexWriter(
              dir, new IndexWriterConfig().setCodec(getCodec()).setUseCompoundFile(false))) {
        for (int i = 0; i < 16; i++) {
          Document doc = new Document();
          doc.add(
              new KnnFloatVectorField("f", randomVector(dims), VectorSimilarityFunction.EUCLIDEAN));
          w.addDocument(doc);
        }
        w.commit();
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        KnnVectorsReader vectors =
            ((CodecReader) getOnlyLeafReader(reader)).getVectorReader().unwrapReaderForField("f");
        assertEquals(List.of(), mergeOpens);
        KnnVectorsReader merging = vectors.getMergeInstance();
        assertEquals("the raw vectors were not opened for the merge", 1, mergeOpens.size());

        FloatVectorValues actual = merging.getFloatVectorValues("f");
        assertTrue("the merge instance did not read what was opened for it", mergeSlices.get() > 0);
        FloatVectorValues expected = vectors.getFloatVectorValues("f");
        assertEquals(expected.size(), actual.size());
        for (int ord = 0; ord < expected.size(); ord++) {
          assertArrayEquals(expected.vectorValue(ord), actual.vectorValue(ord), 0f);
        }

        merging.finishMerge();
        assertTrue("finishing the merge did not close them", mergeOpens.get(0).get());
      }
    }
  }
}
