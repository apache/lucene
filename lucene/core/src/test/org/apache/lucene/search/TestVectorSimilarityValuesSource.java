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

package org.apache.lucene.search;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestVectorSimilarityValuesSource extends LuceneTestCase {
  private static Directory dir;
  private static Analyzer analyzer;
  private static IndexReader reader;
  private static IndexSearcher searcher;

  @BeforeClass
  public static void beforeClass() throws Exception {
    dir = newDirectory();
    analyzer = new MockAnalyzer(random());
    IndexWriterConfig iwConfig = newIndexWriterConfig(analyzer);
    iwConfig.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwConfig);

    Document document = new Document();
    document.add(new StringField("id", "1", Field.Store.NO));
    document.add(new SortedDocValuesField("id", new BytesRef("1")));
    document.add(new KnnFloatVectorField("knnFloatField1", new float[] {1.f, 2.f, 3.f}));
    document.add(
        new KnnFloatVectorField(
            "knnFloatField2",
            new float[] {2.2f, -3.2f, -3.1f},
            VectorSimilarityFunction.DOT_PRODUCT));
    document.add(
        new KnnFloatVectorField(
            "knnFloatField3", new float[] {4.5f, 10.3f, -7.f}, VectorSimilarityFunction.COSINE));
    document.add(
        new KnnFloatVectorField(
            "knnFloatField4",
            new float[] {-1.3f, 1.0f, 1.0f},
            VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT));
    document.add(new KnnFloatVectorField("knnFloatField5", new float[] {-6.7f, -1.0f, -0.9f}));
    document.add(new KnnByteVectorField("knnByteField1", new byte[] {106, 80, 127}));
    document.add(
        new KnnByteVectorField(
            "knnByteField2", new byte[] {4, 2, 3}, VectorSimilarityFunction.DOT_PRODUCT));
    document.add(
        new KnnByteVectorField(
            "knnByteField3", new byte[] {-121, -64, -1}, VectorSimilarityFunction.COSINE));
    document.add(
        new KnnByteVectorField(
            "knnByteField4",
            new byte[] {-127, 127, 127},
            VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT));
    iw.addDocument(document);

    Document document2 = new Document();
    document2.add(new StringField("id", "2", Field.Store.NO));
    document2.add(new SortedDocValuesField("id", new BytesRef("2")));
    document2.add(new KnnFloatVectorField("knnFloatField1", new float[] {1.f, 2.f, 3.f}));
    document2.add(
        new KnnFloatVectorField(
            "knnFloatField2",
            new float[] {-5.2f, 8.7f, 3.1f},
            VectorSimilarityFunction.DOT_PRODUCT));
    document2.add(
        new KnnFloatVectorField(
            "knnFloatField3", new float[] {0.2f, -3.2f, 3.1f}, VectorSimilarityFunction.COSINE));
    document2.add(new KnnFloatVectorField("knnFloatField5", new float[] {2.f, 13.2f, 9.1f}));
    document2.add(new KnnByteVectorField("knnByteField1", new byte[] {1, -2, -30}));
    document2.add(
        new KnnByteVectorField(
            "knnByteField2", new byte[] {40, 21, 3}, VectorSimilarityFunction.DOT_PRODUCT));
    document2.add(
        new KnnByteVectorField(
            "knnByteField3", new byte[] {9, 2, 3}, VectorSimilarityFunction.COSINE));
    document2.add(
        new KnnByteVectorField(
            "knnByteField4",
            new byte[] {14, 29, 31},
            VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT));
    iw.addDocument(document2);

    Document document3 = new Document();
    document3.add(new StringField("id", "3", Field.Store.NO));
    document3.add(new SortedDocValuesField("id", new BytesRef("3")));
    document3.add(new KnnFloatVectorField("knnFloatField1", new float[] {1.f, 2.f, 3.f}));
    document3.add(
        new KnnFloatVectorField(
            "knnFloatField2", new float[] {-8.f, 7.f, -6.f}, VectorSimilarityFunction.DOT_PRODUCT));
    document3.add(new KnnFloatVectorField("knnFloatField5", new float[] {5.2f, 3.2f, 3.1f}));
    document3.add(new KnnByteVectorField("knnByteField1", new byte[] {-128, 0, 127}));
    document3.add(
        new KnnByteVectorField(
            "knnByteField2", new byte[] {-1, -2, -3}, VectorSimilarityFunction.DOT_PRODUCT));
    document3.add(
        new KnnByteVectorField(
            "knnByteField3", new byte[] {4, 2, 3}, VectorSimilarityFunction.COSINE));
    document3.add(
        new KnnByteVectorField(
            "knnByteField4",
            new byte[] {-4, -2, -128},
            VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT));
    document3.add(new KnnByteVectorField("knnByteField5", new byte[] {-120, -2, 3}));
    iw.addDocument(document3);
    iw.commit();
    iw.forceMerge(1);

    reader = iw.getReader();
    searcher = newSearcher(reader);
    iw.close();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    searcher = null;
    IOUtils.close(reader, dir, analyzer);
  }

  public void testEuclideanSimilarityValuesSource() throws Exception {
    float[] floatQueryVector = new float[] {9.f, 1.f, -10.f};

    // Checks the computed similarity score between indexed vectors and query vector
    // using DVS is correct by passing indexed and query vector in #compare
    DoubleValues dv =
        DoubleValuesSource.similarityToQueryVector(
            searcher.reader.leaves().get(0), floatQueryVector, "knnFloatField1");
    assertTrue(
        dv.advanceExact(0)
            && dv.doubleValue()
                == VectorSimilarityFunction.EUCLIDEAN.compare(
                    new float[] {1.f, 2.f, 3.f}, floatQueryVector));
    assertTrue(
        dv.advanceExact(1)
            && dv.doubleValue()
                == VectorSimilarityFunction.EUCLIDEAN.compare(
                    new float[] {1.f, 2.f, 3.f}, floatQueryVector));
    assertTrue(
        dv.advanceExact(2)
            && dv.doubleValue()
                == VectorSimilarityFunction.EUCLIDEAN.compare(
                    new float[] {1.f, 2.f, 3.f}, floatQueryVector));

    dv =
        DoubleValuesSource.similarityToQueryVector(
            searcher.reader.leaves().get(0), floatQueryVector, "knnFloatField5");
    assertTrue(
        dv.advanceExact(0)
            && dv.doubleValue()
                == VectorSimilarityFunction.EUCLIDEAN.compare(
                    new float[] {-6.7f, -1.0f, -0.9f}, floatQueryVector));
    assertTrue(
        dv.advanceExact(1)
            && dv.doubleValue()
                == VectorSimilarityFunction.EUCLIDEAN.compare(
                    new float[] {2.f, 13.2f, 9.1f}, floatQueryVector));
    assertTrue(
        dv.advanceExact(2)
            && dv.doubleValue()
                == VectorSimilarityFunction.EUCLIDEAN.compare(
                    new float[] {5.2f, 3.2f, 3.1f}, floatQueryVector));

    byte[] byteQueryVector = new byte[] {-128, 2, 127};

    dv =
        DoubleValuesSource.similarityToQueryVector(
            searcher.reader.leaves().get(0), byteQueryVector, "knnByteField1");
    assertTrue(
        dv.advanceExact(0)
            && dv.doubleValue()
                == VectorSimilarityFunction.EUCLIDEAN.compare(
                    new byte[] {106, 80, 127}, byteQueryVector));
    assertTrue(
        dv.advanceExact(1)
            && dv.doubleValue()
                == VectorSimilarityFunction.EUCLIDEAN.compare(
                    new byte[] {1, -2, -30}, byteQueryVector));
    assertTrue(
        dv.advanceExact(2)
            && dv.doubleValue()
                == VectorSimilarityFunction.EUCLIDEAN.compare(
                    new byte[] {-128, 0, 127}, byteQueryVector));

    dv =
        DoubleValuesSource.similarityToQueryVector(
            searcher.reader.leaves().get(0), byteQueryVector, "knnByteField5");
    assertFalse(dv.advanceExact(0));
    assertFalse(dv.advanceExact(1));
    assertTrue(
        dv.advanceExact(2)
            && dv.doubleValue()
                == VectorSimilarityFunction.EUCLIDEAN.compare(
                    new byte[] {-120, -2, 3}, byteQueryVector));
  }

  public void testDotSimilarityValuesSource() throws Exception {
    float[] floatQueryVector = new float[] {10.f, 1.f, -8.5f};

    // Checks the computed similarity score between indexed vectors and query vector
    // using DVS is correct by passing indexed and query vector in #compare
    DoubleValues dv =
        DoubleValuesSource.similarityToQueryVector(
            searcher.reader.leaves().get(0), floatQueryVector, "knnFloatField2");
    assertTrue(
        dv.advanceExact(0)
            && dv.doubleValue()
                == VectorSimilarityFunction.DOT_PRODUCT.compare(
                    new float[] {2.2f, -3.2f, -3.1f}, floatQueryVector));
    assertTrue(
        dv.advanceExact(1)
            && dv.doubleValue()
                == VectorSimilarityFunction.DOT_PRODUCT.compare(
                    new float[] {-5.2f, 8.7f, 3.1f}, floatQueryVector));
    assertTrue(
        dv.advanceExact(2)
            && dv.doubleValue()
                == VectorSimilarityFunction.DOT_PRODUCT.compare(
                    new float[] {-8.f, 7.f, -6.f}, floatQueryVector));

    byte[] byteQueryVector = new byte[] {-128, 2, 127};

    dv =
        DoubleValuesSource.similarityToQueryVector(
            searcher.reader.leaves().get(0), byteQueryVector, "knnByteField2");
    assertTrue(
        dv.advanceExact(0)
            && dv.doubleValue()
                == VectorSimilarityFunction.DOT_PRODUCT.compare(
                    new byte[] {4, 2, 3}, byteQueryVector));
    assertTrue(
        dv.advanceExact(1)
            && dv.doubleValue()
                == VectorSimilarityFunction.DOT_PRODUCT.compare(
                    new byte[] {40, 21, 3}, byteQueryVector));
    assertTrue(
        dv.advanceExact(2)
            && dv.doubleValue()
                == VectorSimilarityFunction.DOT_PRODUCT.compare(
                    new byte[] {-1, -2, -3}, byteQueryVector));
  }

  public void testCosineSimilarityValuesSource() throws Exception {
    float[] floatQueryVector = new float[] {0.6f, -1.6f, 38.0f};

    // Checks the computed similarity score between indexed vectors and query vector
    // using DVS is correct by passing indexed and query vector in #compare
    DoubleValues dv =
        DoubleValuesSource.similarityToQueryVector(
            searcher.reader.leaves().get(0), floatQueryVector, "knnFloatField3");
    assertTrue(
        dv.advanceExact(0)
            && dv.doubleValue()
                == VectorSimilarityFunction.COSINE.compare(
                    new float[] {4.5f, 10.3f, -7.f}, floatQueryVector));
    assertTrue(
        dv.advanceExact(1)
            && dv.doubleValue()
                == VectorSimilarityFunction.COSINE.compare(
                    new float[] {0.2f, -3.2f, 3.1f}, floatQueryVector));
    assertFalse(dv.advanceExact(2));

    byte[] byteQueryVector = new byte[] {-10, 8, 0};

    dv =
        DoubleValuesSource.similarityToQueryVector(
            searcher.reader.leaves().get(0), byteQueryVector, "knnByteField3");
    assertTrue(
        dv.advanceExact(0)
            && dv.doubleValue()
                == VectorSimilarityFunction.COSINE.compare(
                    new byte[] {-121, -64, -1}, byteQueryVector));
    assertTrue(
        dv.advanceExact(1)
            && dv.doubleValue()
                == VectorSimilarityFunction.COSINE.compare(new byte[] {9, 2, 3}, byteQueryVector));
    assertTrue(
        dv.advanceExact(2)
            && dv.doubleValue()
                == VectorSimilarityFunction.COSINE.compare(new byte[] {4, 2, 3}, byteQueryVector));
  }

  public void testMaximumProductSimilarityValuesSource() throws Exception {
    float[] floatQueryVector = new float[] {1.f, -6.f, -10.f};

    // Checks the computed similarity score between indexed vectors and query vector
    // using DVS is correct by passing indexed and query vector in #compare
    DoubleValues dv =
        DoubleValuesSource.similarityToQueryVector(
            searcher.reader.leaves().get(0), floatQueryVector, "knnFloatField4");
    assertTrue(dv.advanceExact(0));
    assertEquals(
        VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT.compare(
            new float[] {-1.3f, 1.0f, 1.0f}, floatQueryVector),
        dv.doubleValue(),
        0.0001);
    assertFalse(dv.advanceExact(1));
    assertFalse(dv.advanceExact(2));

    byte[] byteQueryVector = new byte[] {-127, 127, 127};

    dv =
        DoubleValuesSource.similarityToQueryVector(
            searcher.reader.leaves().get(0), byteQueryVector, "knnByteField4");
    assertTrue(dv.advanceExact(0));
    assertEquals(
        VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT.compare(
            new byte[] {-127, 127, 127}, byteQueryVector),
        dv.doubleValue(),
        0.0001);
    assertTrue(dv.advanceExact(1));
    assertEquals(
        VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT.compare(
            new byte[] {14, 29, 31}, byteQueryVector),
        dv.doubleValue(),
        0.0001);
    assertTrue(dv.advanceExact(2));
    assertEquals(
        VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT.compare(
            new byte[] {-4, -2, -128}, byteQueryVector),
        dv.doubleValue(),
        0.0001);
  }

  public void testFailuresWithSimilarityValuesSource() throws Exception {
    float[] floatQueryVector = new float[] {1.1f, 2.2f, 3.3f};
    byte[] byteQueryVector = new byte[] {-10, 20, 30};

    expectThrows(
        IllegalArgumentException.class,
        () ->
            DoubleValuesSource.similarityToQueryVector(
                searcher.reader.leaves().get(0), floatQueryVector, "knnByteField1"));
    expectThrows(
        IllegalArgumentException.class,
        () ->
            DoubleValuesSource.similarityToQueryVector(
                searcher.reader.leaves().get(0), byteQueryVector, "knnFloatField1"));

    DoubleValues dv =
        DoubleValuesSource.similarityToQueryVector(
            searcher.reader.leaves().get(0), floatQueryVector, "knnFloatField1");
    assertTrue(dv.advanceExact(0));
    assertEquals(
        dv.doubleValue(),
        VectorSimilarityFunction.EUCLIDEAN.compare(new float[] {1.f, 2.f, 3.f}, floatQueryVector),
        0.0);
    assertNotEquals(
        dv.doubleValue(),
        VectorSimilarityFunction.DOT_PRODUCT.compare(
            new float[] {1.f, 2.f, 3.f}, floatQueryVector));
    assertNotEquals(
        dv.doubleValue(),
        VectorSimilarityFunction.COSINE.compare(new float[] {1.f, 2.f, 3.f}, floatQueryVector));
    assertNotEquals(
        dv.doubleValue(),
        VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT.compare(
            new float[] {1.f, 2.f, 3.f}, floatQueryVector));
  }
}
