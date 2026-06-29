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
package org.apache.lucene.document.column;

import static org.apache.lucene.document.column.ColumnBatchTestUtil.ArrayDenseBinaryColumn;
import static org.apache.lucene.document.column.ColumnBatchTestUtil.simpleBatch;

import java.io.IOException;
import java.net.InetAddress;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.XYPointField;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.XYEncodingUtils;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

/**
 * Tests for multi-dimensional point indexing via {@link BinaryColumn}. {@link BinaryColumn} already
 * supports sparse multi-dimensional points; these tests verify the dense bulk fast path for
 * points-only batches.
 */
public class TestColumnBatchPointColumn extends LuceneTestCase {

  // ---------------------------------------------------------------------------
  // LatLonPoint (2D geo, 4 bytes per dimension = 8 bytes total)
  // ---------------------------------------------------------------------------

  public void testLatLonPointSparse() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {

      double[] lats = {40.7, 34.0, 51.5};
      double[] lons = {-74.0, -118.2, -0.1};
      int[] docIds = {0, 1, 2};
      BytesRef[] values = new BytesRef[3];
      for (int i = 0; i < 3; i++) {
        values[i] = new BytesRef(encodeLatLon(lats[i], lons[i]));
      }

      w.addBatch(
          simpleBatch(
              3,
              new ColumnBatchTestUtil.ArrayBinaryColumn("geo", LatLonPoint.TYPE, docIds, values)));

      try (DirectoryReader r = DirectoryReader.open(w)) {
        IndexSearcher searcher = newSearcher(r);
        assertEquals(1, searcher.count(LatLonPoint.newBoxQuery("geo", 40.0, 41.0, -75.0, -73.0)));
        assertEquals(3, searcher.count(LatLonPoint.newBoxQuery("geo", -90.0, 90.0, -180.0, 180.0)));
      }
    }
  }

  public void testLatLonPointDense() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {

      double[] lats = {40.7, 34.0, 51.5};
      double[] lons = {-74.0, -118.2, -0.1};
      BytesRef[] values = new BytesRef[3];
      for (int i = 0; i < 3; i++) {
        values[i] = new BytesRef(encodeLatLon(lats[i], lons[i]));
      }

      w.addBatch(simpleBatch(3, new ArrayDenseBinaryColumn("geo", LatLonPoint.TYPE, values)));

      try (DirectoryReader r = DirectoryReader.open(w)) {
        IndexSearcher searcher = newSearcher(r);
        assertEquals(1, searcher.count(LatLonPoint.newBoxQuery("geo", 40.0, 41.0, -75.0, -73.0)));
        assertEquals(3, searcher.count(LatLonPoint.newBoxQuery("geo", -90.0, 90.0, -180.0, 180.0)));
      }
    }
  }

  public void testLatLonPointMultiValued() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {

      int[] docIds = {0, 0, 1};
      BytesRef[] values = new BytesRef[3];
      values[0] = new BytesRef(encodeLatLon(40.7, -74.0));
      values[1] = new BytesRef(encodeLatLon(34.0, -118.2));
      values[2] = new BytesRef(encodeLatLon(51.5, -0.1));

      w.addBatch(
          simpleBatch(
              2,
              new ColumnBatchTestUtil.ArrayBinaryColumn("geo", LatLonPoint.TYPE, docIds, values)));

      try (DirectoryReader r = DirectoryReader.open(w)) {
        IndexSearcher searcher = newSearcher(r);

        assertEquals(1, searcher.count(LatLonPoint.newBoxQuery("geo", 40.0, 41.0, -75.0, -73.0)));
        assertEquals(1, searcher.count(LatLonPoint.newBoxQuery("geo", 33.0, 35.0, -119.0, -117.0)));
        assertEquals(1, searcher.count(LatLonPoint.newBoxQuery("geo", 51.0, 52.0, -1.0, 0.0)));
      }
    }
  }

  // ---------------------------------------------------------------------------
  // IntPoint multi-dimensional (2D, 4 bytes per dimension = 8 bytes total)
  // ---------------------------------------------------------------------------

  public void testIntPoint2DSparse() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {

      FieldType type = new FieldType();
      type.setDimensions(2, Integer.BYTES);
      type.freeze();

      int[] docIds = {0, 1, 2};
      BytesRef[] values =
          new BytesRef[] {IntPoint.pack(10, 20), IntPoint.pack(30, 40), IntPoint.pack(50, 60)};

      w.addBatch(
          simpleBatch(3, new ColumnBatchTestUtil.ArrayBinaryColumn("point", type, docIds, values)));

      try (DirectoryReader r = DirectoryReader.open(w)) {
        IndexSearcher searcher = newSearcher(r);

        assertEquals(
            1,
            searcher.count(IntPoint.newRangeQuery("point", new int[] {5, 15}, new int[] {15, 25})));
        assertEquals(
            3,
            searcher.count(
                IntPoint.newRangeQuery("point", new int[] {0, 0}, new int[] {100, 100})));
      }
    }
  }

  public void testIntPoint2DDense() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {

      FieldType type = new FieldType();
      type.setDimensions(2, Integer.BYTES);
      type.freeze();

      BytesRef[] values =
          new BytesRef[] {IntPoint.pack(10, 20), IntPoint.pack(30, 40), IntPoint.pack(50, 60)};

      w.addBatch(simpleBatch(3, new ArrayDenseBinaryColumn("point", type, values)));

      try (DirectoryReader r = DirectoryReader.open(w)) {
        IndexSearcher searcher = newSearcher(r);

        assertEquals(
            3,
            searcher.count(
                IntPoint.newRangeQuery("point", new int[] {0, 0}, new int[] {100, 100})));
      }
    }
  }

  // ---------------------------------------------------------------------------
  // FloatPoint multi-dimensional (2D, 4 bytes per dimension = 8 bytes total)
  // ---------------------------------------------------------------------------

  public void testFloatPoint2DSparse() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {

      FieldType type = new FieldType();
      type.setDimensions(2, Float.BYTES);
      type.freeze();

      int[] docIds = {0, 1};
      BytesRef[] values = new BytesRef[] {FloatPoint.pack(1.5f, 2.5f), FloatPoint.pack(3.5f, 4.5f)};

      w.addBatch(
          simpleBatch(2, new ColumnBatchTestUtil.ArrayBinaryColumn("point", type, docIds, values)));

      try (DirectoryReader r = DirectoryReader.open(w)) {
        IndexSearcher searcher = newSearcher(r);

        assertEquals(
            1,
            searcher.count(
                FloatPoint.newRangeQuery(
                    "point", new float[] {1.0f, 2.0f}, new float[] {2.0f, 3.0f})));
      }
    }
  }

  // ---------------------------------------------------------------------------
  // DoublePoint multi-dimensional (2D, 8 bytes per dimension = 16 bytes total)
  // ---------------------------------------------------------------------------

  public void testDoublePoint2DSparse() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {

      FieldType type = new FieldType();
      type.setDimensions(2, Double.BYTES);
      type.freeze();

      int[] docIds = {0, 1};
      BytesRef[] values =
          new BytesRef[] {DoublePoint.pack(1.5d, 2.5d), DoublePoint.pack(3.5d, 4.5d)};

      w.addBatch(
          simpleBatch(2, new ColumnBatchTestUtil.ArrayBinaryColumn("point", type, docIds, values)));

      try (DirectoryReader r = DirectoryReader.open(w)) {
        IndexSearcher searcher = newSearcher(r);

        assertEquals(
            1,
            searcher.count(
                DoublePoint.newRangeQuery(
                    "point", new double[] {1.0d, 2.0d}, new double[] {2.0d, 3.0d})));
      }
    }
  }

  // ---------------------------------------------------------------------------
  // XYPointField (2D cartesian, 4 bytes per dimension = 8 bytes total)
  // ---------------------------------------------------------------------------

  public void testXYPointFieldSparse() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {

      int[] docIds = {0, 1};
      BytesRef[] values =
          new BytesRef[] {
            new BytesRef(encodeXY(100.0f, 200.0f)), new BytesRef(encodeXY(300.0f, 400.0f))
          };

      w.addBatch(
          simpleBatch(
              2,
              new ColumnBatchTestUtil.ArrayBinaryColumn("xy", XYPointField.TYPE, docIds, values)));

      try (DirectoryReader r = DirectoryReader.open(w)) {
        IndexSearcher searcher = newSearcher(r);

        assertEquals(
            1, searcher.count(XYPointField.newBoxQuery("xy", 50.0f, 150.0f, 150.0f, 250.0f)));
      }
    }
  }

  // ---------------------------------------------------------------------------
  // LongPoint multi-dimensional (2D, 8 bytes per dimension = 16 bytes total)
  // ---------------------------------------------------------------------------

  public void testLongPoint2DSparse() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {

      FieldType type = new FieldType();
      type.setDimensions(2, Long.BYTES);
      type.freeze();

      int[] docIds = {0, 1};
      BytesRef[] values = new BytesRef[] {LongPoint.pack(100L, 200L), LongPoint.pack(300L, 400L)};

      w.addBatch(
          simpleBatch(2, new ColumnBatchTestUtil.ArrayBinaryColumn("point", type, docIds, values)));

      try (DirectoryReader r = DirectoryReader.open(w)) {
        IndexSearcher searcher = newSearcher(r);

        assertEquals(
            1,
            searcher.count(
                LongPoint.newRangeQuery("point", new long[] {50L, 150L}, new long[] {150L, 250L})));
      }
    }
  }

  // ---------------------------------------------------------------------------
  // InetAddressPoint (1D, 16 bytes per dimension)
  // ---------------------------------------------------------------------------

  public void testInetAddressPointSparse() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {

      FieldType type = new FieldType();
      type.setDimensions(1, 16);
      type.freeze();

      int[] docIds = {0, 1};
      BytesRef[] values =
          new BytesRef[] {
            new BytesRef(
                org.apache.lucene.document.InetAddressPoint.encode(
                    InetAddress.getByName("192.168.1.1"))),
            new BytesRef(
                org.apache.lucene.document.InetAddressPoint.encode(
                    InetAddress.getByName("10.0.0.1")))
          };

      w.addBatch(
          simpleBatch(2, new ColumnBatchTestUtil.ArrayBinaryColumn("ip", type, docIds, values)));

      try (DirectoryReader r = DirectoryReader.open(w)) {
        IndexSearcher searcher = newSearcher(r);

        assertEquals(
            1,
            searcher.count(
                org.apache.lucene.document.InetAddressPoint.newExactQuery(
                    "ip", InetAddress.getByName("192.168.1.1"))));
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Custom point type with unusual byte width (3D, 3 bytes per dimension = 9 bytes)
  // ---------------------------------------------------------------------------

  public void testCustomPoint3D() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {

      FieldType type = new FieldType();
      type.setDimensions(3, 3);
      type.freeze();

      int[] docIds = {0, 1, 2};
      BytesRef[] values =
          new BytesRef[] {
            new BytesRef(new byte[] {0, 0, 0, 10, 10, 10, 20, 20, 20}),
            new BytesRef(new byte[] {5, 5, 5, 15, 15, 15, 25, 25, 25}),
            new BytesRef(new byte[] {10, 10, 10, 20, 20, 20, 30, 30, 30})
          };

      w.addBatch(
          simpleBatch(3, new ColumnBatchTestUtil.ArrayBinaryColumn("point", type, docIds, values)));

      try (DirectoryReader r = DirectoryReader.open(w)) {
        LeafReader leaf = getOnlyLeafReader(r);

        PointValues pv = leaf.getPointValues("point");
        assertNotNull(pv);
        assertEquals(3, pv.size());
        assertEquals(3, pv.getDocCount());
      }
    }
  }

  public void testCustomPoint3DDense() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {

      FieldType type = new FieldType();
      type.setDimensions(3, 3);
      type.freeze();

      BytesRef[] values =
          new BytesRef[] {
            new BytesRef(new byte[] {0, 0, 0, 10, 10, 10, 20, 20, 20}),
            new BytesRef(new byte[] {5, 5, 5, 15, 15, 15, 25, 25, 25}),
            new BytesRef(new byte[] {10, 10, 10, 20, 20, 20, 30, 30, 30})
          };

      w.addBatch(simpleBatch(3, new ArrayDenseBinaryColumn("point", type, values)));

      try (DirectoryReader r = DirectoryReader.open(w)) {
        LeafReader leaf = getOnlyLeafReader(r);

        PointValues pv = leaf.getPointValues("point");
        assertNotNull(pv);
        assertEquals(3, pv.size());
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Parity test: dense BinaryColumn vs sparse BinaryColumn
  // ---------------------------------------------------------------------------

  public void testDenseMatchesSparseLatLon() throws IOException {
    int numDocs = 100;
    double[] lats = new double[numDocs];
    double[] lons = new double[numDocs];
    for (int i = 0; i < numDocs; i++) {
      lats[i] = -90.0 + (180.0 * i / numDocs);
      lons[i] = -180.0 + (360.0 * i / numDocs);
    }

    // Dense path
    try (Directory dirA = newDirectory();
        IndexWriter wA = new IndexWriter(dirA, newIndexWriterConfig())) {
      BytesRef[] denseValues = new BytesRef[numDocs];
      for (int i = 0; i < numDocs; i++) {
        denseValues[i] = new BytesRef(encodeLatLon(lats[i], lons[i]));
      }
      wA.addBatch(
          simpleBatch(numDocs, new ArrayDenseBinaryColumn("geo", LatLonPoint.TYPE, denseValues)));

      try (DirectoryReader rA = DirectoryReader.open(wA)) {
        // Sparse path
        try (Directory dirB = newDirectory();
            IndexWriter wB = new IndexWriter(dirB, newIndexWriterConfig())) {
          int[] docIds = new int[numDocs];
          BytesRef[] sparseValues = new BytesRef[numDocs];
          for (int i = 0; i < numDocs; i++) {
            docIds[i] = i;
            sparseValues[i] = new BytesRef(encodeLatLon(lats[i], lons[i]));
          }
          wB.addBatch(
              simpleBatch(
                  numDocs,
                  new ColumnBatchTestUtil.ArrayBinaryColumn(
                      "geo", LatLonPoint.TYPE, docIds, sparseValues)));

          try (DirectoryReader rB = DirectoryReader.open(wB)) {
            for (String field : new String[] {"geo"}) {
              PointValues pvA = getOnlyLeafReader(rA).getPointValues(field);
              PointValues pvB = getOnlyLeafReader(rB).getPointValues(field);
              assertNotNull(pvA);
              assertNotNull(pvB);
              assertEquals(pvA.size(), pvB.size());
              assertEquals(pvA.getDocCount(), pvB.getDocCount());
            }

            IndexSearcher searcherA = newSearcher(rA);
            IndexSearcher searcherB = newSearcher(rB);

            for (int i = 0; i < numDocs; i += 7) {
              double lat = lats[i];
              double lon = lons[i];
              double latMin = Math.max(-90.0, lat - 0.0001);
              double latMax = Math.min(90.0, lat + 0.0001);
              double lonMin = Math.max(-180.0, lon - 0.0001);
              double lonMax = Math.min(180.0, lon + 0.0001);
              var q = LatLonPoint.newBoxQuery("geo", latMin, latMax, lonMin, lonMax);
              assertEquals("query doc " + i + " mismatch", searcherA.count(q), searcherB.count(q));
            }

            assertEquals(
                searcherA.count(LatLonPoint.newBoxQuery("geo", 0, 45, -100, 0)),
                searcherB.count(LatLonPoint.newBoxQuery("geo", 0, 45, -100, 0)));
            assertEquals(
                searcherA.count(LatLonPoint.newBoxQuery("geo", -90, 90, -180, 180)),
                searcherB.count(LatLonPoint.newBoxQuery("geo", -90, 90, -180, 180)));
            assertEquals(
                searcherA.count(LatLonPoint.newDistanceQuery("geo", 0, 0, 10_000_000)),
                searcherB.count(LatLonPoint.newDistanceQuery("geo", 0, 0, 10_000_000)));
          }
        }
      }
    }
  }

  public void testDenseMatchesSparseInt2D() throws IOException {
    int numDocs = 50;
    FieldType type = new FieldType();
    type.setDimensions(2, Integer.BYTES);
    type.freeze();

    // Dense path
    try (Directory dirA = newDirectory();
        IndexWriter wA = new IndexWriter(dirA, newIndexWriterConfig())) {
      BytesRef[] denseValues = new BytesRef[numDocs];
      for (int i = 0; i < numDocs; i++) {
        denseValues[i] = IntPoint.pack(i, i * 2);
      }
      wA.addBatch(simpleBatch(numDocs, new ArrayDenseBinaryColumn("pt", type, denseValues)));

      try (DirectoryReader rA = DirectoryReader.open(wA)) {
        // Sparse path
        try (Directory dirB = newDirectory();
            IndexWriter wB = new IndexWriter(dirB, newIndexWriterConfig())) {
          int[] docIds = new int[numDocs];
          BytesRef[] sparseValues = new BytesRef[numDocs];
          for (int i = 0; i < numDocs; i++) {
            docIds[i] = i;
            sparseValues[i] = IntPoint.pack(i, i * 2);
          }
          wB.addBatch(
              simpleBatch(
                  numDocs,
                  new ColumnBatchTestUtil.ArrayBinaryColumn("pt", type, docIds, sparseValues)));

          try (DirectoryReader rB = DirectoryReader.open(wB)) {
            IndexSearcher searcherA = newSearcher(rA);
            IndexSearcher searcherB = newSearcher(rB);

            for (int i = 0; i < numDocs; i += 5) {
              var q = IntPoint.newRangeQuery("pt", new int[] {i, i * 2}, new int[] {i, i * 2});
              assertEquals("query doc " + i + " mismatch", searcherA.count(q), searcherB.count(q));
            }

            assertEquals(
                searcherA.count(
                    IntPoint.newRangeQuery("pt", new int[] {0, 0}, new int[] {100, 100})),
                searcherB.count(
                    IntPoint.newRangeQuery("pt", new int[] {0, 0}, new int[] {100, 100})));
          }
        }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Mixed batch: BinaryColumn (dense points) alongside LongColumn
  // ---------------------------------------------------------------------------

  public void testMixedBatchWithLongColumn() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {

      FieldType pointType = new FieldType();
      pointType.setDimensions(2, Integer.BYTES);
      pointType.freeze();
      BytesRef[] pointValues = new BytesRef[] {IntPoint.pack(10, 20), IntPoint.pack(30, 40)};

      long[] numericValues = {100L, 200L};

      w.addBatch(
          simpleBatch(
              2,
              new ArrayDenseBinaryColumn("point", pointType, pointValues),
              new ColumnBatchTestUtil.ArrayDenseLongColumn(
                  "numeric",
                  org.apache.lucene.document.NumericDocValuesField.TYPE,
                  numericValues)));

      try (DirectoryReader r = DirectoryReader.open(w)) {
        IndexSearcher searcher = newSearcher(r);

        assertEquals(
            2,
            searcher.count(
                IntPoint.newRangeQuery("point", new int[] {0, 0}, new int[] {100, 100})));
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Validation: wrong BytesRef length
  // ---------------------------------------------------------------------------

  public void testDenseWrongPackedLength() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {

      // LatLonPoint expects 8 bytes, provide 4
      BytesRef[] values = new BytesRef[] {new BytesRef(new byte[] {0, 0, 0, 0})};

      IllegalArgumentException e =
          expectThrows(
              IllegalArgumentException.class,
              () ->
                  w.addBatch(
                      simpleBatch(1, new ArrayDenseBinaryColumn("geo", LatLonPoint.TYPE, values))));
      assertTrue(e.getMessage().contains("point value has length=4"));
    }
  }

  // ---------------------------------------------------------------------------
  // Wide packed points (> ~64 bytes per value) exercise dedicated buffer growth
  // and the fillPackedPoints override path.
  // ---------------------------------------------------------------------------

  public void testDenseWidePointValuesDedicatedBufferAndOverride() throws IOException {
    // 5 dimensions * 16 bytes = 80 bytes packed. This exceeds the 4 KiB shared scratch
    // threshold for MIN_VALUES_PER_CHUNK (64), forcing the dedicated larger buffer path
    // (min 80*64 = 5120 bytes) and multiple chunks for a moderate batch.
    final int dims = 5;
    final int bytesPerDim = 16;
    final int width = dims * bytesPerDim; // 80
    final int numDocs = 300; // ~4-5 chunks at 64 values/chunk

    byte[] packed = new byte[numDocs * width];
    for (int i = 0; i < numDocs; i++) {
      for (int b = 0; b < width; b++) {
        packed[i * width + b] = (byte) ((i + b) & 0xff);
      }
    }

    FieldType type = new FieldType();
    type.setDimensions(dims, bytesPerDim);
    type.freeze();

    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {

      // Use the contiguous impl so we exercise the optimized fillPackedPoints override
      // (single arraycopy) together with the dedicated buffer logic.
      w.addBatch(
          simpleBatch(
              numDocs,
              new ColumnBatchTestUtil.ContiguousDenseBinaryColumn("wide", type, packed, width)));

      w.commit();
      try (DirectoryReader r = DirectoryReader.open(w)) {
        LeafReader leaf = getOnlyLeafReader(r);
        PointValues pv = leaf.getPointValues("wide");
        assertNotNull(pv);
        assertEquals(numDocs, pv.size());
        assertEquals(numDocs, pv.getDocCount());
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Large batch exercising chunking (multiple chunks even for narrow 8-byte points)
  // ---------------------------------------------------------------------------

  public void testDenseLargeBatchChunking() throws IOException {
    // 8-byte values (LatLon) with 4 KiB shared scratch => 512 values per chunk.
    // Use enough docs to cross several chunk boundaries when using the dense ND path.
    int numDocs = 2000;
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {

      BytesRef[] values = new BytesRef[numDocs];
      for (int i = 0; i < numDocs; i++) {
        values[i] =
            new BytesRef(
                encodeLatLon(-90.0 + (180.0 * i / numDocs), -180.0 + (360.0 * i / numDocs)));
      }

      w.addBatch(simpleBatch(numDocs, new ArrayDenseBinaryColumn("geo", LatLonPoint.TYPE, values)));

      w.commit();
      try (DirectoryReader r = DirectoryReader.open(w)) {
        LeafReader leaf = getOnlyLeafReader(r);

        PointValues pv = leaf.getPointValues("geo");
        assertNotNull(pv);
        assertEquals(numDocs, pv.size());
        assertEquals(numDocs, pv.getDocCount());
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Dense BinaryColumn supplying both doc values and points (exercises fresh cursors)
  // ---------------------------------------------------------------------------

  public void testDenseBinaryColumnWithDocValuesAndPoints() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {

      FieldType type = new FieldType();
      type.setDimensions(2, Integer.BYTES);
      type.setDocValuesType(org.apache.lucene.index.DocValuesType.BINARY);
      type.freeze();

      BytesRef[] values = new BytesRef[] {IntPoint.pack(10, 20), IntPoint.pack(30, 40)};

      w.addBatch(simpleBatch(2, new ArrayDenseBinaryColumn("point", type, values)));

      try (DirectoryReader r = DirectoryReader.open(w)) {
        IndexSearcher searcher = newSearcher(r);

        assertEquals(
            2,
            searcher.count(
                IntPoint.newRangeQuery("point", new int[] {0, 0}, new int[] {100, 100})));
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Index sorting with dense binary points
  // ---------------------------------------------------------------------------

  public void testDenseWithIndexSort() throws IOException {
    int numDocs = 100;
    try (Directory dir = newDirectory()) {

      IndexWriterConfig iwc = newIndexWriterConfig();
      iwc.setIndexSort(
          new org.apache.lucene.search.Sort(
              new org.apache.lucene.search.SortField(
                  "sort", org.apache.lucene.search.SortField.Type.INT)));

      try (IndexWriter w = new IndexWriter(dir, iwc)) {

        // Add a sort field via regular doc, then batch with points
        org.apache.lucene.document.Document doc = new org.apache.lucene.document.Document();
        doc.add(new org.apache.lucene.document.IntPoint("sort", 1));
        doc.add(new org.apache.lucene.document.NumericDocValuesField("sort", 1));
        w.addDocument(doc);

        BytesRef[] values = new BytesRef[numDocs];
        for (int i = 0; i < numDocs; i++) {
          values[i] =
              new BytesRef(
                  encodeLatLon(-90.0 + (180.0 * i / numDocs), -180.0 + (360.0 * i / numDocs)));
        }
        w.addBatch(
            simpleBatch(numDocs, new ArrayDenseBinaryColumn("geo", LatLonPoint.TYPE, values)));

        w.forceMerge(1);

        try (DirectoryReader r = DirectoryReader.open(w)) {
          LeafReader leaf = getOnlyLeafReader(r);

          PointValues pv = leaf.getPointValues("geo");
          assertNotNull(pv);
          assertEquals(numDocs, pv.size());
          assertEquals(numDocs, pv.getDocCount());
        }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static byte[] encodeLatLon(double lat, double lon) {
    byte[] bytes = new byte[LatLonPoint.BYTES * 2];
    NumericUtils.intToSortableBytes(GeoEncodingUtils.encodeLatitude(lat), bytes, 0);
    NumericUtils.intToSortableBytes(
        GeoEncodingUtils.encodeLongitude(lon), bytes, LatLonPoint.BYTES);
    return bytes;
  }

  private static byte[] encodeXY(float x, float y) {
    byte[] bytes = new byte[XYPointField.BYTES * 2];
    NumericUtils.intToSortableBytes(XYEncodingUtils.encode(x), bytes, 0);
    NumericUtils.intToSortableBytes(XYEncodingUtils.encode(y), bytes, XYPointField.BYTES);
    return bytes;
  }
}
