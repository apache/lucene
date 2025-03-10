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

package org.apache.lucene.index;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.Version;

public class TestSegmentToThreadMapping extends LuceneTestCase {

  private static LeafReader dummyIndexReader(final int maxDoc) {
    return new LeafReader() {
      @Override
      public int maxDoc() {
        return maxDoc;
      }

      @Override
      public int numDocs() {
        return maxDoc;
      }

      @Override
      public FieldInfos getFieldInfos() {
        return FieldInfos.EMPTY;
      }

      @Override
      public Bits getLiveDocs() {
        return null;
      }

      @Override
      public Terms terms(String field) throws IOException {
        return null;
      }

      @Override
      public TermVectors termVectors() {
        return TermVectors.EMPTY;
      }

      @Override
      public NumericDocValues getNumericDocValues(String field) {
        return null;
      }

      @Override
      public BinaryDocValues getBinaryDocValues(String field) {
        return null;
      }

      @Override
      public SortedDocValues getSortedDocValues(String field) {
        return null;
      }

      @Override
      public SortedNumericDocValues getSortedNumericDocValues(String field) {
        return null;
      }

      @Override
      public SortedSetDocValues getSortedSetDocValues(String field) {
        return null;
      }

      @Override
      public NumericDocValues getNormValues(String field) {
        return null;
      }

      @Override
      public DocValuesSkipper getDocValuesSkipper(String field) {
        return null;
      }

      @Override
      public PointValues getPointValues(String field) {
        return null;
      }

      @Override
      public FloatVectorValues getFloatVectorValues(String field) {
        return null;
      }

      @Override
      public ByteVectorValues getByteVectorValues(String field) {
        return null;
      }

      @Override
      public void searchNearestVectors(
          String field, float[] target, KnnCollector knnCollector, Bits acceptDocs) {}

      @Override
      public void searchNearestVectors(
          String field, byte[] target, KnnCollector knnCollector, Bits acceptDocs) {}

      @Override
      protected void doClose() {}

      @Override
      public StoredFields storedFields() {
        return new StoredFields() {
          @Override
          public void document(int doc, StoredFieldVisitor visitor) {}
        };
      }

      @Override
      public void checkIntegrity() throws IOException {}

      @Override
      public LeafMetaData getMetaData() {
        return new LeafMetaData(Version.LATEST.major, Version.LATEST, null, false);
      }

      @Override
      public CacheHelper getCoreCacheHelper() {
        return null;
      }

      @Override
      public CacheHelper getReaderCacheHelper() {
        return null;
      }
    };
  }

  private static List<LeafReaderContext> createLeafReaderContexts(int... maxDocs) {
    List<LeafReaderContext> leafReaderContexts = new ArrayList<>();
    for (int maxDoc : maxDocs) {
      leafReaderContexts.add(new LeafReaderContext(dummyIndexReader(maxDoc)));
    }
    Collections.shuffle(leafReaderContexts, random());
    return leafReaderContexts;
  }

  public void testSingleSlice() {
    List<LeafReaderContext> leafReaderContexts =
        createLeafReaderContexts(50_000, 30_000, 30_000, 30_000);
    IndexSearcher.LeafSlice[] resultSlices =
        IndexSearcher.slices(
            leafReaderContexts, 250_000, RandomizedTest.randomIntBetween(4, 10), false);
    assertEquals(1, resultSlices.length);
    assertEquals(4, resultSlices[0].partitions.length);
  }

  public void testSingleSliceWithPartitions() {
    List<LeafReaderContext> leafReaderContexts =
        createLeafReaderContexts(50_000, 30_000, 30_000, 30_000);
    IndexSearcher.LeafSlice[] resultSlices =
        IndexSearcher.slices(
            leafReaderContexts, 250_000, RandomizedTest.randomIntBetween(4, 10), true);
    assertEquals(1, resultSlices.length);
    assertEquals(4, resultSlices[0].partitions.length);
  }

  public void testMaxSegmentsPerSlice() {
    List<LeafReaderContext> leafReaderContexts =
        createLeafReaderContexts(50_000, 30_000, 30_000, 30_000);
    {
      IndexSearcher.LeafSlice[] resultSlices =
          IndexSearcher.slices(leafReaderContexts, 250_000, 3, false);
      assertEquals(2, resultSlices.length);
      assertEquals(3, resultSlices[0].partitions.length);
      assertEquals(110_000, resultSlices[0].getMaxDocs());
      assertEquals(1, resultSlices[1].partitions.length);
      assertEquals(30_000, resultSlices[1].getMaxDocs());
    }
    {
      IndexSearcher.LeafSlice[] resultSlices =
          IndexSearcher.slices(leafReaderContexts, 250_000, 2, false);
      assertEquals(2, resultSlices.length);
      assertEquals(2, resultSlices[0].partitions.length);
      assertEquals(80_000, resultSlices[0].getMaxDocs());
      assertEquals(2, resultSlices[1].partitions.length);
      assertEquals(60_000, resultSlices[1].getMaxDocs());
    }
    {
      IndexSearcher.LeafSlice[] resultSlices =
          IndexSearcher.slices(leafReaderContexts, 250_000, 1, false);
      assertEquals(4, resultSlices.length);
      assertEquals(1, resultSlices[0].partitions.length);
      assertEquals(50_000, resultSlices[0].getMaxDocs());
      assertEquals(1, resultSlices[1].partitions.length);
      assertEquals(30_000, resultSlices[1].getMaxDocs());
      assertEquals(1, resultSlices[2].partitions.length);
      assertEquals(30_000, resultSlices[2].getMaxDocs());
      assertEquals(1, resultSlices[3].partitions.length);
      assertEquals(30_000, resultSlices[3].getMaxDocs());
    }
  }

  public void testMaxSegmentsPerSliceWithPartitions() {
    List<LeafReaderContext> leafReaderContexts =
        createLeafReaderContexts(50_000, 30_000, 30_000, 30_000);
    {
      IndexSearcher.LeafSlice[] resultSlices =
          IndexSearcher.slices(leafReaderContexts, 250_000, 3, true);
      assertEquals(2, resultSlices.length);
      assertEquals(3, resultSlices[0].partitions.length);
      assertEquals(110_000, resultSlices[0].getMaxDocs());
      assertEquals(1, resultSlices[1].partitions.length);
      assertEquals(30_000, resultSlices[1].getMaxDocs());
    }
    {
      IndexSearcher.LeafSlice[] resultSlices =
          IndexSearcher.slices(leafReaderContexts, 250_000, 2, true);
      assertEquals(2, resultSlices.length);
      assertEquals(2, resultSlices[0].partitions.length);
      assertEquals(80_000, resultSlices[0].getMaxDocs());
      assertEquals(2, resultSlices[1].partitions.length);
      assertEquals(60_000, resultSlices[1].getMaxDocs());
    }
    {
      IndexSearcher.LeafSlice[] resultSlices =
          IndexSearcher.slices(leafReaderContexts, 250_000, 1, true);
      assertEquals(4, resultSlices.length);
      assertEquals(1, resultSlices[0].partitions.length);
      assertEquals(50_000, resultSlices[0].getMaxDocs());
      assertEquals(1, resultSlices[1].partitions.length);
      assertEquals(30_000, resultSlices[1].getMaxDocs());
      assertEquals(1, resultSlices[2].partitions.length);
      assertEquals(30_000, resultSlices[2].getMaxDocs());
      assertEquals(1, resultSlices[3].partitions.length);
      assertEquals(30_000, resultSlices[3].getMaxDocs());
    }
  }

  public void testSmallSegments() {
    List<LeafReaderContext> leafReaderContexts =
        createLeafReaderContexts(10_000, 10_000, 10_000, 10_000, 10_000, 10_000, 130_000, 130_000);

    {
      IndexSearcher.LeafSlice[] resultSlices =
          IndexSearcher.slices(leafReaderContexts, 250_000, 5, false);
      assertEquals(3, resultSlices.length);

      assertEquals(2, resultSlices[0].partitions.length);
      assertEquals(260_000, resultSlices[0].getMaxDocs());
      assertEquals(5, resultSlices[1].partitions.length);
      assertEquals(50_000, resultSlices[1].getMaxDocs());
      assertEquals(1, resultSlices[2].partitions.length);
      assertEquals(10_000, resultSlices[2].getMaxDocs());
    }
    {
      IndexSearcher.LeafSlice[] resultSlices =
          IndexSearcher.slices(leafReaderContexts, 130_000, 5, false);
      assertEquals(3, resultSlices.length);
      // this is odd, because we allow two segments in the same slice with both size ==
      // maxDocsPerSlice
      assertEquals(2, resultSlices[0].partitions.length);
      assertEquals(260_000, resultSlices[0].getMaxDocs());
      assertEquals(5, resultSlices[1].partitions.length);
      assertEquals(50_000, resultSlices[1].getMaxDocs());
      assertEquals(1, resultSlices[2].partitions.length);
      assertEquals(10_000, resultSlices[2].getMaxDocs());
    }
  }

  public void testSmallSegmentsWithPartitions() {
    List<LeafReaderContext> leafReaderContexts =
        createLeafReaderContexts(10_000, 10_000, 10_000, 10_000, 10_000, 10_000, 130_000, 130_000);

    {
      IndexSearcher.LeafSlice[] resultSlices =
          IndexSearcher.slices(leafReaderContexts, 250_000, 5, true);
      assertEquals(3, resultSlices.length);

      assertEquals(2, resultSlices[0].partitions.length);
      assertEquals(260_000, resultSlices[0].getMaxDocs());
      assertEquals(5, resultSlices[1].partitions.length);
      assertEquals(50_000, resultSlices[1].getMaxDocs());
      assertEquals(1, resultSlices[2].partitions.length);
      assertEquals(10_000, resultSlices[2].getMaxDocs());
    }
    {
      IndexSearcher.LeafSlice[] resultSlices =
          IndexSearcher.slices(leafReaderContexts, 130_000, 5, true);
      assertEquals(3, resultSlices.length);
      // this is odd, because we allow two segments in the same slice with both size ==
      // maxDocsPerSlice
      assertEquals(2, resultSlices[0].partitions.length);
      assertEquals(260_000, resultSlices[0].getMaxDocs());
      assertEquals(5, resultSlices[1].partitions.length);
      assertEquals(50_000, resultSlices[1].getMaxDocs());
      assertEquals(1, resultSlices[2].partitions.length);
      assertEquals(10_000, resultSlices[2].getMaxDocs());
    }
  }

  public void testLargeSlices() {
    List<LeafReaderContext> leafReaderContexts =
        createLeafReaderContexts(290_900, 170_000, 170_000, 170_000);
    IndexSearcher.LeafSlice[] resultSlices =
        IndexSearcher.slices(leafReaderContexts, 250_000, 5, false);

    assertEquals(3, resultSlices.length);
    assertEquals(1, resultSlices[0].partitions.length);
    assertEquals(2, resultSlices[1].partitions.length);
    assertEquals(1, resultSlices[2].partitions.length);
  }

  public void testLargeSlicesWithPartitions() {
    List<LeafReaderContext> leafReaderContexts =
        createLeafReaderContexts(290_900, 170_000, 170_000, 170_000);
    IndexSearcher.LeafSlice[] resultSlices =
        IndexSearcher.slices(
            leafReaderContexts, 250_000, RandomizedTest.randomIntBetween(5, 10), true);

    assertEquals(4, resultSlices.length);
    assertEquals(1, resultSlices[0].partitions.length);
    assertEquals(145_450, resultSlices[0].getMaxDocs());
    assertEquals(1, resultSlices[1].partitions.length);
    assertEquals(145_450, resultSlices[1].getMaxDocs());
    assertEquals(2, resultSlices[2].partitions.length);
    assertEquals(340_000, resultSlices[2].getMaxDocs());
    assertEquals(1, resultSlices[3].partitions.length);
    assertEquals(170_000, resultSlices[3].getMaxDocs());
  }

  public void testSingleSegmentPartitions() {
    List<LeafReaderContext> leafReaderContexts = createLeafReaderContexts(750_001);
    IndexSearcher.LeafSlice[] resultSlices =
        IndexSearcher.slices(
            leafReaderContexts, 250_000, RandomizedTest.randomIntBetween(1, 10), true);

    assertEquals(4, resultSlices.length);
    assertEquals(1, resultSlices[0].partitions.length);
    assertEquals(187_500, resultSlices[0].getMaxDocs());
    assertEquals(1, resultSlices[1].partitions.length);
    assertEquals(187_500, resultSlices[1].getMaxDocs());
    assertEquals(1, resultSlices[2].partitions.length);
    assertEquals(187_500, resultSlices[2].getMaxDocs());
    assertEquals(1, resultSlices[3].partitions.length);
    assertEquals(187_501, resultSlices[3].getMaxDocs());
  }

  public void testExtremeSegmentsPartitioning() {
    List<LeafReaderContext> leafReaderContexts = createLeafReaderContexts(2, 5, 10);
    IndexSearcher.LeafSlice[] resultSlices = IndexSearcher.slices(leafReaderContexts, 1, 1, true);

    assertEquals(12, resultSlices.length);
    int i = 0;
    for (IndexSearcher.LeafSlice leafSlice : resultSlices) {
      if (i++ > 4) {
        assertEquals(1, leafSlice.getMaxDocs());
      } else {
        assertEquals(2, leafSlice.getMaxDocs());
      }
      assertEquals(1, leafSlice.partitions.length);
    }
  }

  public void testIntraSliceDocIDOrder() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    w.addDocument(new Document());
    w.addDocument(new Document());
    w.commit();
    w.addDocument(new Document());
    w.addDocument(new Document());
    w.commit();
    IndexReader r = w.getReader();
    w.close();

    IndexSearcher s = new IndexSearcher(r, _ -> {});
    IndexSearcher.LeafSlice[] slices = s.getSlices();
    assertNotNull(slices);

    for (IndexSearcher.LeafSlice leafSlice : slices) {
      int previousDocBase = leafSlice.partitions[0].ctx.docBase;

      for (IndexSearcher.LeafReaderContextPartition leafReaderContextPartition :
          leafSlice.partitions) {
        assertTrue(previousDocBase <= leafReaderContextPartition.ctx.docBase);
        previousDocBase = leafReaderContextPartition.ctx.docBase;
      }
    }
    IOUtils.close(r, dir);
  }

  public void testIntraSliceDocIDOrderWithPartitions() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    w.addDocument(new Document());
    w.addDocument(new Document());
    w.commit();
    w.addDocument(new Document());
    w.addDocument(new Document());
    w.commit();
    IndexReader r = w.getReader();
    w.close();

    IndexSearcher s =
        new IndexSearcher(r, _ -> {}) {
          @Override
          protected LeafSlice[] slices(List<LeafReaderContext> leaves) {
            // force partitioning of segment with max docs per slice set to 1: 1 doc per partition.
            return slices(leaves, 1, 1, true);
          }
        };
    IndexSearcher.LeafSlice[] slices = s.getSlices();
    assertNotNull(slices);

    for (IndexSearcher.LeafSlice leafSlice : slices) {
      int previousDocBase = leafSlice.partitions[0].ctx.docBase;

      for (IndexSearcher.LeafReaderContextPartition leafReaderContextPartition :
          leafSlice.partitions) {
        assertTrue(previousDocBase <= leafReaderContextPartition.ctx.docBase);
        previousDocBase = leafReaderContextPartition.ctx.docBase;
      }
    }
    IOUtils.close(r, dir);
  }

  public void testRandom() {
    List<LeafReaderContext> leafReaderContexts = new ArrayList<>();
    int max = 500_000;
    int min = 10_000;
    int numSegments = 1 + random().nextInt(50);

    for (int i = 0; i < numSegments; i++) {
      leafReaderContexts.add(
          new LeafReaderContext(dummyIndexReader(random().nextInt((max - min) + 1) + min)));
    }
    final IndexSearcher.LeafSlice[] resultSlices =
        IndexSearcher.slices(leafReaderContexts, 250_000, 5, random().nextBoolean());
    assertTrue(resultSlices.length > 0);
  }
}
