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
package org.apache.lucene.codecs.lucene90;

import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.document.BinaryPoint;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.BasePointsFormatTestCase;
import org.apache.lucene.tests.index.MockRandomMergePolicy;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.bkd.BKDConfig;
import org.apache.lucene.util.bkd.BKDReader;

public class TestLucene90PointsFormat extends BasePointsFormatTestCase {

  private final Codec codec;
  private final int maxPointsInLeafNode;

  public TestLucene90PointsFormat() {
    // standard issue
    Codec defaultCodec = TestUtil.getDefaultCodec();
    if (random().nextBoolean()) {
      // randomize parameters
      maxPointsInLeafNode = TestUtil.nextInt(random(), 50, 500);
      double maxMBSortInHeap = 3.0 + (3 * random().nextDouble());
      if (VERBOSE) {
        System.out.println(
            "TEST: using Lucene60PointsFormat with maxPointsInLeafNode="
                + maxPointsInLeafNode
                + " and maxMBSortInHeap="
                + maxMBSortInHeap);
      }

      // sneaky impersonation!
      codec =
          new FilterCodec(defaultCodec.getName(), defaultCodec) {
            @Override
            public PointsFormat pointsFormat() {
              return new PointsFormat() {
                @Override
                public PointsWriter fieldsWriter(SegmentWriteState writeState) throws IOException {
                  return new Lucene90PointsWriter(writeState, maxPointsInLeafNode, maxMBSortInHeap);
                }

                @Override
                public PointsReader fieldsReader(SegmentReadState readState) throws IOException {
                  return new Lucene90PointsReader(readState);
                }
              };
            }
          };
    } else {
      // standard issue
      codec = defaultCodec;
      maxPointsInLeafNode = BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE;
    }
  }

  @Override
  protected Codec getCodec() {
    return codec;
  }

  @Override
  public void testMergeStability() throws Exception {
    assumeFalse(
        "TODO: mess with the parameters and test gets angry!", codec instanceof FilterCodec);
    super.testMergeStability();
  }

  // TODO: clean up the math/estimation here rather than suppress so many warnings
  @SuppressWarnings({"NarrowCalculation", "LongDoubleConversion"})
  public void testEstimatePointCount() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    // Avoid mockRandomMP since it may cause non-optimal merges that make the
    // number of points per leaf hard to predict
    while (iwc.getMergePolicy() instanceof MockRandomMergePolicy) {
      iwc.setMergePolicy(newMergePolicy());
    }
    IndexWriter w = new IndexWriter(dir, iwc);
    byte[] pointValue = new byte[3];
    byte[] uniquePointValue = new byte[3];
    random().nextBytes(uniquePointValue);
    final int numDocs =
        TEST_NIGHTLY ? atLeast(10000) : atLeast(500); // at night, make sure we have several leaves
    final boolean multiValues = random().nextBoolean();
    int totalValues = 0;
    for (int i = 0; i < numDocs; ++i) {
      Document doc = new Document();
      if (i == numDocs / 2) {
        totalValues++;
        doc.add(new BinaryPoint("f", uniquePointValue));
      } else {
        final int numValues = (multiValues) ? TestUtil.nextInt(random(), 2, 100) : 1;
        for (int j = 0; j < numValues; j++) {
          do {
            random().nextBytes(pointValue);
          } while (Arrays.equals(pointValue, uniquePointValue));
          doc.add(new BinaryPoint("f", pointValue));
          totalValues++;
        }
      }
      w.addDocument(doc);
    }
    w.forceMerge(1);
    final IndexReader r = DirectoryReader.open(w);
    w.close();
    final LeafReader lr = getOnlyLeafReader(r);
    PointValues points = lr.getPointValues("f");

    IntersectVisitor allPointsVisitor =
        new IntersectVisitor() {
          @Override
          public void visit(int docID, byte[] packedValue) throws IOException {}

          @Override
          public void visit(int docID) throws IOException {}

          @Override
          public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            return Relation.CELL_INSIDE_QUERY;
          }
        };

    assertEquals(totalValues, points.estimatePointCount(allPointsVisitor));
    assertEquals(numDocs, points.estimateDocCount(allPointsVisitor));

    IntersectVisitor noPointsVisitor =
        new IntersectVisitor() {
          @Override
          public void visit(int docID, byte[] packedValue) throws IOException {}

          @Override
          public void visit(int docID) throws IOException {}

          @Override
          public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            return Relation.CELL_OUTSIDE_QUERY;
          }
        };

    // Return 0 if no points match
    assertEquals(0, points.estimatePointCount(noPointsVisitor));
    assertEquals(0, points.estimateDocCount(noPointsVisitor));

    IntersectVisitor onePointMatchVisitor =
        new IntersectVisitor() {
          @Override
          public void visit(int docID, byte[] packedValue) throws IOException {}

          @Override
          public void visit(int docID) throws IOException {}

          @Override
          public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            if (Arrays.compareUnsigned(uniquePointValue, 0, 3, maxPackedValue, 0, 3) > 0
                || Arrays.compareUnsigned(uniquePointValue, 0, 3, minPackedValue, 0, 3) < 0) {
              return Relation.CELL_OUTSIDE_QUERY;
            }
            return Relation.CELL_CROSSES_QUERY;
          }
        };

    // If only one point matches, then the point count is (maxPointsInLeafNode + 1) / 2
    // in general, or maybe 2x that if the point is a split value
    final long pointCount = points.estimatePointCount(onePointMatchVisitor);
    final long lastNodePointCount = totalValues % maxPointsInLeafNode;
    assertTrue(
        "" + pointCount,
        pointCount == (maxPointsInLeafNode + 1) / 2 // common case
            || pointCount == (lastNodePointCount + 1) / 2 // not fully populated leaf
            || pointCount == 2 * ((maxPointsInLeafNode + 1) / 2) // if the point is a split value
            || pointCount
                == ((maxPointsInLeafNode + 1) / 2)
                    + ((lastNodePointCount + 1)
                        / 2)); // if the point is a split value and one leaf is not fully populated

    final long docCount = points.estimateDocCount(onePointMatchVisitor);

    if (multiValues) {
      assertEquals(
          docCount,
          (long)
              (docCount
                  * (1d
                      - Math.pow(
                          (numDocs - pointCount) / points.size(), points.size() / docCount))));
    } else {
      assertEquals(Math.min(pointCount, numDocs), docCount);
    }
    r.close();
    dir.close();
  }

  // The tree is always balanced in the N dims case, and leaves are
  // not all full so things are a bit different
  // TODO: clean up the math/estimation here rather than suppress so many warnings
  @SuppressWarnings({"NarrowCalculation", "LongDoubleConversion"})
  public void testEstimatePointCount2Dims() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    byte[][] pointValue = new byte[2][];
    pointValue[0] = new byte[3];
    pointValue[1] = new byte[3];
    byte[][] uniquePointValue = new byte[2][];
    uniquePointValue[0] = new byte[3];
    uniquePointValue[1] = new byte[3];
    random().nextBytes(uniquePointValue[0]);
    random().nextBytes(uniquePointValue[1]);
    final int numDocs =
        TEST_NIGHTLY
            ? atLeast(10000)
            : atLeast(1000); // in nightly, make sure we have several leaves
    final boolean multiValues = random().nextBoolean();
    int totalValues = 0;
    for (int i = 0; i < numDocs; ++i) {
      Document doc = new Document();
      if (i == numDocs / 2) {
        doc.add(new BinaryPoint("f", uniquePointValue));
        totalValues++;
      } else {
        final int numValues = (multiValues) ? TestUtil.nextInt(random(), 2, 100) : 1;
        for (int j = 0; j < numValues; j++) {
          do {
            random().nextBytes(pointValue[0]);
            random().nextBytes(pointValue[1]);
          } while (Arrays.equals(pointValue[0], uniquePointValue[0])
              || Arrays.equals(pointValue[1], uniquePointValue[1]));
          doc.add(new BinaryPoint("f", pointValue));
          totalValues++;
        }
      }
      w.addDocument(doc);
    }
    w.forceMerge(1);
    final IndexReader r = DirectoryReader.open(w);
    w.close();
    final LeafReader lr = getOnlyLeafReader(r);
    PointValues points = lr.getPointValues("f");

    IntersectVisitor allPointsVisitor =
        new IntersectVisitor() {
          @Override
          public void visit(int docID, byte[] packedValue) throws IOException {}

          @Override
          public void visit(int docID) throws IOException {}

          @Override
          public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            return Relation.CELL_INSIDE_QUERY;
          }
        };

    assertEquals(totalValues, points.estimatePointCount(allPointsVisitor));
    assertEquals(numDocs, points.estimateDocCount(allPointsVisitor));

    IntersectVisitor noPointsVisitor =
        new IntersectVisitor() {
          @Override
          public void visit(int docID, byte[] packedValue) throws IOException {}

          @Override
          public void visit(int docID) throws IOException {}

          @Override
          public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            return Relation.CELL_OUTSIDE_QUERY;
          }
        };

    // Return 0 if no points match
    assertEquals(0, points.estimatePointCount(noPointsVisitor));
    assertEquals(0, points.estimateDocCount(noPointsVisitor));

    IntersectVisitor onePointMatchVisitor =
        new IntersectVisitor() {
          @Override
          public void visit(int docID, byte[] packedValue) throws IOException {}

          @Override
          public void visit(int docID) throws IOException {}

          @Override
          public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            for (int dim = 0; dim < 2; ++dim) {
              if (Arrays.compareUnsigned(
                          uniquePointValue[dim], 0, 3, maxPackedValue, dim * 3, dim * 3 + 3)
                      > 0
                  || Arrays.compareUnsigned(
                          uniquePointValue[dim], 0, 3, minPackedValue, dim * 3, dim * 3 + 3)
                      < 0) {
                return Relation.CELL_OUTSIDE_QUERY;
              }
            }
            return Relation.CELL_CROSSES_QUERY;
          }
        };

    final long pointCount = points.estimatePointCount(onePointMatchVisitor);
    final long lastNodePointCount = totalValues % maxPointsInLeafNode;
    assertTrue(
        "" + pointCount,
        pointCount == (maxPointsInLeafNode + 1) / 2 // common case
            || pointCount == (lastNodePointCount + 1) / 2 // not fully populated leaf
            || pointCount == 2 * ((maxPointsInLeafNode + 1) / 2) // if the point is a split value
            || pointCount == ((maxPointsInLeafNode + 1) / 2) + ((lastNodePointCount + 1) / 2)
            // in extreme cases, a point can be shared by 4 leaves
            || pointCount == 4 * ((maxPointsInLeafNode + 1) / 2)
            || pointCount == 3 * ((maxPointsInLeafNode + 1) / 2) + ((lastNodePointCount + 1) / 2));

    final long docCount = points.estimateDocCount(onePointMatchVisitor);
    if (multiValues) {
      assertEquals(
          docCount,
          (long)
              (docCount
                  * (1d
                      - Math.pow(
                          (numDocs - pointCount) / points.size(), points.size() / docCount))));
    } else {
      assertEquals(Math.min(pointCount, numDocs), docCount);
    }
    r.close();
    dir.close();
  }

  public void testBasicWithPrefetchVisitor() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    // Avoid mockRandomMP since it may cause non-optimal merges that make the
    // number of points per leaf hard to predict
    while (iwc.getMergePolicy() instanceof MockRandomMergePolicy) {
      iwc.setMergePolicy(newMergePolicy());
    }
    IndexWriter w = new IndexWriter(dir, iwc);
    byte[] pointValue = new byte[3];
    byte[] uniquePointValue = new byte[3];
    random().nextBytes(uniquePointValue);
    final int numDocs =
        TEST_NIGHTLY ? atLeast(10000) : atLeast(500); // at night, make sure we have several leaves
    final boolean multiValues = random().nextBoolean();
    int totalValues = 0;
    for (int i = 0; i < numDocs; ++i) {
      Document doc = new Document();
      if (i == numDocs / 2) {
        totalValues++;
        doc.add(new BinaryPoint("f", uniquePointValue));
      } else {
        final int numValues = (multiValues) ? TestUtil.nextInt(random(), 2, 100) : 1;
        for (int j = 0; j < numValues; j++) {
          do {
            random().nextBytes(pointValue);
          } while (Arrays.equals(pointValue, uniquePointValue));
          doc.add(new BinaryPoint("f", pointValue));
          totalValues++;
        }
      }
      w.addDocument(doc);
    }
    w.forceMerge(1);
    final IndexReader r = DirectoryReader.open(w);
    w.close();

    final LeafReader lr = getOnlyLeafReader(r);
    PointValues points = lr.getPointValues("f");

    BKDReader.BaseTwoPhaseIntersectVisitor allPointsVisitor =
        new BKDReader.BaseTwoPhaseIntersectVisitor() {
          @Override
          public void visit(int docID, byte[] packedValue) throws IOException {}

          @Override
          public void visit(int docID) throws IOException {}

          @Override
          public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            return Relation.CELL_INSIDE_QUERY;
          }
        };

    List<Long> savedBlocks = allPointsVisitor.deferredBlocks();
    assertEquals(0, savedBlocks.size()); // Test that all deferred blocks were processed
    assertEquals(totalValues, points.estimatePointCount(allPointsVisitor));
    assertEquals(numDocs, points.estimateDocCount(allPointsVisitor));

    r.close();
    dir.close();
  }

  public void testBasicWithPrefetchCapableVisitor() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setMergePolicy(newLogMergePolicy());
    IndexWriter w = new IndexWriter(dir, iwc);
    byte[] point = new byte[4];
    for (int i = 0; i < 20; i++) {
      Document doc = new Document();
      NumericUtils.intToSortableBytes(i, point, 0);
      doc.add(new BinaryPoint("dim", point));
      w.addDocument(doc);
    }
    w.forceMerge(1);
    w.close();

    DirectoryReader r = DirectoryReader.open(dir);
    LeafReader sub = getOnlyLeafReader(r);
    PointValues values = sub.getPointValues("dim");

    // Simple test: make sure prefetch capable visitor can visit every doc when cell crosses query:
    BitSet seen = new BitSet();
    values.intersect(
        new BKDReader.BaseTwoPhaseIntersectVisitor() {
          @Override
          public Relation compare(byte[] minPacked, byte[] maxPacked) {
            return Relation.CELL_CROSSES_QUERY;
          }

          @Override
          public void visit(int docID) {
            throw new IllegalStateException();
          }

          @Override
          public void visit(int docID, byte[] packedValue) {
            seen.set(docID);
            assertEquals(docID, NumericUtils.sortableBytesToInt(packedValue, 0));
          }
        });
    assertEquals(20, seen.cardinality());
    // Make sure prefetch capable visitor can visit all docs when all docs are inside query
    // Also test we are not visiting documents twice based on whether PointTree has a prefetch
    // implementation of
    // prepareOrVisit or uses the default implementation
    seen.clear();
    final int[] docCount = {0};
    values.intersect(
        new BKDReader.BaseTwoPhaseIntersectVisitor() {
          @Override
          public void visit(int docID) throws IOException {
            seen.set(docID);
            docCount[0]++;
          }

          @Override
          public void visit(int docID, byte[] packedValue) throws IOException {}

          @Override
          public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            return Relation.CELL_INSIDE_QUERY;
          }
        });
    assertEquals(20, seen.cardinality());
    assertEquals(20, docCount[0]);
    IOUtils.close(r, dir);
  }
}
