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
package org.apache.lucene.util.bkd;

import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import org.apache.lucene.codecs.MutablePointValues;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

/**
 * This testcase will benchmark with different bytesPerDim and isDocIdIncremental parameters, report
 * will be generated like below:
 *
 * <pre>
 *  -------------------------------------------------
 * | bytesPerDim | isDocIdIncremental | avg time(us) |
 *  -------------------------------------------------
 * |      1      |         N          |   1167819.5  |
 * |      1      |         Y          |     60118.3  |
 * |      2      |         N          |   1143350.6  |
 * |      2      |         Y          |    338069.0  |
 * |      3      |         N          |   1508473.6  |
 * |      3      |         Y          |    901047.8  |
 * |      4      |         N          |   1465402.0  |
 * |      4      |         Y          |   1418225.6  |
 * |      8      |         N          |   1505255.3  |
 * |      8      |         Y          |   1413590.8  |
 *  -------------------------------------------------
 * </pre>
 */
@TimeoutSuite(millis = Integer.MAX_VALUE)
// @LuceneTestCase.Monster("takes minutes to finish")
public class TestBKDDisableSortDocId extends LuceneTestCase {

  private List<Integer> docIdList = new ArrayList<>();

  public void testBenchmark() throws Exception {
    System.out.println("warm up");
    doTestBenchmark(4, false);
    doTestBenchmark(4, true);

    System.out.println("start benchmark");
    System.out.println(" -------------------------------------------------");
    System.out.println("| bytesPerDim | isDocIdIncremental | avg time(us) |");
    System.out.println(" -------------------------------------------------");
    int[] bytesPerDimArray = {1, 2, 3, 4, 8, 16, 32};
    for (int i = 0; i < bytesPerDimArray.length; i++) {
      doTestBenchmark(bytesPerDimArray[i], false);
      doTestBenchmark(bytesPerDimArray[i], true);
    }
    System.out.println(" -------------------------------------------------");
  }

  public void testBenchmarkWithLeadingZeroBytes() throws Exception {
    System.out.println("warm up");
    doTestBenchmark(4, false);
    doTestBenchmark(4, true);

    int bytesPerDim = 4;
    int[] leadingZeroByteNum = {1, 2, 3};
    System.out.println("start benchmark");
    System.out.println("bytesPerDim=" + bytesPerDim + ", leadingZeroByteNum=[1,2,3]");
    System.out.println(" -------------------------------------------------");
    System.out.println("| bytesPerDim | isDocIdIncremental | avg time(us) |");
    System.out.println(" -------------------------------------------------");
    for (int i = 0; i < leadingZeroByteNum.length; i++) {
      doTestBenchmark(bytesPerDim, false, leadingZeroByteNum[i]);
      doTestBenchmark(bytesPerDim, true, leadingZeroByteNum[i]);
    }
    System.out.println(" -------------------------------------------------");
  }

  public void doTestBenchmark(int bytesPerDim, boolean isDocIdIncremental) throws Exception {
    doTestBenchmark(bytesPerDim, isDocIdIncremental, 0);
  }

  public void doTestBenchmark(int bytesPerDim, boolean isDocIdIncremental, int leadingZeroByteNum)
      throws Exception {
    int runTimes = 10;
    int docNum = 2000000;

    System.gc();
    Thread.sleep(5000);

    long sortDocIdTotalTime = 0L;
    for (int i = 0; i < runTimes; i++) {
      sortDocIdTotalTime += doTestSort(bytesPerDim, docNum, isDocIdIncremental, leadingZeroByteNum);
      Thread.sleep(1000);
    }

    System.out.println(
        String.format(
            Locale.ROOT,
            "|     %2d      |         %s          |  %10.1f  |",
            bytesPerDim,
            (isDocIdIncremental ? "Y" : "N"),
            sortDocIdTotalTime * 1.0f / runTimes / 1000.0f));
  }

  public void testLongRunBenchmark() throws Exception {
    boolean isDocIdIncremental = true;
    int runTimes = 100;
    int bytesPerDim = 3;
    int docNum = 2000000;
    for (int i = 0; i < runTimes; i++) {
      doTestSort(bytesPerDim, docNum, isDocIdIncremental, 0);
    }
  }

  private long doTestSort(
      int bytesPerDim, int docNum, boolean isDocIdIncremental, int leadingZeroByteNum) {
    final int maxDoc = docNum;
    BKDConfig config = new BKDConfig(1, 1, bytesPerDim, BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE);
    Point[] points =
        createRandomPoints(config, maxDoc, new int[1], isDocIdIncremental, leadingZeroByteNum);
    DummyPointsReader reader = new DummyPointsReader(points);
    long start = System.nanoTime();
    MutablePointsReaderUtils.sort(config, maxDoc, reader, 0, points.length);
    long end = System.nanoTime();
    return end - start;
  }

  private Point[] createRandomPoints(
      BKDConfig config,
      int maxDoc,
      int[] commonPrefixLengths,
      boolean isDocIdIncremental,
      int leadingZeroByteNum) {
    assertTrue(commonPrefixLengths.length == config.numDims);
    assertTrue(leadingZeroByteNum < config.packedBytesLength);
    final int numPoints = maxDoc;
    Point[] points = new Point[numPoints];
    if (docIdList.isEmpty()) {
      for (int i = 0; i < maxDoc; i++) {
        docIdList.add(i);
      }
    }
    Collections.shuffle(docIdList, random());
    for (int i = 0; i < numPoints; ++i) {
      byte[] value = new byte[config.packedBytesLength];
      if (leadingZeroByteNum == 0) {
        random().nextBytes(value);
      } else {
        byte[] suffix = new byte[config.packedBytesLength - leadingZeroByteNum];
        random().nextBytes(suffix);
        System.arraycopy(suffix, 0, value, leadingZeroByteNum, suffix.length);
      }
      points[i] = new Point(value, isDocIdIncremental ? i : docIdList.get(i));
    }
    return points;
  }

  static class Point {
    final BytesRef packedValue;
    final int doc;

    Point(byte[] packedValue, int doc) {
      // use a non-null offset to make sure MutablePointsReaderUtils does not ignore it
      this.packedValue = new BytesRef(packedValue.length + 1);
      this.packedValue.offset = 1;
      this.packedValue.length = packedValue.length;
      System.arraycopy(packedValue, 0, this.packedValue.bytes, 0, packedValue.length);
      this.doc = doc;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || obj instanceof Point == false) {
        return false;
      }
      Point that = (Point) obj;
      return packedValue.equals(that.packedValue) && doc == that.doc;
    }

    @Override
    public int hashCode() {
      return 31 * packedValue.hashCode() + doc;
    }

    @Override
    public String toString() {
      return "value=" + packedValue + " doc=" + doc;
    }
  }

  static class DummyPointsReader extends MutablePointValues {

    private final Point[] points;

    private Point[] temp;

    DummyPointsReader(Point[] points) {
      this.points = points.clone();
    }

    @Override
    public void getValue(int i, BytesRef packedValue) {
      packedValue.bytes = points[i].packedValue.bytes;
      packedValue.offset = points[i].packedValue.offset;
      packedValue.length = points[i].packedValue.length;
    }

    @Override
    public byte getByteAt(int i, int k) {
      BytesRef packedValue = points[i].packedValue;
      return packedValue.bytes[packedValue.offset + k];
    }

    @Override
    public int getDocID(int i) {
      return points[i].doc;
    }

    @Override
    public void swap(int i, int j) {
      ArrayUtil.swap(points, i, j);
    }

    @Override
    public void intersect(IntersectVisitor visitor) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long estimatePointCount(IntersectVisitor visitor) {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getMinPackedValue() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getMaxPackedValue() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getNumDimensions() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getNumIndexDimensions() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getBytesPerDimension() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long size() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getDocCount() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void assign(int from, int to) {
      if (temp == null) {
        temp = new Point[points.length];
      }
      temp[to] = points[from];
    }

    @Override
    public void finalizeAssign(int from, int to) {
      if (temp != null) {
        System.arraycopy(temp, from, points, from, to - from);
      }
    }
  }
}
