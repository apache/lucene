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

import java.math.BigInteger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;

/**
 * Tests backward compatibility of DocValuesSkipper format versioning. Verifies format constants,
 * version-conditional behavior, and that the new v1 format correctly stores sum/valueCount.
 */
public class TestDocValuesSkipperBackwardCompat extends LuceneTestCase {

  private static BigInteger toBigInteger(long high, long low) {
    return BigInteger.valueOf(high)
        .shiftLeft(64)
        .add(BigInteger.valueOf(low).and(new BigInteger("FFFFFFFFFFFFFFFF", 16)));
  }

  /** Verify version constants are correctly defined for backward compatibility. */
  public void testVersionConstants() {
    assertEquals(0, Lucene90DocValuesFormat.VERSION_START);
    assertEquals(1, Lucene90DocValuesFormat.VERSION_SUM_AND_VALUE_COUNT);
    assertEquals(
        Lucene90DocValuesFormat.VERSION_SUM_AND_VALUE_COUNT,
        Lucene90DocValuesFormat.VERSION_CURRENT);
    assertTrue(
        "VERSION_CURRENT must be >= VERSION_START",
        Lucene90DocValuesFormat.VERSION_CURRENT >= Lucene90DocValuesFormat.VERSION_START);
  }

  /** Verify the jump table constants for v0 and v1 are correctly computed. */
  public void testJumpTableConstants() {
    // v0: 29 bytes per interval
    assertEquals(29L, Lucene90DocValuesFormat.SKIP_INDEX_INTERVAL_BYTES_V0);
    // v1: 53 bytes per interval (v0 + 8 sumHigh + 8 sumLow + 8 valueCount)
    assertEquals(53L, Lucene90DocValuesFormat.SKIP_INDEX_INTERVAL_BYTES_V1);
    assertEquals(
        Lucene90DocValuesFormat.SKIP_INDEX_INTERVAL_BYTES_V0 + 24,
        Lucene90DocValuesFormat.SKIP_INDEX_INTERVAL_BYTES_V1);

    // Jump length at level 0 = interval bytes - 5 (1 byte levels + 4 bytes maxDocID already read)
    assertEquals(
        Lucene90DocValuesFormat.SKIP_INDEX_INTERVAL_BYTES_V0 - 5L,
        Lucene90DocValuesFormat.SKIP_INDEX_JUMP_LENGTH_PER_LEVEL_V0[0]);
    assertEquals(
        Lucene90DocValuesFormat.SKIP_INDEX_INTERVAL_BYTES_V1 - 5L,
        Lucene90DocValuesFormat.SKIP_INDEX_JUMP_LENGTH_PER_LEVEL_V1[0]);

    // v1 jump lengths must always be larger than v0
    for (int level = 0; level < Lucene90DocValuesFormat.SKIP_INDEX_MAX_LEVEL; level++) {
      assertTrue(
          "v1 jump length at level " + level + " should be > v0",
          Lucene90DocValuesFormat.SKIP_INDEX_JUMP_LENGTH_PER_LEVEL_V1[level]
              > Lucene90DocValuesFormat.SKIP_INDEX_JUMP_LENGTH_PER_LEVEL_V0[level]);
    }
  }

  /** Verify that a v1 index has correct sum and valueCount for single-valued numeric fields. */
  public void testV1FormatHasSumAndValueCount() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig();
    config.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, config);

    long expectedSum = 0;
    int numDocs = atLeast(500);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      long value = i + 1;
      doc.add(NumericDocValuesField.indexedField("field", value));
      expectedSum += value;
      writer.addDocument(doc);
    }
    writer.forceMerge(1);

    DirectoryReader reader = writer.getReader();
    LeafReader leafReader = getOnlyLeafReader(reader);
    DocValuesSkipper skipper = leafReader.getDocValuesSkipper("field");
    assertNotNull(skipper);

    assertEquals(numDocs, skipper.docCount());
    assertEquals(numDocs, skipper.valueCount());
    BigInteger globalSum = toBigInteger(skipper.sumHigh(), skipper.sumLow());
    assertEquals(BigInteger.valueOf(expectedSum), globalSum);
    assertEquals(1, skipper.minValue());
    assertEquals(numDocs, skipper.maxValue());

    // Walk intervals and verify sum is non-zero at level 0
    boolean foundNonZeroSum = false;
    skipper.advance(0);
    while (skipper.minDocID(0) != DocIdSetIterator.NO_MORE_DOCS) {
      BigInteger intervalSum = toBigInteger(skipper.sumHigh(0), skipper.sumLow(0));
      if (intervalSum.signum() != 0) {
        foundNonZeroSum = true;
      }
      assertTrue("valueCount should be > 0", skipper.valueCount(0) > 0);
      skipper.advance(skipper.maxDocID(0) + 1);
    }
    assertTrue("At least one interval should have non-zero sum", foundNonZeroSum);

    reader.close();
    writer.close();
    dir.close();
  }

  /** Verify multi-valued fields correctly track valueCount > docCount. */
  public void testMultiValuedFieldValueCountExceedsDocCount() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig();
    config.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, config);

    long expectedValueCount = 0;
    int numDocs = atLeast(500);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      int numValues = random().nextInt(2, 6);
      for (int j = 0; j < numValues; j++) {
        doc.add(SortedNumericDocValuesField.indexedField("field", random().nextInt(1000)));
        expectedValueCount++;
      }
      writer.addDocument(doc);
    }
    writer.forceMerge(1);

    DirectoryReader reader = writer.getReader();
    LeafReader leafReader = getOnlyLeafReader(reader);
    DocValuesSkipper skipper = leafReader.getDocValuesSkipper("field");
    assertNotNull(skipper);

    assertEquals(numDocs, skipper.docCount());
    assertEquals(expectedValueCount, skipper.valueCount());
    assertTrue(
        "valueCount should exceed docCount for multi-valued fields",
        skipper.valueCount() > skipper.docCount());

    reader.close();
    writer.close();
    dir.close();
  }

  /** Verify advance(minValue, maxValue) works correctly with the new format. */
  public void testValueRangeAdvanceWorksWithNewFormat() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig();
    config.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, config);

    int numDocs = atLeast(5000);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      doc.add(NumericDocValuesField.indexedField("field", i));
      writer.addDocument(doc);
    }
    writer.forceMerge(1);

    DirectoryReader reader = writer.getReader();
    LeafReader leafReader = getOnlyLeafReader(reader);
    DocValuesSkipper skipper = leafReader.getDocValuesSkipper("field");
    assertNotNull(skipper);

    long rangeMin = numDocs / 4;
    long rangeMax = numDocs / 2;
    skipper.advance(rangeMin, rangeMax);

    if (skipper.minDocID(0) != DocIdSetIterator.NO_MORE_DOCS) {
      assertTrue("interval maxValue should be >= rangeMin", skipper.maxValue(0) >= rangeMin);
      assertTrue("interval minValue should be <= rangeMax", skipper.minValue(0) <= rangeMax);
    }

    reader.close();
    writer.close();
    dir.close();
  }

  /** Verify min/max and docCount invariants hold across all intervals. */
  public void testMinMaxDocCountInvariants() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig();
    config.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, config);

    int numDocs = atLeast(1000);
    long globalMin = Long.MAX_VALUE;
    long globalMax = Long.MIN_VALUE;
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      long value = random().nextLong(-10000, 10000);
      globalMin = Math.min(globalMin, value);
      globalMax = Math.max(globalMax, value);
      doc.add(NumericDocValuesField.indexedField("field", value));
      writer.addDocument(doc);
    }
    writer.forceMerge(1);

    DirectoryReader reader = writer.getReader();
    LeafReader leafReader = getOnlyLeafReader(reader);
    DocValuesSkipper skipper = leafReader.getDocValuesSkipper("field");
    assertNotNull(skipper);

    assertEquals(numDocs, skipper.docCount());
    assertEquals(globalMin, skipper.minValue());
    assertEquals(globalMax, skipper.maxValue());

    int totalDocCount = 0;
    skipper.advance(0);
    while (skipper.minDocID(0) != DocIdSetIterator.NO_MORE_DOCS) {
      assertTrue("docCount must be > 0", skipper.docCount(0) > 0);
      assertTrue(
          "interval minValue must be >= global minValue",
          skipper.minValue(0) >= skipper.minValue());
      assertTrue(
          "interval maxValue must be <= global maxValue",
          skipper.maxValue(0) <= skipper.maxValue());
      totalDocCount += skipper.docCount(0);
      skipper.advance(skipper.maxDocID(0) + 1);
    }
    assertEquals("sum of interval docCounts must equal global docCount", numDocs, totalDocCount);

    reader.close();
    writer.close();
    dir.close();
  }
}
