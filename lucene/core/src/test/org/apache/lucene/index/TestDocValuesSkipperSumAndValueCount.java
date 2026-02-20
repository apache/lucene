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

import java.math.BigInteger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;

/** Tests for sum and valueCount in {@link DocValuesSkipper}. */
public class TestDocValuesSkipperSumAndValueCount extends LuceneTestCase {

  /** Helper to reconstruct a 128-bit BigInteger from high/low longs. */
  private static BigInteger toBigInteger(long high, long low) {
    return BigInteger.valueOf(high)
        .shiftLeft(64)
        .add(BigInteger.valueOf(low).and(new BigInteger("FFFFFFFFFFFFFFFF", 16)));
  }

  public void testSingleValuedNumericSum() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig();
    config.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, config);

    long expectedSum = 0;
    int numDocs = atLeast(500);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      long value = random().nextInt(1000);
      doc.add(NumericDocValuesField.indexedField("field", value));
      expectedSum += value;
      writer.addDocument(doc);
    }
    writer.forceMerge(1);

    DirectoryReader reader = writer.getReader();
    LeafReader leafReader = getOnlyLeafReader(reader);
    DocValuesSkipper skipper = leafReader.getDocValuesSkipper("field");
    assertNotNull(skipper);

    // Check global values
    assertEquals(numDocs, skipper.docCount());
    assertEquals(numDocs, skipper.valueCount());
    BigInteger globalSum = toBigInteger(skipper.sumHigh(), skipper.sumLow());
    assertEquals(BigInteger.valueOf(expectedSum), globalSum);

    // Walk through level 0 intervals and verify sum/valueCount add up
    long runningSum = 0;
    long runningValueCount = 0;
    int runningDocCount = 0;
    skipper.advance(0);
    while (skipper.minDocID(0) != DocIdSetIterator.NO_MORE_DOCS) {
      BigInteger intervalSum = toBigInteger(skipper.sumHigh(0), skipper.sumLow(0));
      long intervalValueCount = skipper.valueCount(0);
      int intervalDocCount = skipper.docCount(0);

      assertTrue("valueCount must be >= docCount", intervalValueCount >= intervalDocCount);
      runningSum += intervalSum.longValueExact();
      runningValueCount += intervalValueCount;
      runningDocCount += intervalDocCount;

      skipper.advance(skipper.maxDocID(0) + 1);
    }
    assertEquals(expectedSum, runningSum);
    assertEquals(numDocs, runningValueCount);
    assertEquals(numDocs, runningDocCount);

    reader.close();
    writer.close();
    dir.close();
  }

  public void testMultiValuedNumericSum() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig();
    config.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, config);

    long expectedSum = 0;
    long expectedValueCount = 0;
    int numDocs = atLeast(500);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      int numValues = random().nextInt(1, 5);
      for (int j = 0; j < numValues; j++) {
        long value = random().nextInt(1000);
        doc.add(SortedNumericDocValuesField.indexedField("field", value));
        expectedSum += value;
        expectedValueCount++;
      }
      writer.addDocument(doc);
    }
    writer.forceMerge(1);

    DirectoryReader reader = writer.getReader();
    LeafReader leafReader = getOnlyLeafReader(reader);
    DocValuesSkipper skipper = leafReader.getDocValuesSkipper("field");
    assertNotNull(skipper);

    // Global checks
    assertEquals(numDocs, skipper.docCount());
    assertEquals(expectedValueCount, skipper.valueCount());
    BigInteger globalSum = toBigInteger(skipper.sumHigh(), skipper.sumLow());
    assertEquals(BigInteger.valueOf(expectedSum), globalSum);

    // Walk level 0 and verify valueCount > docCount for multi-valued
    long totalValueCount = 0;
    skipper.advance(0);
    while (skipper.minDocID(0) != DocIdSetIterator.NO_MORE_DOCS) {
      totalValueCount += skipper.valueCount(0);
      skipper.advance(skipper.maxDocID(0) + 1);
    }
    assertEquals(expectedValueCount, totalValueCount);

    reader.close();
    writer.close();
    dir.close();
  }

  public void testSumWithNegativeValues() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig();
    config.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, config);

    long expectedSum = 0;
    int numDocs = atLeast(500);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      long value = random().nextLong(-1000, 1000);
      doc.add(NumericDocValuesField.indexedField("field", value));
      expectedSum += value;
      writer.addDocument(doc);
    }
    writer.forceMerge(1);

    DirectoryReader reader = writer.getReader();
    LeafReader leafReader = getOnlyLeafReader(reader);
    DocValuesSkipper skipper = leafReader.getDocValuesSkipper("field");
    assertNotNull(skipper);

    BigInteger globalSum = toBigInteger(skipper.sumHigh(), skipper.sumLow());
    assertEquals(BigInteger.valueOf(expectedSum), globalSum);

    reader.close();
    writer.close();
    dir.close();
  }

  public void testSumOverflow128Bit() throws Exception {
    // Test that 128-bit sum handles values near Long.MAX_VALUE without overflow
    Directory dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig();
    config.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, config);

    BigInteger expectedSum = BigInteger.ZERO;
    int numDocs = atLeast(500);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      // Use values near Long.MAX_VALUE to force overflow of a single long
      long value = Long.MAX_VALUE - random().nextInt(100);
      doc.add(NumericDocValuesField.indexedField("field", value));
      expectedSum = expectedSum.add(BigInteger.valueOf(value));
      writer.addDocument(doc);
    }
    writer.forceMerge(1);

    DirectoryReader reader = writer.getReader();
    LeafReader leafReader = getOnlyLeafReader(reader);
    DocValuesSkipper skipper = leafReader.getDocValuesSkipper("field");
    assertNotNull(skipper);

    BigInteger globalSum = toBigInteger(skipper.sumHigh(), skipper.sumLow());
    assertEquals(
        "128-bit sum should handle overflow beyond Long.MAX_VALUE", expectedSum, globalSum);

    // Verify sumHigh is non-zero (overflow actually happened)
    assertTrue(
        "sumHigh should be non-zero for large sums exceeding Long.MAX_VALUE",
        skipper.sumHigh() > 0);

    reader.close();
    writer.close();
    dir.close();
  }

  public void testSumOverflowNegative128Bit() throws Exception {
    // Test that 128-bit sum handles values near Long.MIN_VALUE
    Directory dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig();
    config.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, config);

    BigInteger expectedSum = BigInteger.ZERO;
    int numDocs = atLeast(500);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      long value = Long.MIN_VALUE + random().nextInt(100);
      doc.add(NumericDocValuesField.indexedField("field", value));
      expectedSum = expectedSum.add(BigInteger.valueOf(value));
      writer.addDocument(doc);
    }
    writer.forceMerge(1);

    DirectoryReader reader = writer.getReader();
    LeafReader leafReader = getOnlyLeafReader(reader);
    DocValuesSkipper skipper = leafReader.getDocValuesSkipper("field");
    assertNotNull(skipper);

    BigInteger globalSum = toBigInteger(skipper.sumHigh(), skipper.sumLow());
    assertEquals(
        "128-bit sum should handle negative overflow beyond Long.MIN_VALUE",
        expectedSum,
        globalSum);

    assertTrue("sumHigh should be negative for large negative sums", skipper.sumHigh() < 0);

    reader.close();
    writer.close();
    dir.close();
  }

  public void testSumAndValueCountConsistencyAcrossLevels() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig();
    config.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, config);

    // Need enough docs to get multiple levels in the skip index
    int numDocs = atLeast(10000);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      doc.add(NumericDocValuesField.indexedField("field", random().nextInt(10000)));
      writer.addDocument(doc);
    }
    writer.forceMerge(1);

    DirectoryReader reader = writer.getReader();
    LeafReader leafReader = getOnlyLeafReader(reader);
    DocValuesSkipper skipper = leafReader.getDocValuesSkipper("field");
    assertNotNull(skipper);

    // Advance and check that higher levels contain sums that are >= level 0 sums
    skipper.advance(0);
    while (skipper.minDocID(0) != DocIdSetIterator.NO_MORE_DOCS) {
      int numLevels = skipper.numLevels();
      for (int level = 1; level < numLevels; level++) {
        // Higher level should have >= valueCount than lower level
        assertTrue(
            "Higher level valueCount should be >= lower level",
            skipper.valueCount(level) >= skipper.valueCount(level - 1));
        assertTrue(
            "Higher level docCount should be >= lower level",
            skipper.docCount(level) >= skipper.docCount(level - 1));
      }
      skipper.advance(skipper.maxDocID(0) + 1);
    }

    reader.close();
    writer.close();
    dir.close();
  }

  public void testEmptyField() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, config);

    // Add docs without the field
    for (int i = 0; i < 100; i++) {
      writer.addDocument(new Document());
    }
    // Add one doc with the field to create the field info
    Document doc = new Document();
    doc.add(NumericDocValuesField.indexedField("field", 42));
    writer.addDocument(doc);
    writer.forceMerge(1);

    DirectoryReader reader = writer.getReader();
    LeafReader leafReader = getOnlyLeafReader(reader);
    DocValuesSkipper skipper = leafReader.getDocValuesSkipper("field");
    assertNotNull(skipper);

    assertEquals(1, skipper.docCount());
    assertEquals(1, skipper.valueCount());
    BigInteger globalSum = toBigInteger(skipper.sumHigh(), skipper.sumLow());
    assertEquals(BigInteger.valueOf(42), globalSum);

    reader.close();
    writer.close();
    dir.close();
  }

  public void testAllSameValues() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig();
    config.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, config);

    long value = random().nextLong(-1000, 1000);
    int numDocs = atLeast(500);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      doc.add(NumericDocValuesField.indexedField("field", value));
      writer.addDocument(doc);
    }
    writer.forceMerge(1);

    DirectoryReader reader = writer.getReader();
    LeafReader leafReader = getOnlyLeafReader(reader);
    DocValuesSkipper skipper = leafReader.getDocValuesSkipper("field");
    assertNotNull(skipper);

    BigInteger expectedSum = BigInteger.valueOf(value).multiply(BigInteger.valueOf(numDocs));
    BigInteger globalSum = toBigInteger(skipper.sumHigh(), skipper.sumLow());
    assertEquals(expectedSum, globalSum);
    assertEquals(numDocs, skipper.valueCount());

    reader.close();
    writer.close();
    dir.close();
  }

  public void testIntervalSumsAddUpToGlobal() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig();
    config.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, config);

    int numDocs = atLeast(5000);
    BigInteger expectedSum = BigInteger.ZERO;
    long expectedValueCount = 0;
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      int numValues = random().nextInt(1, 4);
      for (int j = 0; j < numValues; j++) {
        long value = random().nextLong();
        doc.add(SortedNumericDocValuesField.indexedField("field", value));
        expectedSum = expectedSum.add(BigInteger.valueOf(value));
        expectedValueCount++;
      }
      writer.addDocument(doc);
    }
    writer.forceMerge(1);

    DirectoryReader reader = writer.getReader();
    LeafReader leafReader = getOnlyLeafReader(reader);
    DocValuesSkipper skipper = leafReader.getDocValuesSkipper("field");
    assertNotNull(skipper);

    // Walk level 0 intervals and sum them up
    BigInteger intervalSumTotal = BigInteger.ZERO;
    long intervalValueCountTotal = 0;
    skipper.advance(0);
    while (skipper.minDocID(0) != DocIdSetIterator.NO_MORE_DOCS) {
      intervalSumTotal = intervalSumTotal.add(toBigInteger(skipper.sumHigh(0), skipper.sumLow(0)));
      intervalValueCountTotal += skipper.valueCount(0);
      skipper.advance(skipper.maxDocID(0) + 1);
    }

    BigInteger globalSum = toBigInteger(skipper.sumHigh(), skipper.sumLow());
    assertEquals("Level 0 interval sums must equal global sum", globalSum, intervalSumTotal);
    assertEquals(
        "Level 0 interval valueCounts must equal global valueCount",
        skipper.valueCount(),
        intervalValueCountTotal);
    assertEquals(expectedSum, globalSum);
    assertEquals(expectedValueCount, skipper.valueCount());

    reader.close();
    writer.close();
    dir.close();
  }
}
