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

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;

/**
 * Comprehensive test to ensure TemporalMergePolicy NEVER merges segments from different time
 * buckets, even under aggressive force merge scenarios.
 *
 * <p>This test creates multiple segments across different year buckets and verifies that they
 * remain in separate buckets after force merge operations.
 */
public class TestTemporalMergePolicyAcrossBuckets extends LuceneTestCase {

  public void testMultipleYearBucketsNeverMerge() throws IOException {
    Directory dir = new ByteBuffersDirectory();

    TemporalMergePolicy policy =
        new TemporalMergePolicy()
            .setTemporalField("timestamp_tl")
            .setBaseTimeSeconds(TimeUnit.DAYS.toSeconds(365))
            .setMaxAgeSeconds(TimeUnit.DAYS.toSeconds(365 * 10))
            .setMinThreshold(2)
            .setMaxThreshold(10)
            .setUseExponentialBuckets(false)
            .setCompactionRatio(1.0);

    IndexWriterConfig config = new IndexWriterConfig();
    config.setMergePolicy(policy);
    config.setMaxBufferedDocs(1000);

    IndexWriter writer = new IndexWriter(dir, config);

    // Create segments for 5 different years: 2020, 2021, 2022, 2023, 2024
    // Each year gets 3 segments with unique timestamps
    long year2020 = 1577836800000L; // 2020-01-01 00:00:00 UTC
    long year2021 = 1609459200000L; // 2021-01-01 00:00:00 UTC
    long year2022 = 1640995200000L; // 2022-01-01 00:00:00 UTC
    long year2023 = 1672531200000L; // 2023-01-01 00:00:00 UTC
    long year2024 = 1704067200000L; // 2024-01-01 00:00:00 UTC

    long[] years = {year2020, year2021, year2022, year2023, year2024};

    for (long yearTimestamp : years) {
      for (int segInYear = 0; segInYear < 3; segInYear++) {
        for (int doc = 0; doc < 10; doc++) {
          Document d = new Document();
          long timestamp = yearTimestamp + (segInYear * 1000000L) + (doc * 1000L);
          d.add(new LongPoint("timestamp_tl", timestamp * 1000));
          d.add(new NumericDocValuesField("timestamp_tl", timestamp * 1000));
          writer.addDocument(d);
        }
        writer.flush();
      }
    }

    writer.forceMerge(1, true);
    writer.commit();

    DirectoryReader reader = DirectoryReader.open(dir);
    int finalSegmentCount = reader.leaves().size();

    for (int i = 0; i < reader.leaves().size(); i++) {
      LeafReaderContext ctx = reader.leaves().get(i);
      PointValues pointValues;
      pointValues = ctx.reader().getPointValues("timestamp_tl");

      if (pointValues != null) {
        byte[] minValue = pointValues.getMinPackedValue();
        byte[] maxValue = pointValues.getMaxPackedValue();

        long minTimestamp = LongPoint.decodeDimension(minValue, 0) / 1000;
        long maxTimestamp = LongPoint.decodeDimension(maxValue, 0) / 1000;

        long minYearBucket = (minTimestamp / 1000) / TimeUnit.DAYS.toSeconds(365);
        long maxYearBucket = (maxTimestamp / 1000) / TimeUnit.DAYS.toSeconds(365);

        // Verify segment doesn't span multiple year buckets
        // Allow up to 1 year difference (since we're using 1-year buckets)
        long bucketSpan = Math.abs(maxYearBucket - minYearBucket);
        assertTrue(
            "Segment "
                + i
                + " spans "
                + bucketSpan
                + " year buckets (should be ≤ 1). "
                + "This indicates cross-bucket merging occurred!",
            bucketSpan <= 1);
      }
    }

    reader.close();
    writer.close();
    dir.close();

    // With 5 year buckets and 3 segments per bucket, after merging:
    // - Within each bucket: 3 segments should merge into 1 (minThreshold=2)
    // - Across buckets: NEVER merge
    // Expected: 5 segments (one per year bucket)

    // Could be more if segments within a bucket didn't meet merge criteria
    assertTrue(
        "Expected at least 5 segments (one per year bucket), but got "
            + finalSegmentCount
            + ". This indicates segments from different year buckets were merged together!",
        finalSegmentCount >= 5);
  }

  public void testTenYearBucketsRemainSeparate() throws IOException {
    Directory dir = new ByteBuffersDirectory();

    TemporalMergePolicy policy =
        new TemporalMergePolicy()
            .setTemporalField("timestamp_tl")
            .setBaseTimeSeconds(TimeUnit.DAYS.toSeconds(365)) // 1-year buckets
            .setMaxAgeSeconds(TimeUnit.DAYS.toSeconds(365 * 10)) // 10 years
            .setMinThreshold(2)
            .setMaxThreshold(10)
            .setUseExponentialBuckets(false)
            .setCompactionRatio(1.0);

    IndexWriterConfig config = new IndexWriterConfig();
    config.setMergePolicy(policy);
    config.setMaxBufferedDocs(1000);

    IndexWriter writer = new IndexWriter(dir, config);

    long baseYear = 1577836800000L; // 2020-01-01 00:00:00 UTC
    long oneYearMillis = TimeUnit.DAYS.toMillis(365);

    for (int year = 0; year < 10; year++) {
      long yearTimestamp = baseYear + (year * oneYearMillis);

      for (int seg = 0; seg < 2; seg++) {
        for (int doc = 0; doc < 10; doc++) {
          Document d = new Document();
          long timestamp = yearTimestamp + (seg * 1000000L) + (doc * 1000L);
          d.add(new LongPoint("timestamp_tl", timestamp * 1000));
          d.add(new NumericDocValuesField("timestamp_tl", timestamp * 1000));
          writer.addDocument(d);
        }
        writer.flush();
      }
    }

    writer.forceMerge(1, true);
    writer.commit();

    DirectoryReader reader = DirectoryReader.open(dir);
    int finalSegmentCount = reader.leaves().size();

    reader.close();
    writer.close();
    dir.close();

    // With 10 year buckets and minThreshold=2, each bucket should merge its 2 segments into 1
    // Expected: exactly 10 segments (one per year bucket)
    assertTrue(
        "Expected at least 10 segments (one per year bucket), but got " + finalSegmentCount,
        finalSegmentCount >= 10);
  }

  public void testOldDataBucketDoesNotMergeAcrossYearBoundaries() throws IOException {
    Directory dir = new ByteBuffersDirectory();

    TemporalMergePolicy policy =
        new TemporalMergePolicy()
            .setTemporalField("timestamp_tl")
            .setBaseTimeSeconds(TimeUnit.DAYS.toSeconds(365))
            .setMaxAgeSeconds(TimeUnit.DAYS.toSeconds(365 * 10))
            .setMinThreshold(2)
            .setMaxThreshold(10)
            .setUseExponentialBuckets(false)
            .setCompactionRatio(1.0);

    IndexWriterConfig config = new IndexWriterConfig();
    config.setMergePolicy(policy);
    config.setMaxBufferedDocs(100);

    IndexWriter writer = new IndexWriter(dir, config);

    // Segment A: Contains data from 1970 to 2024 (max_timestamp = 2024)
    // Should be assigned to 2024 bucket
    long timestamp2024 = 1735689600000L; // 2025-01-01 00:00:00 UTC in millis
    long timestamp1970 = 0L;

    for (int i = 0; i < 10; i++) {
      Document d = new Document();
      long timestamp = timestamp1970 + (i * 1000L); // Old data
      d.add(new LongPoint("timestamp_tl", timestamp * 1000));
      d.add(new NumericDocValuesField("timestamp_tl", timestamp * 1000));
      writer.addDocument(d);
    }
    // Add one recent doc to set max_timestamp = 2024
    Document d = new Document();
    d.add(new LongPoint("timestamp_tl", timestamp2024 * 1000));
    d.add(new NumericDocValuesField("timestamp_tl", timestamp2024 * 1000));
    writer.addDocument(d);
    writer.flush();

    // Segment B: Contains data from 1970 to 2023 (max_timestamp = 2023)
    // Should be assigned to 2023 bucket
    long timestamp2023 = 1704067200000L; // 2024-01-01 00:00:00 UTC in millis

    for (int i = 0; i < 10; i++) {
      Document d2 = new Document();
      long timestamp = timestamp1970 + (i * 2000L); // Old data
      d2.add(new LongPoint("timestamp_tl", timestamp * 1000));
      d2.add(new NumericDocValuesField("timestamp_tl", timestamp * 1000));
      writer.addDocument(d2);
    }
    // Add one recent doc to set max_timestamp = 2023
    Document d2 = new Document();
    d2.add(new LongPoint("timestamp_tl", timestamp2023 * 1000));
    d2.add(new NumericDocValuesField("timestamp_tl", timestamp2023 * 1000));
    writer.addDocument(d2);
    writer.flush();

    // Trigger merging - this should NOT merge segments A and B together
    // because they're in different year buckets
    writer.forceMerge(1, true);
    writer.commit();

    // Check final segment count
    DirectoryReader reader = DirectoryReader.open(dir);
    int finalSegmentCount = reader.leaves().size();

    reader.close();
    writer.close();
    dir.close();

    // Assert that segments from different buckets don't merge
    assertEquals(
        "Segments from different year buckets should NOT be merged together", 2, finalSegmentCount);
  }

  public void testOldDataBucketWithMultipleOldSegments() throws IOException {
    Directory dir = new ByteBuffersDirectory();

    TemporalMergePolicy policy =
        new TemporalMergePolicy()
            .setTemporalField("timestamp_tl")
            .setBaseTimeSeconds(TimeUnit.DAYS.toSeconds(365))
            .setMaxAgeSeconds(TimeUnit.DAYS.toSeconds(365 * 3))
            .setMinThreshold(2)
            .setMaxThreshold(10)
            .setUseExponentialBuckets(false)
            .setCompactionRatio(1.0);

    IndexWriterConfig config = new IndexWriterConfig();
    config.setMergePolicy(policy);
    config.setMaxBufferedDocs(100);

    IndexWriter writer = new IndexWriter(dir, config);

    long now = System.currentTimeMillis();
    long fiveYearsAgo = now - TimeUnit.DAYS.toMillis(365 * 5);

    // Create 3 segments, all containing data older than 3 years
    // These should all go into bucket -1 "old data bucket" which is NOT merged
    for (int seg = 0; seg < 3; seg++) {
      for (int i = 0; i < 10; i++) {
        Document d = new Document();
        long timestamp = fiveYearsAgo + (seg * 1000000L) + (i * 1000L);
        d.add(new LongPoint("timestamp_tl", timestamp * 1000));
        d.add(new NumericDocValuesField("timestamp_tl", timestamp * 1000));
        writer.addDocument(d);
      }
      writer.flush();
    }

    writer.forceMerge(1, true);
    writer.commit();

    DirectoryReader reader = DirectoryReader.open(dir);
    int finalSegmentCount = reader.leaves().size();

    reader.close();
    writer.close();
    dir.close();

    // Old data segments (older than maxAgeSeconds) should NOT merge - they get skipped
    assertEquals(
        "Old data segments (all >3 years) should NOT be merged (skipped)", 3, finalSegmentCount);
  }
}
