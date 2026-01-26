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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;

public class TestTemporalMergePolicy extends LuceneTestCase {

  public void testSkipsMergeWhenCompactionRatioNotMet() {
    TemporalMergePolicy policy = buildPolicy();
    policy.setMinThreshold(2).setMaxThreshold(4).setCompactionRatio(2.0d);

    SegmentCommitInfo large = newSegment("large", 1000);
    SegmentCommitInfo smallOne = newSegment("small1", 100);
    SegmentCommitInfo smallTwo = newSegment("small2", 100);

    Map<SegmentCommitInfo, TemporalMergePolicy.SegmentDateRange> ranges = new HashMap<>();
    ranges.put(large, new TemporalMergePolicy.SegmentDateRange(0, 3000));
    ranges.put(smallOne, new TemporalMergePolicy.SegmentDateRange(0, 2000));
    ranges.put(smallTwo, new TemporalMergePolicy.SegmentDateRange(0, 1000));

    Map<Long, List<SegmentCommitInfo>> buckets = new TreeMap<>();
    buckets.put(0L, Arrays.asList(large, smallOne, smallTwo));

    MergePolicy.MergeSpecification spec = policy.findMergeCandidates(buckets, ranges);
    assertNull("Merge should wait for more small segments to accumulate", spec);
  }

  public void testMergesSubsetWhenRatioSatisfied() {
    TemporalMergePolicy policy = buildPolicy();
    policy.setMinThreshold(2).setMaxThreshold(3).setCompactionRatio(1.6d);

    SegmentCommitInfo segA = newSegment("segA", 400);
    SegmentCommitInfo segB = newSegment("segB", 200);
    SegmentCommitInfo segC = newSegment("segC", 200);
    SegmentCommitInfo segD = newSegment("segD", 200);
    SegmentCommitInfo segE = newSegment("segE", 200);

    List<SegmentCommitInfo> segments = Arrays.asList(segA, segB, segC, segD, segE);
    Map<SegmentCommitInfo, TemporalMergePolicy.SegmentDateRange> ranges = new HashMap<>();
    long timestamp = 5000;
    for (SegmentCommitInfo segment : segments) {
      ranges.put(segment, new TemporalMergePolicy.SegmentDateRange(timestamp - 100, timestamp));
      timestamp -= 100;
    }

    Map<Long, List<SegmentCommitInfo>> buckets = new TreeMap<>();
    buckets.put(0L, segments);

    MergePolicy.MergeSpecification spec = policy.findMergeCandidates(buckets, ranges);
    assertNotNull(spec);
    assertEquals(2, spec.merges.size());

    assertEquals(3, spec.merges.get(0).segments.size());
    assertEquals(2, spec.merges.get(1).segments.size());
  }

  public void testForcedMergeReducesSegments() throws Exception {
    TemporalMergePolicy policy =
        buildPolicy().setUseExponentialBuckets(false).setBaseTimeSeconds(10);
    SegmentInfos infos = new SegmentInfos(Version.LATEST.major);
    SegmentCommitInfo segA = newSegment("forceA", 400);
    SegmentCommitInfo segB = newSegment("forceB", 300);
    SegmentCommitInfo segC = newSegment("forceC", 200);

    infos.add(segA);
    infos.add(segB);
    infos.add(segC);

    Map<SegmentCommitInfo, Boolean> segmentsToMerge = new HashMap<>();
    segmentsToMerge.put(segA, Boolean.TRUE);
    segmentsToMerge.put(segB, Boolean.TRUE);
    segmentsToMerge.put(segC, Boolean.TRUE);

    Map<SegmentCommitInfo, TemporalMergePolicy.SegmentDateRange> ranges = new HashMap<>();
    ranges.put(segA, range(10000));
    ranges.put(segB, range(11000));
    ranges.put(segC, range(12000));
    policy.setSegmentDateRangeOverrides(ranges);

    MergePolicy.MergeSpecification spec =
        policy.findForcedMerges(infos, 1, segmentsToMerge, new StubMergeContext());
    assertNotNull(spec);
    assertFalse(spec.merges.isEmpty());
    assertEquals(1, spec.merges.size());
    assertEquals(3, spec.merges.getFirst().segments.size());
  }

  public void testForcedDeletesMergesRespectThreshold() throws Exception {
    TemporalMergePolicy policy =
        buildPolicy()
            .setForceMergeDeletesPctAllowed(5.0d)
            .setUseExponentialBuckets(false)
            .setBaseTimeSeconds(10);

    SegmentInfos infos = new SegmentInfos(Version.LATEST.major);
    SegmentCommitInfo segA = newSegment("delA", 100);
    SegmentCommitInfo segB = newSegment("delB", 100);
    SegmentCommitInfo segC = newSegment("delC", 100);
    infos.add(segA);
    infos.add(segB);
    infos.add(segC);

    Map<SegmentCommitInfo, Integer> deletes = new HashMap<>();
    deletes.put(segA, 10);
    deletes.put(segB, 1);
    deletes.put(segC, 20);

    Map<SegmentCommitInfo, TemporalMergePolicy.SegmentDateRange> ranges = new HashMap<>();
    ranges.put(segA, range(10000));
    ranges.put(segB, range(11000));
    ranges.put(segC, range(12000));
    policy.setSegmentDateRangeOverrides(ranges);

    MergePolicy.MergeSpecification spec =
        policy.findForcedDeletesMerges(infos, new StubMergeContext(deletes));

    assertNotNull(spec);
    assertEquals(1, spec.merges.size());
    assertEquals(2, spec.merges.getFirst().segments.size());
  }

  public void testForcedMergeKeepsWindowsDisjoint() throws Exception {
    TemporalMergePolicy policy =
        buildPolicy().setUseExponentialBuckets(false).setBaseTimeSeconds(10);

    SegmentInfos infos = new SegmentInfos(Version.LATEST.major);
    SegmentCommitInfo segA1 = newSegment("bucketA1", 100);
    SegmentCommitInfo segA2 = newSegment("bucketA2", 100);
    SegmentCommitInfo segB1 = newSegment("bucketB1", 100);
    SegmentCommitInfo segB2 = newSegment("bucketB2", 100);

    infos.add(segA1);
    infos.add(segA2);
    infos.add(segB1);
    infos.add(segB2);

    Map<SegmentCommitInfo, Boolean> segmentsToMerge = new HashMap<>();
    segmentsToMerge.put(segA1, Boolean.TRUE);
    segmentsToMerge.put(segA2, Boolean.TRUE);
    segmentsToMerge.put(segB1, Boolean.TRUE);
    segmentsToMerge.put(segB2, Boolean.TRUE);

    Map<SegmentCommitInfo, TemporalMergePolicy.SegmentDateRange> ranges = new HashMap<>();
    ranges.put(segA1, range(5000));
    ranges.put(segA2, range(6000));
    ranges.put(segB1, range(25000));
    ranges.put(segB2, range(26000));
    policy.setSegmentDateRangeOverrides(ranges);

    MergePolicy.MergeSpecification spec =
        policy.findForcedMerges(infos, 1, segmentsToMerge, new StubMergeContext());

    assertNotNull(spec);
    assertEquals("one merge per window", 2, spec.merges.size());

    Set<Set<SegmentCommitInfo>> actual = new HashSet<>();
    for (MergePolicy.OneMerge merge : spec.merges) {
      actual.add(new HashSet<>(merge.segments));
    }

    Set<Set<SegmentCommitInfo>> expected = new HashSet<>();
    expected.add(new HashSet<>(Arrays.asList(segA1, segA2)));
    expected.add(new HashSet<>(Arrays.asList(segB1, segB2)));

    assertEquals(expected, actual);
  }

  public void testForcedDeletesKeepWindowsDisjoint() throws Exception {
    TemporalMergePolicy policy =
        buildPolicy()
            .setForceMergeDeletesPctAllowed(5.0d)
            .setUseExponentialBuckets(false)
            .setBaseTimeSeconds(10);

    SegmentInfos infos = new SegmentInfos(Version.LATEST.major);
    SegmentCommitInfo segA1 = newSegment("delA1", 100);
    SegmentCommitInfo segA2 = newSegment("delA2", 100);
    SegmentCommitInfo segB1 = newSegment("delB1", 100);

    infos.add(segA1);
    infos.add(segA2);
    infos.add(segB1);

    Map<SegmentCommitInfo, Integer> deletes = new HashMap<>();
    deletes.put(segA1, 20);
    deletes.put(segA2, 15);
    deletes.put(segB1, 12);

    Map<SegmentCommitInfo, TemporalMergePolicy.SegmentDateRange> ranges = new HashMap<>();
    ranges.put(segA1, range(5000));
    ranges.put(segA2, range(6000));
    ranges.put(segB1, range(25000));
    policy.setSegmentDateRangeOverrides(ranges);

    MergePolicy.MergeSpecification spec =
        policy.findForcedDeletesMerges(infos, new StubMergeContext(deletes));

    assertNotNull(spec);
    assertEquals(1, spec.merges.size());
    List<SegmentCommitInfo> mergedSegments = spec.merges.getFirst().segments;
    assertEquals(2, mergedSegments.size());
    assertTrue(mergedSegments.contains(segA1));
    assertTrue(mergedSegments.contains(segA2));
  }

  private TemporalMergePolicy buildPolicy() {
    return new TemporalMergePolicy().setTemporalField("ts");
  }

  private SegmentCommitInfo newSegment(String name, int maxDoc) {
    String segmentName =
        name.startsWith("_") ? name.toLowerCase(Locale.ROOT) : "_" + name.toLowerCase(Locale.ROOT);
    SegmentInfo info =
        new SegmentInfo(
            new ByteBuffersDirectory(),
            Version.LATEST,
            Version.LATEST,
            segmentName,
            maxDoc,
            false,
            true,
            Codec.getDefault(),
            Collections.emptyMap(),
            StringHelper.randomId(),
            Collections.emptyMap(),
            null);
    info.setFiles(Collections.singleton(IndexFileNames.segmentFileName(segmentName, "", "si")));
    return new SegmentCommitInfo(info, 0, 0, -1, -1, -1, StringHelper.randomId());
  }

  public void testMergesWorkWithUncommittedSegments() throws IOException {
    Directory dir = new ByteBuffersDirectory();

    TemporalMergePolicy policy =
        new TemporalMergePolicy()
            .setTemporalField("timestamp_tl")
            .setBaseTimeSeconds(3600)
            .setMinThreshold(3)
            .setMaxThreshold(4)
            .setCompactionRatio(1.0);

    IndexWriterConfig config = new IndexWriterConfig();
    config.setMergePolicy(policy);
    config.setMaxBufferedDocs(10);

    IndexWriter writer = new IndexWriter(dir, config);

    long nowMillis = System.currentTimeMillis();
    long nowMicros = nowMillis * 1000;

    for (int segment = 0; segment < 4; segment++) {
      for (int doc = 0; doc < 10; doc++) {
        Document d = new Document();
        long timestamp = nowMicros + (doc * 1000000L);
        d.add(new LongPoint("timestamp_tl", timestamp));
        d.add(new NumericDocValuesField("timestamp_tl", timestamp));
        writer.addDocument(d);
      }
      writer.flush(); // Flush to create segment but DON'T commit
    }

    // Force a merge check - this will use TemporalMergePolicy.findMerges()
    // which calls extractSegmentDateRanges() to read uncommitted segments
    writer.maybeMerge();

    writer.forceMerge(1, true);

    writer.commit();

    DirectoryReader reader = DirectoryReader.open(dir);
    int finalSegmentCount = reader.leaves().size();

    reader.close();
    writer.close();
    dir.close();

    assertTrue(
        "Temporal merging should work on uncommitted segments (Codec approach)",
        finalSegmentCount < 4);
  }

  private TemporalMergePolicy.SegmentDateRange range(long maxMillis) {
    return new TemporalMergePolicy.SegmentDateRange(0, maxMillis);
  }

  private record StubMergeContext(
      Map<SegmentCommitInfo, Integer> deleteCounts, Set<SegmentCommitInfo> mergingSegments)
      implements MergePolicy.MergeContext {
    StubMergeContext() {
      this(Collections.emptyMap(), Collections.emptySet());
    }

    StubMergeContext(Map<SegmentCommitInfo, Integer> deleteCounts) {
      this(deleteCounts, Collections.emptySet());
    }

    @Override
    public int numDeletesToMerge(SegmentCommitInfo info) {
      return deleteCounts.getOrDefault(info, 0);
    }

    @Override
    public int numDeletedDocs(SegmentCommitInfo info) {
      return numDeletesToMerge(info);
    }

    @Override
    public InfoStream getInfoStream() {
      return InfoStream.NO_OUTPUT;
    }

    @Override
    public Set<SegmentCommitInfo> getMergingSegments() {
      return mergingSegments;
    }
  }
}
