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
package org.apache.lucene.misc.index;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.SimpleMergedSegmentWarmer;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.util.InfoStream;

/** Factory methods for {@link IndexWriterConfig}s with sensible defaults for various use-cases. */
public class IndexWriterConfigs {

  // Prevent instantiation
  private IndexWriterConfigs() {}

  /**
   * Create a new {@link IndexWriterConfig} for time-based data. It internally configures a {@link
   * LogByteSizeMergePolicy}, whose policy of only merging adjacent segments helps keep the time
   * range overlap between segments very low. This in-turn makes filtering on timestamp ranges
   * faster.
   */
  public static IndexWriterConfig forTimeBasedData() {
    IndexWriterConfig config = new IndexWriterConfig();
    config.setMergePolicy(new LogByteSizeMergePolicy());
    return config;
  }

  /**
   * Create a new {@link IndexWriterConfig} that optimized for fast near-realtime search. Internally
   * it:
   *
   * <ul>
   *   <li>configures a {@link SimpleMergedSegmentWarmer},
   *   <li>configures 8 {@link TieredMergePolicy#setSegmentsPerTier(double) segments per tier},
   *   <li>configures a {@link TieredMergePolicy#setFloorSegmentMB(double) floor segment size} of
   *       20MB,
   *   <li>enables {@link BPIndexReorderer BP reordering} with default parameters for segments above
   *       100k docs and half the max segment size.
   * </ul>
   */
  public static IndexWriterConfig forFastNRTSearch() {
    IndexWriterConfig config = new IndexWriterConfig();
    config.setMergedSegmentWarmer(new SimpleMergedSegmentWarmer(InfoStream.NO_OUTPUT));
    TieredMergePolicy tmp = new TieredMergePolicy();
    tmp.setSegmentsPerTier(8);
    tmp.setMaxMergeAtOnce(8);
    tmp.setFloorSegmentMB(20); // 10x the default of 2MB
    BPIndexReorderer reorderer = new BPIndexReorderer();
    BPReorderingMergePolicy bpmp = new BPReorderingMergePolicy(tmp, reorderer);
    bpmp.setMinNaturalMergeNumDocs(100_000);
    bpmp.setMinNaturalMergeRatioFromBiggestSegment(0.5f);
    config.setMergePolicy(bpmp);
    return config;
  }

  /**
   * Create a new {@link IndexWriterConfig} that optimized for fast search on read-only indexes. It
   * expects the index to be created and then {@link IndexWriter#forceMerge(int) force-merged}
   * before serving searches. Internally it:
   *
   * <ul>
   *   <li>configures 32 {@link TieredMergePolicy#setSegmentsPerTier(double) segments per tier},
   *   <li>disables {@link IndexWriterConfig#setMaxFullFlushMergeWaitMillis(long) merge on refresh},
   *   <li>enables {@link BPIndexReorderer BP reordering} on forced merges with default parameters.
   * </ul>
   */
  public static IndexWriterConfig forFastReadOnlySearch() {
    IndexWriterConfig config = new IndexWriterConfig();
    config.setMaxFullFlushMergeWaitMillis(0L);
    TieredMergePolicy tmp = new TieredMergePolicy();
    tmp.setSegmentsPerTier(32);
    tmp.setMaxMergeAtOnce(32);
    BPIndexReorderer reorderer = new BPIndexReorderer();
    BPReorderingMergePolicy bpmp = new BPReorderingMergePolicy(tmp, reorderer);
    bpmp.setMinNaturalMergeNumDocs(Integer.MAX_VALUE);
    config.setMergePolicy(bpmp);
    return config;
  }
}
