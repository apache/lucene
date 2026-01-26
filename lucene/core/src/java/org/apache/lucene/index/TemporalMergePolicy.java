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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

/**
 * A merge policy that groups segments by time windows and merges segments within the same window,
 * This policy is designed for time-series data where documents contain a timestamp field indexed as
 * a {@link org.apache.lucene.document.LongPoint}.
 *
 * <p>This policy organizes segments into time buckets based on the maximum timestamp in each
 * segment. Recent data goes into small time windows (e.g., 1 hour), while older data is grouped
 * into exponentially larger windows (e.g., 4 hours, 16 hours, etc.). Segments within the same time
 * window are merged together when they meet the configured thresholds, but segments from different
 * time windows are never merged together, preserving temporal locality.
 *
 * <p><b>When to use this policy:</b>
 *
 * <ul>
 *   <li>Time-series data where queries typically filter by time ranges
 *   <li>Data with a timestamp field that can be used for bucketing
 *   <li>Workloads where older data is queried less frequently than recent data
 *   <li>Use cases where you want to avoid mixing old and new data in the same segment
 * </ul>
 *
 * <p><b>Configuration:</b>
 *
 * <pre class="prettyprint">
 * TemporalMergePolicy policy = new TemporalMergePolicy()
 *     .setTemporalField("timestamp")           // Required: name of the timestamp field
 *     .setBaseTimeSeconds(3600)                // Base window size: 1 hour
 *     .setMinThreshold(4)                      // Merge when 4+ segments in a window
 *     .setMaxThreshold(8)                      // Merge at most 8 segments at once
 *     .setCompactionRatio(1.2)                 // Size ratio threshold for merging
 *     .setUseExponentialBuckets(true);         // Use exponentially growing windows
 *
 * IndexWriterConfig config = new IndexWriterConfig(analyzer);
 * config.setMergePolicy(policy);
 * </pre>
 *
 * <p><b>Time bucketing:</b> When {@link #setUseExponentialBuckets} is true (default), window sizes
 * grow exponentially: {@code baseTime}, {@code baseTime * minThreshold}, {@code baseTime *
 * minThreshold^2}, etc. This ensures that recent data is in small, frequently-merged windows while
 * older data is in larger, less-frequently-merged windows. When false, all windows have the same
 * size ({@code baseTime}).
 *
 * <p><b>Compaction ratio:</b> The {@link #setCompactionRatio} parameter controls when merges are
 * triggered. A merge is considered when the total document count across candidate segments exceeds
 * {@code largestSegment * compactionRatio}. Lower values (e.g., 1.2) trigger merges more
 * aggressively, while higher values (e.g., 2.0) allow more segments to accumulate before merging.
 * Set to 1.0 for most aggressive merging.
 *
 * <p><b>NOTE:</b> This policy requires a timestamp field indexed as a {@link
 * org.apache.lucene.document.LongPoint}. The timestamp can be in seconds, milliseconds, or
 * microseconds (auto-detected based on value magnitude).
 *
 * <p><b>NOTE:</b> Segments from different time windows are never merged together, even during
 * {@link IndexWriter#forceMerge(int)}. If you call {@code forceMerge(1)} but have segments in
 * multiple time windows, you will end up with one segment per time window.
 *
 * <p><b>NOTE:</b> Very old segments (older than {@link #setMaxAgeSeconds}) are not merged to avoid
 * unnecessary I/O on cold data.
 *
 * @lucene.experimental
 */
public class TemporalMergePolicy extends MergePolicy {

  private static final Logger log = Logger.getLogger(TemporalMergePolicy.class.getName());

  // Configuration parameters
  private String temporalField = "";
  private long baseTimeSeconds = 3600; // 1 hour default
  private int minThreshold = 4; // Minimum segments to trigger merge within a window
  private boolean useExponentialBuckets = true; // Use exponential window sizing
  private long maxWindowSizeSeconds = TimeUnit.DAYS.toSeconds(365); // Maximum window size (1 year)
  private long maxAgeSeconds =
      Long.MAX_VALUE; // Don't compact segments older than this (default: no limit)
  private int maxThreshold = 8; // Maximum segments to merge from a window at once
  private double compactionRatio =
      1.2d; // Ratio between the total docs and biggest segment to trigger merge
  private double forceMergeDeletesPctAllowed = 10.0d;
  private Map<SegmentCommitInfo, SegmentDateRange> segmentDateRangeOverrides;

  /** Sole constructor, setting all settings to their defaults. */
  public TemporalMergePolicy() {}

  /**
   * Sets the name of the timestamp field used for temporal bucketing. This field must be indexed as
   * a {@link LongPoint} and contain timestamp values in seconds, milliseconds, or microseconds
   * (auto-detected based on value magnitude).
   *
   * <p><b>This parameter is required</b> and must be set before the policy can schedule any merges.
   * The merge policy will extract the minimum and maximum timestamps from each segment to determine
   * which time window the segment belongs to.
   *
   * <p>Default is empty (no temporal field configured, policy is inactive).
   */
  public TemporalMergePolicy setTemporalField(String temporalField) {
    if (temporalField == null || temporalField.isBlank()) {
      throw new IllegalArgumentException("temporalField cannot be blank");
    }
    this.temporalField = temporalField;
    return this;
  }

  /**
   * Returns the current temporal field name.
   *
   * @see #setTemporalField
   */
  public String getTemporalField() {
    return temporalField;
  }

  /**
   * Sets the base time window size in seconds. This determines the size of the smallest (most
   * recent) time buckets.
   *
   * <p>When {@link #setUseExponentialBuckets} is enabled (default), window sizes grow
   * exponentially: {@code baseTime}, {@code baseTime * minThreshold}, {@code baseTime *
   * minThreshold^2}, etc. When disabled, all windows have the same size equal to {@code baseTime}.
   *
   * <p>Smaller values create finer-grained time windows, which can improve query performance for
   * time-range queries but may result in more segments. Larger values reduce the number of time
   * windows but may mix data from a wider time range in the same segment.
   *
   * <p>Default is 3600 seconds (1 hour).
   */
  public TemporalMergePolicy setBaseTimeSeconds(long baseTimeSeconds) {
    if (baseTimeSeconds <= 0) {
      throw new IllegalArgumentException("baseTimeSeconds must be positive");
    }
    this.baseTimeSeconds = baseTimeSeconds;
    return this;
  }

  /**
   * Returns the current base time window size in seconds.
   *
   * @see #setBaseTimeSeconds
   */
  public long getBaseTimeSeconds() {
    return baseTimeSeconds;
  }

  /**
   * Sets the minimum number of segments required in a time window to trigger a merge. Higher values
   * reduce merge frequency and I/O but allow more segments to accumulate. Lower values keep segment
   * counts lower but increase write amplification.
   *
   * <p>This threshold is also used as the growth factor for exponential bucketing when {@link
   * #setUseExponentialBuckets} is enabled. For example, with {@code minThreshold=4}, window sizes
   * will be: {@code baseTime}, {@code baseTime * 4}, {@code baseTime * 16}, etc.
   *
   * <p>Must be at least 2 and cannot exceed {@link #setMaxThreshold}. Default is 4.
   */
  public TemporalMergePolicy setMinThreshold(int minThreshold) {
    if (minThreshold < 2) {
      throw new IllegalArgumentException("minThreshold must be at least 2");
    }
    if (minThreshold > maxThreshold) {
      throw new IllegalArgumentException(
          "minThreshold cannot exceed maxThreshold (" + maxThreshold + ")");
    }
    this.minThreshold = minThreshold;
    return this;
  }

  /**
   * Returns the current minimum threshold for merging.
   *
   * @see #setMinThreshold
   */
  public int getMinThreshold() {
    return minThreshold;
  }

  /**
   * Sets whether to use exponentially growing time windows. When enabled (default), older data is
   * grouped into progressively larger time buckets: {@code baseTime}, {@code baseTime *
   * minThreshold}, {@code baseTime * minThreshold^2}, etc.
   *
   * <p>When disabled, all time windows have a fixed size equal to {@code baseTime}, which can be
   * useful for workloads with uniform query patterns across all time ranges.
   *
   * <p>Exponential bucketing is recommended for typical time-series use cases where recent data is
   * accessed more frequently than older data.
   *
   * <p>Default is true.
   */
  public TemporalMergePolicy setUseExponentialBuckets(boolean useExponentialBuckets) {
    this.useExponentialBuckets = useExponentialBuckets;
    return this;
  }

  /**
   * Returns whether exponential bucketing is enabled.
   *
   * @see #setUseExponentialBuckets
   */
  public boolean getUseExponentialBuckets() {
    return useExponentialBuckets;
  }

  /**
   * Sets the maximum number of segments to merge at once within a time window. Larger values allow
   * more aggressive merging (reducing segment count faster) but increase the cost of individual
   * merge operations.
   *
   * <p>Must be at least equal to {@link #setMinThreshold}. When a time window accumulates more
   * segments than this threshold, the policy will schedule multiple smaller merges rather than one
   * large merge.
   *
   * <p>Default is 8.
   */
  public TemporalMergePolicy setMaxThreshold(int maxThreshold) {
    if (maxThreshold < minThreshold) {
      throw new IllegalArgumentException(
          "maxThreshold must be >= minThreshold (" + minThreshold + ")");
    }
    this.maxThreshold = maxThreshold;
    return this;
  }

  /**
   * Returns the current maximum threshold for merging.
   *
   * @see #setMaxThreshold
   */
  public int getMaxThreshold() {
    return maxThreshold;
  }

  /**
   * Sets the compaction ratio that controls when merges are triggered based on segment size
   * distribution. A merge is considered when the total document count of candidate segments exceeds
   * {@code largestSegment * compactionRatio}.
   *
   * <p>Lower values (e.g., 1.2) trigger merges more aggressively, even when segment sizes are
   * relatively balanced. Higher values (e.g., 2.0 or higher) wait for more size imbalance before
   * merging, allowing more segments to accumulate but reducing write amplification.
   *
   * <p>Setting this to exactly 1.0 enables the most aggressive merging mode, where merges occur
   * whenever the minimum threshold is met, regardless of segment size distribution.
   *
   * <p>This parameter works together with {@link #setMinThreshold}: a time window must have both
   * (1) at least {@code minThreshold} segments, and (2) satisfy the compaction ratio, before a
   * merge is triggered.
   *
   * <p>Default is 1.2.
   */
  public TemporalMergePolicy setCompactionRatio(double compactionRatio) {
    if (compactionRatio < 1.0d) {
      throw new IllegalArgumentException("compactionRatio must be >= 1.0");
    }
    this.compactionRatio = compactionRatio;
    return this;
  }

  /**
   * Returns the current compaction ratio.
   *
   * @see #setCompactionRatio
   */
  public double getCompactionRatio() {
    return compactionRatio;
  }

  /**
   * Sets the maximum size for exponentially growing time windows. When {@link
   * #setUseExponentialBuckets} is enabled, window sizes grow exponentially but are capped at this
   * value.
   *
   * <p>This prevents extremely large time windows for very old data, which could mix data from
   * vastly different time periods. Once window size reaches this limit, all older data uses
   * fixed-size windows of this duration.
   *
   * <p>Default is 31536000 seconds (365 days).
   */
  public TemporalMergePolicy setMaxWindowSizeSeconds(long maxWindowSizeSeconds) {
    if (maxWindowSizeSeconds <= 0) {
      throw new IllegalArgumentException("maxWindowSizeSeconds must be positive");
    }
    this.maxWindowSizeSeconds = maxWindowSizeSeconds;
    return this;
  }

  /**
   * Returns the current maximum window size in seconds.
   *
   * @see #setMaxWindowSizeSeconds
   */
  public long getMaxWindowSizeSeconds() {
    return maxWindowSizeSeconds;
  }

  /**
   * Sets the maximum age threshold for merging segments. Segments containing data older than this
   * threshold (based on current time minus the segment's maximum timestamp) will not be merged.
   *
   * <p>This is useful for preventing unnecessary I/O on cold, historical data that is rarely
   * queried. These old segments are placed in a special "old data" bucket and skipped during merge
   * selection.
   *
   * <p>Default is {@link Long#MAX_VALUE} (no age limit, all segments are merge candidates).
   */
  public TemporalMergePolicy setMaxAgeSeconds(long maxAgeSeconds) {
    if (maxAgeSeconds <= 0) {
      throw new IllegalArgumentException("maxAgeSeconds must be positive");
    }
    this.maxAgeSeconds = maxAgeSeconds;
    return this;
  }

  /**
   * Returns the current maximum age threshold in seconds.
   *
   * @see #setMaxAgeSeconds
   */
  public long getMaxAgeSeconds() {
    return maxAgeSeconds;
  }

  /**
   * When {@link IndexWriter#forceMergeDeletes()} is called, only merge segments whose delete
   * percentage exceeds this threshold. Lower values merge more aggressively to reclaim space from
   * deleted documents, but increase I/O and write amplification.
   *
   * <p>The delete percentage is calculated as: {@code (deleted docs / total docs) * 100}.
   *
   * <p>Default is 10.0 (merge segments with more than 10% deleted documents).
   */
  public TemporalMergePolicy setForceMergeDeletesPctAllowed(double pct) {
    if (pct < 0d) {
      throw new IllegalArgumentException("forceMergeDeletesPctAllowed must be >= 0");
    }
    this.forceMergeDeletesPctAllowed = pct;
    return this;
  }

  /**
   * Returns the current force merge deletes percentage threshold.
   *
   * @see #setForceMergeDeletesPctAllowed
   */
  public double getForceMergeDeletesPctAllowed() {
    return forceMergeDeletesPctAllowed;
  }

  // Visible for testing.
  void setSegmentDateRangeOverrides(Map<SegmentCommitInfo, SegmentDateRange> overrides) {
    this.segmentDateRangeOverrides = overrides;
  }

  @Override
  public MergeSpecification findMerges(
      MergeTrigger trigger, SegmentInfos segments, MergeContext context) throws IOException {
    if (temporalField == null || temporalField.isBlank() || segments.size() == 0) {
      // No date field configured, nothing to do
      return null;
    }

    // Identify which segments are already being processed by the MergeScheduler, to avoid already
    // merging errors
    Set<SegmentCommitInfo> alreadyMerging =
        context != null ? context.getMergingSegments() : Collections.emptySet();

    // Resolve date ranges for all segments (ideally this will be cached in the future)
    Map<SegmentCommitInfo, SegmentDateRange> allRanges = resolveSegmentDateRanges(segments);

    if (allRanges.isEmpty()) {
      log.fine("No date ranges found in segments; no temporal merges to schedule");
      return null;
    }

    // Group only segments that are NOT already merging into time windows
    Map<Long, List<SegmentCommitInfo>> windowBuckets = new TreeMap<>();
    long now = System.currentTimeMillis();

    for (SegmentCommitInfo info : segments) {
      // Skip segments that are already being merged
      if (alreadyMerging.contains(info)) {
        continue;
      }

      SegmentDateRange range = allRanges.get(info);
      if (range != null) {
        assignToBucket(windowBuckets, now, info, range);
      }
    }

    if (windowBuckets.isEmpty()) {
      return null;
    }

    log.fine(
        "Grouped "
            + windowBuckets.values().stream().mapToInt(List::size).sum()
            + " available segments into "
            + windowBuckets.size()
            + " time windows (baseTime="
            + baseTimeSeconds
            + "s, minThreshold="
            + minThreshold
            + ")");

    // Find merge candidates within time windows (only considering available segments)
    MergeSpecification spec = findMergeCandidates(windowBuckets, allRanges);

    if (spec != null) {
      log.info("Date-tiered merge: found " + spec.merges.size() + " merge(s)");
      return spec;
    }

    // No date-tiered merges found
    return null;
  }

  @Override
  public MergeSpecification findForcedMerges(
      SegmentInfos segmentInfos,
      int maxSegmentCount,
      Map<SegmentCommitInfo, Boolean> segmentsToMerge,
      MergeContext mergeContext)
      throws IOException {
    if (maxSegmentCount < 1) {
      throw new IllegalArgumentException("maxSegmentCount must be >= 1");
    }

    Map<SegmentCommitInfo, SegmentDateRange> segmentDateRanges =
        resolveSegmentDateRanges(segmentInfos);
    if (segmentDateRanges.isEmpty()) {
      log.fine("No date ranges available for forced merge");
      return null;
    }

    boolean mergeAll = segmentsToMerge == null || segmentsToMerge.isEmpty();
    Set<SegmentCommitInfo> merging =
        mergeContext != null ? mergeContext.getMergingSegments() : Collections.emptySet();

    Map<SegmentCommitInfo, SegmentDateRange> eligibleRanges = new HashMap<>();
    boolean forceMergeRunning = false;
    for (SegmentCommitInfo info : segmentInfos) {
      boolean include = mergeAll || segmentsToMerge.containsKey(info);
      if (include) {
        if (merging.contains(info)) {
          forceMergeRunning = true;
          break;
        }

        SegmentDateRange range = segmentDateRanges.get(info);
        if (range != null) {
          eligibleRanges.put(info, range);
        }
      }
    }

    if (forceMergeRunning) {
      log.fine("Force merge already running; skipping new forced merge request");
      return null;
    }

    if (eligibleRanges.isEmpty()) {
      return null;
    }

    Map<Long, List<SegmentCommitInfo>> windowBuckets = groupSegmentsByTimeWindow(eligibleRanges);
    int windowCount = windowBuckets.size();

    if (windowCount == 0) {
      return null;
    }

    // Calculate target segments per window based on maxSegmentCount
    // IMPORTANT: We NEVER merge across time buckets, even in forceMerge
    // If maxSegmentCount < windowCount, we'll end up with more segments than requested
    int segmentsPerWindow = 1; // Default: merge each window to 1 segment
    if (maxSegmentCount < windowCount) {
      log.info(
          "Forced merge requested "
              + maxSegmentCount
              + " segments but "
              + windowCount
              + " time windows exist; "
              + "will merge to 1 segment per window (respecting bucket boundaries) "
              + "resulting in "
              + windowCount
              + " total segments");
    } else {
      segmentsPerWindow = Math.max(1, maxSegmentCount / windowCount);
    }

    MergeSpecification spec = null;
    int eligibleWindows;

    // First pass: count all windows (all windows are eligible for forced merge)
    eligibleWindows = windowBuckets.size();

    // Recalculate segmentsPerWindow based on eligible windows
    if (eligibleWindows > 0) {
      segmentsPerWindow = Math.max(1, maxSegmentCount / eligibleWindows);
      // Cap at maxThreshold to ensure we still merge aggressively even with few windows
      // Without this cap, a single window with 27 segments and maxSegmentCount=32
      // would result in segmentsPerWindow=32, causing no merges to be scheduled
      segmentsPerWindow = Math.min(segmentsPerWindow, maxThreshold);
    }

    log.fine(
        "Force merge: "
            + eligibleWindows
            + " eligible windows, target "
            + segmentsPerWindow
            + " segments per window (maxSegmentCount="
            + maxSegmentCount
            + ")");

    // Second pass: schedule merges for all windows
    for (Map.Entry<Long, List<SegmentCommitInfo>> entry : windowBuckets.entrySet()) {
      long windowStart = entry.getKey();
      List<SegmentCommitInfo> bucketSegments = entry.getValue();

      // Skip old data bucket (-1 sentinel) - don't merge very old data
      if (windowStart == -1) {
        log.fine(
            "Skipping force merge for old data bucket (>"
                + maxAgeSeconds
                + "s old) with "
                + bucketSegments.size()
                + " segments");
        continue;
      }

      if (bucketSegments.size() < 2) {
        continue;
      }

      MergeSpecification bucketSpec = buildForcedMerges(bucketSegments, segmentsPerWindow);
      if (bucketSpec == null) {
        continue;
      }

      if (spec == null) {
        spec = new MergeSpecification();
      }
      for (OneMerge merge : bucketSpec.merges) {
        spec.add(merge);
      }
    }

    if (spec != null) {
      log.info(
          "Forced merge: scheduling "
              + spec.merges.size()
              + " merge(s) across "
              + windowCount
              + " window(s)");
    }
    return spec;
  }

  @Override
  public MergeSpecification findForcedDeletesMerges(
      SegmentInfos segmentInfos, MergeContext mergeContext) throws IOException {
    if (mergeContext == null) {
      return null;
    }

    Map<SegmentCommitInfo, SegmentDateRange> segmentDateRanges =
        resolveSegmentDateRanges(segmentInfos);
    if (segmentDateRanges.isEmpty()) {
      log.fine("No date ranges available for forced delete merges");
      return null;
    }

    Set<SegmentCommitInfo> merging = mergeContext.getMergingSegments();
    Map<SegmentCommitInfo, SegmentDateRange> candidateRanges = new HashMap<>();
    Map<SegmentCommitInfo, Double> deleteRatios = new HashMap<>();

    for (SegmentCommitInfo info : segmentInfos) {
      if (merging.contains(info)) {
        continue;
      }

      int delCount = mergeContext.numDeletesToMerge(info);
      if (delCount <= 0) {
        continue;
      }
      int maxDoc = Math.max(1, info.info.maxDoc());
      double pctDeletes = (100.0d * delCount) / maxDoc;
      if (pctDeletes > forceMergeDeletesPctAllowed) {
        SegmentDateRange range = segmentDateRanges.get(info);
        if (range != null) {
          candidateRanges.put(info, range);
          deleteRatios.put(info, pctDeletes);
        }
      }
    }

    if (candidateRanges.isEmpty()) {
      return null;
    }

    Map<Long, List<SegmentCommitInfo>> windowBuckets = groupSegmentsByTimeWindow(candidateRanges);
    MergeSpecification spec = null;

    for (Map.Entry<Long, List<SegmentCommitInfo>> entry : windowBuckets.entrySet()) {
      List<SegmentCommitInfo> bucketSegments = entry.getValue();
      if (bucketSegments.size() < 2) {
        continue;
      }

      bucketSegments.sort(
          (a, b) ->
              Double.compare(
                  deleteRatios.getOrDefault(b, 0.0d), deleteRatios.getOrDefault(a, 0.0d)));

      MergeSpecification bucketSpec = buildSequentialMerges(bucketSegments);
      if (bucketSpec == null) {
        continue;
      }

      if (spec == null) {
        spec = new MergeSpecification();
      }

      for (OneMerge merge : bucketSpec.merges) {
        spec.add(merge);
      }
    }

    if (spec != null) {
      log.fine(
          "Forced deletes merge: scheduling "
              + spec.merges.size()
              + " merge(s) (threshold="
              + forceMergeDeletesPctAllowed
              + "%)");
    }
    return spec;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

  /**
   * Extract min/max date ranges from all segments by reading point values directly from segment
   * files.
   *
   * <p>This method uses the Codec API to read segment files directly, which allows it to access
   * both committed and uncommitted (newly flushed) segments. This is done on purpose instead of
   * using DirectoryReader.open(), which only sees committed segments and would miss segments that
   * have been flushed but not yet committed during indexing.
   *
   * <p>The method handles both regular segment files and compound file format (.cfs) segments.
   *
   * @param segments the segment infos to extract date ranges from
   * @return a map from segment info to its date range (min/max timestamps in milliseconds)
   */
  private Map<SegmentCommitInfo, SegmentDateRange> extractSegmentDateRanges(SegmentInfos segments) {
    Map<SegmentCommitInfo, SegmentDateRange> ranges = new HashMap<>();

    if (segments.size() == 0) {
      log.fine("No segments to extract date ranges from");
      return ranges;
    }

    for (SegmentCommitInfo segmentInfo : segments) {
      try {
        SegmentDateRange range = extractDateRangeFromSegment(segmentInfo);
        if (range != null) {
          ranges.put(segmentInfo, range);
        }
      } catch (java.io.FileNotFoundException | java.nio.file.NoSuchFileException e) {
        // Segment files not fully written yet (mid-flush), skip for now
        // This can happen when findMerges() is called during SEGMENT_FLUSH trigger
        log.fine(
            "Segment "
                + segmentInfo.info.name
                + " files not ready yet, skipping: "
                + e.getMessage());
      } catch (IOException e) {
        log.warning(
            "Failed to read point values from segment "
                + segmentInfo.info.name
                + ": "
                + e.getMessage());
      }
    }

    log.fine("Extracted date ranges for " + ranges.size() + "/" + segments.size() + " segments");
    return ranges;
  }

  /**
   * Extract date range (min/max timestamps) from a single segment by reading point values.
   *
   * @param segmentInfo the segment to read from
   * @return the date range, or null if the temporal field is not present or has no values
   * @throws IOException if there's an error reading the segment
   */
  private SegmentDateRange extractDateRangeFromSegment(SegmentCommitInfo segmentInfo)
      throws IOException {
    SegmentInfo si = segmentInfo.info;

    // Track compound directory separately to ensure we never close si.dir
    Directory compoundDir = null;
    try {
      Directory readerDir =
          si.getUseCompoundFile()
              ? (compoundDir = si.getCodec().compoundFormat().getCompoundReader(si.dir, si))
              : si.dir;

      FieldInfos fieldInfos =
          si.getCodec().fieldInfosFormat().read(readerDir, si, "", IOContext.READONCE);

      // Validate that the temporal field exists and is a point field
      FieldInfo fieldInfo = fieldInfos.fieldInfo(temporalField);
      if (fieldInfo == null) {
        log.fine("Segment " + si.name + ": temporal field '" + temporalField + "' not found");
        return null;
      }

      if (fieldInfo.getPointDimensionCount() == 0) {
        log.warning(
            "Segment "
                + si.name
                + ": temporal field '"
                + temporalField
                + "' is not indexed as a point field (found: "
                + fieldInfo
                + "). "
                + "Skipping this segment for date-tiered merging. This may occur with legacy segments "
                + "or after schema changes.");
        return null;
      }

      // Read point values using the codec
      PointsFormat pointsFormat = si.getCodec().pointsFormat();
      try (PointsReader pointsReader =
          pointsFormat.fieldsReader(
              new SegmentReadState(readerDir, si, fieldInfos, IOContext.READONCE))) {

        PointValues pointValues = pointsReader.getValues(temporalField);
        if (pointValues == null) {
          log.fine("Segment " + si.name + ": no point values for field '" + temporalField + "'");
          return null;
        }

        byte[] minPackedValue = pointValues.getMinPackedValue();
        byte[] maxPackedValue = pointValues.getMaxPackedValue();

        if (minPackedValue == null || maxPackedValue == null) {
          log.fine("Segment " + si.name + ": no min/max values for field '" + temporalField + "'");
          return null;
        }

        // Decode the packed values as longs, we have to make an assumption here
        // since we don't have the schema :/
        long minDate = LongPoint.decodeDimension(minPackedValue, 0);
        long maxDate = LongPoint.decodeDimension(maxPackedValue, 0);

        // Convert to milliseconds based on detected unit
        long divisor = getTemporalFieldDivisor(maxDate);
        long minDateMillis, maxDateMillis;
        if (divisor < 0) {
          long multiplier = -divisor;
          minDateMillis = minDate * multiplier;
          maxDateMillis = maxDate * multiplier;
        } else {
          minDateMillis = minDate / divisor;
          maxDateMillis = maxDate / divisor;
        }

        return new SegmentDateRange(minDateMillis, maxDateMillis);
      }
    } finally {
      // Close compound directory if we opened it (never close si.dir)
      if (compoundDir != null) {
        compoundDir.close();
      }
    }
  }

  // Auto-detect the divisor to convert the temporal field's raw value to milliseconds.
  private long getTemporalFieldDivisor(long maxValue) {
    if (maxValue > 100_000_000_000_000L) {
      // Values > 10^14 are microseconds
      return 1_000;
    } else if (maxValue > 100_000_000_000L) {
      // Values > 10^11 are milliseconds
      return 1;
    } else {
      // Values <= 10^11 are seconds
      return -1000;
    }
  }

  // Group segments into time windows based on their maximum date.
  private Map<Long, List<SegmentCommitInfo>> groupSegmentsByTimeWindow(
      Map<SegmentCommitInfo, SegmentDateRange> segmentDateRanges) {

    // Map from window identifier (bucket start timestamp) to segments
    Map<Long, List<SegmentCommitInfo>> buckets = new TreeMap<>();

    long now = System.currentTimeMillis();

    for (Map.Entry<SegmentCommitInfo, SegmentDateRange> entry : segmentDateRanges.entrySet()) {
      SegmentCommitInfo segment = entry.getKey();
      SegmentDateRange dateRange = entry.getValue();
      assignToBucket(buckets, now, segment, dateRange);
    }

    log.fine(
        "Grouped "
            + segmentDateRanges.size()
            + " segments into "
            + buckets.size()
            + " time windows (baseTime="
            + baseTimeSeconds
            + "s, minThreshold="
            + minThreshold
            + ")");

    return buckets;
  }

  private void assignToBucket(
      Map<Long, List<SegmentCommitInfo>> buckets,
      long now,
      SegmentCommitInfo segment,
      SegmentDateRange dateRange) {

    long maxDateSeconds = dateRange.maxDate / 1000;
    long bucket = getBucketForTimestamp(maxDateSeconds, now / 1000);

    buckets.computeIfAbsent(bucket, k -> new ArrayList<>()).add(segment);
    log.fine(
        "Segment "
            + segment.info.name
            + ": max_date="
            + dateRange.maxDate
            + ", assigned to bucket="
            + bucket);
  }

  private long getBucketForTimestamp(long timestampSeconds, long nowSeconds) {
    long ageSeconds = nowSeconds - timestampSeconds;
    if (ageSeconds < 0) {
      // Future timestamp, put in most recent bucket
      ageSeconds = 0;
    }

    // If data is older than maxAgeSeconds, put it all in one monster "old data" bucket
    // Use -1 as the sentinel value since bucket IDs are timestamps (always >= 0)
    if (ageSeconds > maxAgeSeconds) {
      return -1;
    }

    if (!useExponentialBuckets) {
      return (timestampSeconds / baseTimeSeconds) * baseTimeSeconds;
    }

    // Determine bucket (0 = baseTime, 1 = baseTime*minThreshold, 2 = baseTime*minThreshold^2,...
    long bucketSizeSeconds = baseTimeSeconds;

    while (ageSeconds >= bucketSizeSeconds * minThreshold
        && bucketSizeSeconds < maxWindowSizeSeconds) {
      bucketSizeSeconds *= minThreshold;
    }

    if (bucketSizeSeconds > maxWindowSizeSeconds) {
      bucketSizeSeconds = maxWindowSizeSeconds;
    }

    return (timestampSeconds / bucketSizeSeconds) * bucketSizeSeconds;
  }

  /**
   * Find merge candidates within time windows. Merges segments when a window has >= minThreshold
   * segments. Skips windows that are older than maxAgeSeconds.
   */
  MergeSpecification findMergeCandidates(
      Map<Long, List<SegmentCommitInfo>> buckets,
      Map<SegmentCommitInfo, SegmentDateRange> segmentDateRanges) {
    MergeSpecification spec = null;

    for (Map.Entry<Long, List<SegmentCommitInfo>> entry : buckets.entrySet()) {
      Long windowStart = entry.getKey();
      List<SegmentCommitInfo> segmentsInWindow = entry.getValue();

      // Skip old data bucket (-1 sentinel) - don't merge very old data
      if (windowStart == -1) {
        log.fine(
            "Skipping old data bucket (>"
                + maxAgeSeconds
                + "s old) with "
                + segmentsInWindow.size()
                + " segments");
        continue;
      }

      if (segmentsInWindow.size() < minThreshold) {
        log.fine(
            "Window "
                + windowStart
                + " has "
                + segmentsInWindow.size()
                + " segments (below threshold of "
                + minThreshold
                + ")");
        continue;
      }

      List<List<SegmentCommitInfo>> mergesForWindow =
          planWindowMerges(windowStart, segmentsInWindow, segmentDateRanges);

      if (mergesForWindow.isEmpty()) {
        log.fine(
            "Window "
                + windowStart
                + " has "
                + segmentsInWindow.size()
                + " segments but no merge met compaction ratio "
                + compactionRatio
                + " (maxThreshold="
                + maxThreshold
                + ")");
        continue;
      }

      if (spec == null) {
        spec = new MergeSpecification();
      }

      for (List<SegmentCommitInfo> mergeSegments : mergesForWindow) {
        OneMerge merge = new OneMerge(mergeSegments);
        spec.add(merge);
        log.fine(
            "Date-tiered merge: window "
                + windowStart
                + " scheduling "
                + mergeSegments.size()
                + "-segment merge (ratio "
                + compactionRatio
                + ")");
      }
    }

    return spec;
  }

  /**
   * Builds merges for a single window, enforcing the compaction ratio and thresholds. Segments are
   * sorted by their newest timestamp to ensure we merge recent data first.
   */
  private List<List<SegmentCommitInfo>> planWindowMerges(
      long windowStart,
      List<SegmentCommitInfo> segmentsInWindow,
      Map<SegmentCommitInfo, SegmentDateRange> segmentDateRanges) {

    List<SegmentCommitInfo> ordered = new ArrayList<>(segmentsInWindow);
    ordered.sort(Comparator.comparingLong(sci -> segmentDateRanges.get(sci).maxDate).reversed());

    List<List<SegmentCommitInfo>> planned = new ArrayList<>();
    int cursor = 0;

    while (ordered.size() - cursor >= minThreshold) {
      long totalDocs = 0;
      long largestDocs = 0;
      int end = cursor;
      boolean emittedMerge = false;

      while (end < ordered.size() && end - cursor < maxThreshold) {
        SegmentCommitInfo candidate = ordered.get(end);
        long docCount = candidate.info.maxDoc();
        totalDocs += docCount;
        largestDocs = Math.max(largestDocs, docCount);
        end++;

        int candidateSize = end - cursor;
        if (candidateSize < minThreshold) {
          continue;
        }

        boolean reachedMax = candidateSize == maxThreshold;
        boolean exhaustedSegments = end == ordered.size();

        // In aggressive mode (ratio <= 1.0), merge when:
        // 1. We reach maxThreshold (preferred), OR
        // 2. We've exhausted all segments and have at least minThreshold
        if (compactionRatio <= 1.0d) {
          if (reachedMax || exhaustedSegments) {
            planned.add(new ArrayList<>(ordered.subList(cursor, end)));
            cursor = end;
            emittedMerge = true;
            break;
          }
        } else {
          // In normal mode, merge when ratio is satisfied OR we reach maxThreshold
          boolean ratioSatisfied = totalDocs >= Math.ceil(largestDocs * compactionRatio);
          if (ratioSatisfied || reachedMax) {
            planned.add(new ArrayList<>(ordered.subList(cursor, end)));
            cursor = end;
            emittedMerge = true;
            break;
          }
        }
      }

      if (!emittedMerge) {
        log.fine(
            "Window "
                + windowStart
                + " waiting for more segments (available="
                + (ordered.size() - cursor)
                + ", totalDocs="
                + totalDocs
                + ", largestSegmentDocs="
                + largestDocs
                + ", ratio="
                + compactionRatio
                + ")");
        break;
      }
    }

    return planned;
  }

  private MergeSpecification buildForcedMerges(
      List<SegmentCommitInfo> candidates, int maxSegmentCount) {
    if (candidates.size() < 2) {
      return null;
    }

    List<SegmentCommitInfo> pool = new ArrayList<>(candidates);
    MergeSpecification spec = null;
    int remaining = pool.size();

    while (remaining > maxSegmentCount && pool.size() >= 2) {
      int neededReduction = remaining - maxSegmentCount;
      int mergeInputs = Math.min(maxThreshold, Math.max(2, neededReduction + 1));
      mergeInputs = Math.min(mergeInputs, pool.size());

      List<SegmentCommitInfo> batch = new ArrayList<>(pool.subList(0, mergeInputs));
      pool.subList(0, mergeInputs).clear();

      if (spec == null) {
        spec = new MergeSpecification();
      }
      spec.add(new OneMerge(batch));
      remaining -= (mergeInputs - 1);
    }

    return spec;
  }

  private MergeSpecification buildSequentialMerges(List<SegmentCommitInfo> candidates) {
    if (candidates.size() < 2) {
      return null;
    }

    MergeSpecification spec = null;
    List<SegmentCommitInfo> batch = new ArrayList<>(maxThreshold);

    for (SegmentCommitInfo info : candidates) {
      batch.add(info);
      if (batch.size() == maxThreshold) {
        if (spec == null) {
          spec = new MergeSpecification();
        }
        spec.add(new OneMerge(new ArrayList<>(batch)));
        batch.clear();
      }
    }

    if (batch.size() > 1) {
      if (spec == null) {
        spec = new MergeSpecification();
      }
      spec.add(new OneMerge(new ArrayList<>(batch)));
    }

    return spec;
  }

  private Map<SegmentCommitInfo, SegmentDateRange> resolveSegmentDateRanges(SegmentInfos segments) {
    if (segmentDateRangeOverrides != null) {
      return segmentDateRangeOverrides;
    }
    return extractSegmentDateRanges(segments);
  }

  /** Helper class to store min/max date range for a segment. */
  record SegmentDateRange(long minDate, long maxDate) {}
}
