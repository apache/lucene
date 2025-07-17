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
import org.apache.lucene.index.MergePolicy.OneMerge;

/**
 * A {@link MergeScheduler} that extends {@link ConcurrentMergeScheduler} with bandwidth tracking
 * and limiting capabilities. This scheduler maintains a bandwidth rate bucket that is divided among
 * active merges. When bandwidth is limited, merges are throttled to preserve system resources.
 *
 * <p>Key features: Global bandwidth rate bucket with configurable capacity, dynamic per-merge
 * throttling.
 *
 * @lucene.experimental
 */
// nocommit
public class BandwidthCappedMergeScheduler extends ConcurrentMergeScheduler {

  /** Floor for IO write rate limit (we will never go any lower than this) */
  private static final double MIN_MERGE_MB_PER_SEC = 5.0;

  /** Ceiling for IO write rate limit (we will never go any higher than this) */
  private static final double MAX_MERGE_MB_PER_SEC = 10240.0;

  /** Initial value for IO write rate limit */
  private static final double START_MB_PER_SEC = 1000.0;

  /** Global bandwidth rate bucket in MB/s */
  private double bandwidthRateBucket = START_MB_PER_SEC;

  /** Default constructor with 1000 MB/s bandwidth rate bucket */
  public BandwidthCappedMergeScheduler() {
    super();
  }

  /** Set the global bandwidth rate bucket in MB/s (default 1000 MB/s) */
  public void setBandwidthRateBucket(double mbPerSec) {
    if (mbPerSec < MIN_MERGE_MB_PER_SEC || mbPerSec > MAX_MERGE_MB_PER_SEC) {
      throw new IllegalArgumentException(
          "Bandwidth rate must be between "
              + MIN_MERGE_MB_PER_SEC
              + " and "
              + MAX_MERGE_MB_PER_SEC
              + " MB/s");
    }
    this.bandwidthRateBucket = mbPerSec;
    updateMergeThreads();
  }

  /** Get the global bandwidth rate bucket in MB/s */
  public double getBandwidthRateBucket() {
    return bandwidthRateBucket;
  }

  /** Distributes the global bandwidth rate bucket evenly among all active merge threads. */
  @Override
  protected synchronized void updateMergeThreads() {
    super.updateMergeThreads();
    int activeMerges = 0;
    for (MergeThread mergeThread : mergeThreads) {
      if (mergeThread.isAlive()) {
        activeMerges++;
      }
    }
    double perMergeRate =
        activeMerges > 0
            ? Math.max(
                MIN_MERGE_MB_PER_SEC,
                Math.min(MAX_MERGE_MB_PER_SEC, bandwidthRateBucket / activeMerges))
            : Double.POSITIVE_INFINITY;

    // Apply the calculated rate limit to each active merge thread
    for (MergeThread mergeThread : mergeThreads) {
      if (mergeThread.isAlive()) {
        mergeThread.rateLimiter.setMBPerSec(perMergeRate);
      }
    }
  }

  /** Creates a custom merge thread with bandwidth tracking capabilities. */
  @Override
  protected synchronized MergeThread getMergeThread(MergeSource mergeSource, OneMerge merge)
      throws IOException {
    return new BandwidthTrackingMergeThread(mergeSource, merge);
  }

  /** Returns a string representation including the current bandwidth rate bucket setting. */
  @Override
  public String toString() {
    return getClass().getSimpleName()
        + ": "
        + super.toString()
        + ", bandwidthRateBucket="
        + bandwidthRateBucket
        + " MB/s";
  }

  /** Merge thread that logs the rate limiter value after merge completes. */
  protected class BandwidthTrackingMergeThread extends MergeThread {
    private final double mergeBandwidthMB;

    /**
     * Creates a new BandwidthTrackingMergeThread for the given merge.
     */
    public BandwidthTrackingMergeThread(MergeSource mergeSource, OneMerge merge) {
      super(mergeSource, merge);
      this.mergeBandwidthMB = merge.estimatedMergeBytes / (1024.0 * 1024.0);
    }

    @Override
    public void run() {
      long startTime = System.currentTimeMillis();
      try {
        if (verbose()) {
          message(
              "Starting bandwidth-capped merge: "
                  + getSegmentName(merge)
                  + " (estimated="
                  + mergeBandwidthMB
                  + " MB)");
        }
        super.run(); // IO throttling is handled by the RateLimiter
      } finally {
        long duration = System.currentTimeMillis() - startTime;
        if (verbose()) {
          double mbPerSec = mergeBandwidthMB / Math.max(duration / 1000.0, 0.001);
          message(
              "Merge completed: "
                  + getSegmentName(merge)
                  + " "
                  + mergeBandwidthMB
                  + " MB in "
                  + duration
                  + "ms ("
                  + String.format(java.util.Locale.US, "%.2f", mbPerSec)
                  + " MB/s)");
        }
      }
    }
  }

  private static String getSegmentName(OneMerge merge) {
    return merge.info != null ? merge.info.info.name : "_na_";
  }
}
