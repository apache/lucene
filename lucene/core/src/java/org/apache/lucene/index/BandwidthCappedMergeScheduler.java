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
 * A {@link MergeScheduler} that caps total IO write bandwidth across all running merges to a
 * specified max MB/sec bandwidth.
 *
 * @lucene.experimental
 */
// nocommit
public class BandwidthCappedMergeScheduler extends ConcurrentMergeScheduler {

  /** Global bandwidth cap in MB/s. Mutable so that updates are applied live. */
  private double bandwidthMbPerSec;

  /** Create a scheduler with a required global bandwidth cap in MB/s. */
  public BandwidthCappedMergeScheduler(double bandwidthMbPerSec) {
    if (!(bandwidthMbPerSec > 0.0)
        || Double.isNaN(bandwidthMbPerSec)
        || Double.isInfinite(bandwidthMbPerSec)) {
      throw new IllegalArgumentException("bandwidthMbPerSec must be a finite positive value");
    }
    this.bandwidthMbPerSec = bandwidthMbPerSec;
  }

  /**
   * Auto IO throttling is managed by this scheduler's bandwidth cap. Enabling parent CMS IO
   * throttling is ignored.
   */
  @Override
  public synchronized void enableAutoIOThrottle() {
    if (verbose()) {
      message("Ignoring enableAutoIOThrottle; using bandwidth cap instead");
    }
    // Intentionally no-op
  }

  /** Ensure auto IO throttling remains disabled. */
  @Override
  public synchronized void disableAutoIOThrottle() {
    // Make sure parent state is disabled if it was somehow enabled earlier
    super.disableAutoIOThrottle();
  }

  /** Always returns false since CMS auto IO throttling is disabled for this scheduler. */
  @Override
  public synchronized boolean getAutoIOThrottle() {
    return false;
  }

  /** Get the global bandwidth cap in MB/s */
  public double getMaxMbPerSec() {
    return bandwidthMbPerSec;
  }

  /**
   * Set the global bandwidth cap in MB/s.
   *
   * <p>This setting is live: merges that are already running will be adjusted to the new per-merge
   * rate derived from this cap, without requiring a restart.
   *
   * @param bandwidthMbPerSec the new global bandwidth cap in MB/s; must be finite and > 0
   */
  public synchronized void setMaxMbPerSec(double bandwidthMbPerSec) {
    if (!(bandwidthMbPerSec > 0.0)
        || Double.isNaN(bandwidthMbPerSec)
        || Double.isInfinite(bandwidthMbPerSec)) {
      throw new IllegalArgumentException("bandwidthMbPerSec must be a finite positive value");
    }
    this.bandwidthMbPerSec = bandwidthMbPerSec;
    updateMergeThreads();
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
    double perMergeRate;
    if (activeMerges > 0) {
      perMergeRate = Math.max(Double.MIN_VALUE, bandwidthMbPerSec / activeMerges);
    } else {
      perMergeRate = Double.POSITIVE_INFINITY;
    }

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
        + ", bandwidthMbPerSec="
        + bandwidthMbPerSec
        + " MB/s";
  }

  /** Merge thread that logs the rate limiter value after merge completes. */
  protected class BandwidthTrackingMergeThread extends MergeThread {
    private final double mergeBandwidthMB;

    /** Creates a new BandwidthTrackingMergeThread for the given merge. */
    public BandwidthTrackingMergeThread(MergeSource mergeSource, OneMerge merge) {
      super(mergeSource, merge);
      this.mergeBandwidthMB = merge.estimatedMergeBytes / (1024.0 * 1024.0);
    }

    @Override
    public void run() {
      long startTimeNS = System.nanoTime();
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
        long durationNS = System.nanoTime() - startTimeNS;
        if (verbose()) {
          double durationMS = durationNS / 1_000_000.0;
          double mbPerSec = mergeBandwidthMB / Math.max(durationMS / 1000.0, 0.001);
          message(
              "Merge completed: "
                  + getSegmentName(merge)
                  + " "
                  + mergeBandwidthMB
                  + " MB in "
                  + String.format(java.util.Locale.US, "%.1f", durationMS)
                  + "ms ("
                  + String.format(java.util.Locale.US, "%.2f", mbPerSec)
                  + " MB/s)");
        }
      }
    }
  }

  private static String getSegmentName(OneMerge merge) {
    return merge.info.info.name;
  }
}
