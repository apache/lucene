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
package org.apache.lucene.sandbox.index;

import java.io.IOException;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.MergePolicy.OneMerge;
import org.apache.lucene.index.MergeScheduler;

/**
 * A {@link MergeScheduler} that caps total IO write bandwidth across all running merges to a
 * specified max MB/sec bandwidth.
 *
 * @lucene.experimental
 */
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

    // Use the effective max thread count to avoid counting threads that CMS has paused
    int effectiveMaxThreads = getMaxThreadCount();
    if (effectiveMaxThreads == ConcurrentMergeScheduler.AUTO_DETECT_MERGES_AND_THREADS) {
      int coreCount = Runtime.getRuntime().availableProcessors();
      effectiveMaxThreads = Math.max(1, coreCount / 2);
    }
    int divisor = Math.min(effectiveMaxThreads, activeMerges);

    double perMergeRate;
    if (divisor > 0) {
      perMergeRate = Math.max(0.1, bandwidthMbPerSec / divisor); // Use 0.1 MB/s minimum
    } else {
      perMergeRate = Double.POSITIVE_INFINITY;
    }

    // Apply the calculated rate limit to each active merge thread without unpausing paused threads
    for (MergeThread mergeThread : mergeThreads) {
      if (mergeThread.isAlive()) {
        double currentRate = mergeThread.getRateLimiter().getMBPerSec();
        if (currentRate > 0.0) { // Only update if not paused by parent CMS (above soft limit)
          mergeThread.getRateLimiter().setMBPerSec(perMergeRate);
        }
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

    /** Creates a new BandwidthTrackingMergeThread for the given merge. */
    public BandwidthTrackingMergeThread(MergeSource mergeSource, OneMerge merge) {
      super(mergeSource, merge);
    }

    @Override
    public void run() {
      long startTimeNS = System.nanoTime();
      OneMerge merge = getMerge();
      try {
        if (verbose()) {
          message(
              "Starting bandwidth-capped merge: (estimatedMergeMB="
                  + merge.estimatedMergeBytes / (1024.0 * 1024.0)
                  + " MB)");
        }
        super.run(); // IO throttling is handled by the RateLimiter
      } finally {
        long durationNS = System.nanoTime() - startTimeNS;
        if (verbose()) {
          double durationMS = durationNS / 1_000_000.0;
          double mbPerSec = merge.estimatedMergeBytes / (1024.0 * 1024.0) / (durationMS / 1000.0);
          message(
              "Merge completed: "
                  + merge.estimatedMergeBytes / (1024.0 * 1024.0)
                  + " MB in "
                  + String.format(java.util.Locale.US, "%.1f", durationMS)
                  + "ms ("
                  + String.format(java.util.Locale.US, "%.2f", mbPerSec)
                  + " MB/s)");
        }
      }
    }
  }
}
