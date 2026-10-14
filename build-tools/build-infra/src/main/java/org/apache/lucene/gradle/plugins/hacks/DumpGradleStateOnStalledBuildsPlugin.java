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
package org.apache.lucene.gradle.plugins.hacks;

import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import org.apache.lucene.gradle.SuppressForbidden;
import org.apache.lucene.gradle.plugins.LuceneGradlePlugin;
import org.gradle.api.Project;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.provider.Provider;
import org.gradle.api.services.BuildService;
import org.gradle.api.services.BuildServiceParameters;
import org.gradle.build.event.BuildEventsListenerRegistry;
import org.gradle.tooling.events.FinishEvent;
import org.gradle.tooling.events.OperationCompletionListener;

/**
 * This tracks the build progress (looking at task completion updates) and emits some diagnostics if
 * no such progress can be observed.
 */
public class DumpGradleStateOnStalledBuildsPlugin extends LuceneGradlePlugin {
  /** Check for stalled progress every minute. */
  static final long checkIntervalMillis = TimeUnit.MINUTES.toMillis(1);

  /**
   * After the no-progress is detected, dump everything every 5 minutes (unless progress is
   * restored).
   */
  static final long recheckIntervalMillis = TimeUnit.MINUTES.toMillis(5);

  /** Consider 3 minutes of no-updates as no-progress. */
  static final long minDurationOfNoUpdates = TimeUnit.MINUTES.toNanos(3);

  public static final String OPT_TRACK_GRADLE_STATE = "lucene.trackGradleState";

  private final BuildEventsListenerRegistry listenerRegistry;

  @Inject
  public DumpGradleStateOnStalledBuildsPlugin(BuildEventsListenerRegistry listenerRegistry) {
    this.listenerRegistry = listenerRegistry;
  }

  @Override
  public void apply(Project project) {
    applicableToRootProjectOnly(project);

    var buildOptions = getBuildOptions(project);
    var trackStateOption =
        buildOptions.addBooleanOption(
            OPT_TRACK_GRADLE_STATE,
            "Track build progress and dump gradle jvm state diagnostics in case of no progress.",
            false);

    if (!trackStateOption.get()) {
      return;
    }

    Provider<TrackProgressService> service =
        project
            .getGradle()
            .getSharedServices()
            .registerIfAbsent("trackProgressService", TrackProgressService.class, _ -> {});

    listenerRegistry.onTaskCompletion(service);
  }

  /**
   * This service collects per-task durations and prints a summary when closed (at the end of the
   * build).
   */
  public abstract static class TrackProgressService
      implements BuildService<BuildServiceParameters.None>,
          OperationCompletionListener,
          AutoCloseable {
    private static final Logger LOGGER = Logging.getLogger(TrackProgressService.class);

    /** Execution time of all successful tasks. Keys are task paths, values are in millis. */
    private volatile long lastUpdateNanos = System.nanoTime();

    private final Thread trackerThread;
    private volatile boolean stop;

    public TrackProgressService() {
      this.trackerThread =
          new Thread("gradle-progress-tracker") {
            @Override
            public void run() {
              while (!stop) {
                try {
                  sleepNow(checkIntervalMillis);

                  long noUpdateTime = System.nanoTime() - lastUpdateNanos;
                  if (noUpdateTime >= minDurationOfNoUpdates) {
                    dumpGradleState(noUpdateTime);
                    sleepNow(recheckIntervalMillis);
                  }
                } catch (InterruptedException _) {
                  // ignore.
                }
              }
            }

            @SuppressForbidden(reason = "legitimate sleep here.")
            private void sleepNow(long millis) throws InterruptedException {
              Thread.sleep(millis);
            }
          };
      trackerThread.setDaemon(true);
      trackerThread.start();
    }

    private void dumpGradleState(long noUpdateTimeNanos) {
      LOGGER.warn(
          "{}: Gradle panic: no status update in {}" + " seconds?",
          Instant.now(),
          Duration.ofNanos(noUpdateTimeNanos).toSeconds());

      String jvmDiags;
      try {
        jvmDiags = collectJvmDiagnostics();
      } catch (Throwable t) {
        jvmDiags = "Failed to collect jvm diagnostics: " + t.getMessage();
      }
      LOGGER.warn(jvmDiags);
    }

    /** Collects heap stats and a full thread dump into a single String. */
    public static String collectJvmDiagnostics() {
      StringBuilder sb = new StringBuilder(32_768);

      sb.append("====== GRADLE JVM DIAGNOSTICS ======\n");

      Runtime rt = Runtime.getRuntime();
      long max = rt.maxMemory();
      long total = rt.totalMemory();
      long free = rt.freeMemory();
      long used = total - free;

      sb.append("=== JVM Heap ===\n")
          .append("Max:   ")
          .append(formatBytes(max))
          .append(" (")
          .append(max)
          .append(" bytes)\n")
          .append("Total: ")
          .append(formatBytes(total))
          .append(" (")
          .append(total)
          .append(" bytes)\n")
          .append("Free:  ")
          .append(formatBytes(free))
          .append(" (")
          .append(free)
          .append(" bytes)\n")
          .append("Used:  ")
          .append(formatBytes(used))
          .append(" (")
          .append(used)
          .append(" bytes)\n\n");

      // --- Thread dump ---
      ThreadMXBean tmx = ManagementFactory.getThreadMXBean();
      boolean withMonitors = tmx.isObjectMonitorUsageSupported();
      boolean withSyncs = tmx.isSynchronizerUsageSupported();

      ThreadInfo[] infos = tmx.dumpAllThreads(withMonitors, withSyncs);
      sb.append("=== Threads (").append(infos.length).append(") ===\n");

      for (ThreadInfo ti : infos) {
        if (ti == null) continue;

        sb.append('"')
            .append(ti.getThreadName())
            .append('"')
            .append(" Id=")
            .append(ti.getThreadId())
            .append(" ")
            .append(ti.getThreadState());
        if (ti.isSuspended()) sb.append(" (suspended)");
        if (ti.isInNative()) sb.append(" (in native)");
        sb.append('\n');

        // Stack trace with monitor/synchronizer details
        StackTraceElement[] stack = ti.getStackTrace();
        for (int i = 0; i < stack.length; i++) {
          StackTraceElement ste = stack[i];
          sb.append("\tat ").append(ste).append('\n');

          // Lock the thread is currently waiting on
          if (i == 0 && ti.getLockInfo() != null) {
            sb.append("\t- waiting on ").append(ti.getLockInfo()).append('\n');
          }
          // Monitors locked at this frame
          if (withMonitors) {
            for (MonitorInfo mi : ti.getLockedMonitors()) {
              if (mi.getLockedStackDepth() == i) {
                sb.append("\t- locked ").append(mi).append('\n');
              }
            }
          }
        }

        // Locked synchronizers (e.g., ReentrantLocks)
        if (withSyncs) {
          LockInfo[] locks = ti.getLockedSynchronizers();
          if (locks.length > 0) {
            sb.append("\n\tLocked synchronizers:\n");
            for (LockInfo li : locks) {
              sb.append("\t- ").append(li).append('\n');
            }
          }
        }
        sb.append('\n');
      }

      // --- Deadlock info (if any) ---
      long[] deadlocked = tmx.findDeadlockedThreads();
      if (deadlocked != null && deadlocked.length > 0) {
        sb.append("=== Deadlocked Threads ===\n");
        for (long id : deadlocked) {
          sb.append(" - Thread Id=").append(id).append('\n');
        }
      }

      sb.append("====== END: GRADLE JVM DIAGNOSTICS ======\n");

      return sb.toString();
    }

    private static String formatBytes(long bytes) {
      if (bytes < 1024) return bytes + " B";
      double v = bytes;
      String[] units = {"KB", "MB", "GB", "TB", "PB"};
      int i = 0;
      while (v >= 1024 && i < units.length - 1) {
        v /= 1024;
        i++;
      }
      return String.format(Locale.ROOT, "%.2f %s", v, units[i]);
    }

    @Override
    public void onFinish(FinishEvent event) {
      lastUpdateNanos = System.nanoTime();
    }

    @Override
    public void close() {
      try {
        this.stop = true;
        this.trackerThread.interrupt();
        this.trackerThread.join(Duration.ofSeconds(10));
      } catch (InterruptedException e) {
        LOGGER.warn("Interrupted while waiting for tracker thread to finish.", e);
      }
    }
  }
}
