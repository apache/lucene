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
package org.apache.lucene.search;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.util.ThreadInterruptedException;

/**
 * A thread that manages periodic reopening of a {@link ReferenceManager}, with methods to wait for
 * a specific index generation to become visible. When a given search request needs to see a
 * specific index change, call {@link #waitForGeneration(long)} to wait for that change to be
 * visible. Note that this will only scale well if most searches do not need to wait for a specific
 * index generation.
 *
 * @lucene.experimental
 */
public class ControlledRealTimeReopenThread<T> extends Thread implements Closeable {
  private final ReferenceManager<T> manager;
  private final long targetMaxStaleNS;
  private final long targetMinStaleNS;
  private final IndexWriter writer;

  private volatile boolean finish;
  private final AtomicLong waitingGen = new AtomicLong(0);
  private volatile long searchingGen;

  /// Releasing a permit to this semaphore is a signal to the background thread to check again.
  private final Semaphore reloadRequests = new Semaphore(0);

  /**
   * Create a new thread instance.
   *
   * <p>Potentially mark it as {@linkplain #setDaemon(boolean) a daemon} and {@link #start()} it
   * subsequently. Call {@link #close()} to cleanly stop this thread.
   *
   * @param targetMaxStaleSec Maximum time until a new reader must be opened; this sets the upper
   *     bound on how slowly reopens may occur, when no caller is waiting for a specific generation
   *     to become visible.
   * @param targetMinStaleSec Minimum time until a new reader can be opened; if a refresh occurred
   *     recently, it will wait at least this time until another refresh, even if a thread is
   *     waiting for it.
   */
  public ControlledRealTimeReopenThread(
      IndexWriter writer,
      ReferenceManager<T> manager,
      double targetMaxStaleSec,
      double targetMinStaleSec) {
    if (targetMaxStaleSec < targetMinStaleSec) {
      throw new IllegalArgumentException(
          "targetMaxScaleSec (= "
              + targetMaxStaleSec
              + ") < targetMinStaleSec (="
              + targetMinStaleSec
              + ")");
    }
    this.writer = writer;
    this.manager = manager;
    this.targetMaxStaleNS = (long) (1000000000 * targetMaxStaleSec);
    this.targetMinStaleNS = (long) (1000000000 * targetMinStaleSec);
    manager.addListener(new HandleRefresh());
  }

  private class HandleRefresh implements ReferenceManager.RefreshListener {
    private long refreshStartGen;

    @Override
    public void beforeRefresh() {
      // Save the gen as of when we started the reopen; the
      // listener (HandleRefresh above) copies this to
      // searchingGen once the reopen completes:
      refreshStartGen = writer.getMaxCompletedSequenceNumber();
    }

    @Override
    public void afterRefresh(boolean didRefresh) {
      synchronized (ControlledRealTimeReopenThread.this) {
        searchingGen = refreshStartGen;
        ControlledRealTimeReopenThread.this.notifyAll();
      }
    }
  }

  /** Stop this thread. Blocks until it terminates. */
  @Override
  public void close() {
    finish = true;

    // Wake up the background thread.
    reloadRequests.release();

    try {
      join();
    } catch (InterruptedException ie) {
      throw new ThreadInterruptedException(ie);
    }

    // Max it out so any waiting search threads will return:
    searchingGen = Long.MAX_VALUE;

    // Wake up waiters.
    synchronized (this) {
      notifyAll();
    }
  }

  /**
   * Waits for the target generation to become visible in the searcher. If the current searcher is
   * older than the target generation, this method will block until the searcher is reopened by
   * another thread calling {@link ReferenceManager#maybeRefresh}, or until the {@link
   * ReferenceManager} is closed.
   *
   * @param targetGen the generation to wait for
   */
  public void waitForGeneration(long targetGen) throws InterruptedException {
    waitForGeneration(targetGen, -1);
  }

  /**
   * Waits for the target generation to become visible in the searcher, up to a maximum specified
   * milliseconds. If the current searcher is older than the target generation, this method will
   * block until the searcher has been reopened by another thread calling {@link
   * ReferenceManager#maybeRefresh}, the given waiting time has elapsed, or until the {@link
   * ReferenceManager} is closed.
   *
   * <p>NOTE: if the waiting time elapses before the requested target generation is available, false
   * is returned.
   *
   * @param targetGen the generation to wait for
   * @param maxMS maximum milliseconds to wait, or -1 to wait indefinitely
   * @return true if the targetGen is now available, or false if maxMS wait time was exceeded or
   *     this instance was closed. True might also be returned if this call returned due to a {@link
   *     #close()}
   */
  public synchronized boolean waitForGeneration(long targetGen, int maxMS)
      throws InterruptedException {
    if (targetGen > searchingGen) {
      waitingGen.updateAndGet(v -> Math.max(v, targetGen));

      // Notify the reopen thread that the waitingGen has
      // changed, so it may wake up and realize it should
      // not sleep for much or any longer before reopening:
      reloadRequests.release();

      long startNS = System.nanoTime();

      while (targetGen > searchingGen) {
        if (maxMS < 0) {
          wait();
        } else {
          long msLeft = maxMS - NANOSECONDS.toMillis(System.nanoTime() - startNS);
          if (msLeft <= 0) {
            return false;
          } else {
            wait(msLeft);
          }
        }
      }
    }

    return true;
  }

  @Override
  public void run() {
    long lastReopenStartNS = System.nanoTime();

    while (!finish) {

      // TODO: try to guesstimate how long reopen might take based on past data?

      // True if we have someone waiting for reopened searcher:
      boolean hasWaiting = waitingGen.get() > searchingGen;
      final long nextReopenStartNS =
          lastReopenStartNS + (hasWaiting ? targetMinStaleNS : targetMaxStaleNS);

      final long sleepNS = nextReopenStartNS - System.nanoTime();

      if (sleepNS > 0) {
        try {
          reloadRequests.tryAcquire(sleepNS, NANOSECONDS);
          reloadRequests.drainPermits();
        } catch (InterruptedException e) {
          throw new ThreadInterruptedException(e);
        }
      } else {
        lastReopenStartNS = System.nanoTime();
        try {
          manager.maybeRefreshBlocking();
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
      }
    }
  }

  /** Returns which {@code generation} the current searcher is guaranteed to include. */
  public long getSearchingGen() {
    return searchingGen;
  }
}
