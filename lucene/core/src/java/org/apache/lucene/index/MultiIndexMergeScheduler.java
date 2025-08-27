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
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.InfoStream;

/**
 * Multi-index or multi-tenant merge scheduling.
 *
 * <p>MultiIndexMergeScheduler builds on existing functionality in ConcurrentMergeScheduler by
 * automatically tracking merge sources and merge threads and the index Directory that each applies
 * to, and then shunting all of them into a single ConcurrentMergeScheduler instance.
 *
 * <p>The multi-tenant merge scheduling can be used easily by creating a MultiIndexMergeScheduler
 * instance for each index and then using each instance normally, the same way you would use a lone
 * ConcurrentMergeScheduler.
 *
 * @lucene.experimental
 */
class MultiIndexMergeScheduler extends MergeScheduler {
  private final Directory directory;
  private final CombinedMergeScheduler combinedMergeScheduler;
  private final boolean manageSingleton;

  /** The main MultiIndexMergeScheduler constructor -- use this one. */
  public MultiIndexMergeScheduler(Directory directory) {
    this.directory = directory;
    this.combinedMergeScheduler = CombinedMergeScheduler.acquireSingleton();
    this.manageSingleton = true;
  }

  /** Alternate MultiIndexMergeScheduler constructor for unit testing. Does not close() the CMS. */
  MultiIndexMergeScheduler(Directory directory, CombinedMergeScheduler combinedMergeScheduler) {
    this.directory = directory;
    this.combinedMergeScheduler = combinedMergeScheduler;
    this.manageSingleton = false;
  }

  public Directory getDirectory() {
    return directory;
  }

  public CombinedMergeScheduler getCombinedMergeScheduler() {
    return combinedMergeScheduler;
  }

  @Override
  public void merge(MergeSource mergeSource, MergeTrigger trigger) throws IOException {
    var taggedMergeSource =
        new CombinedMergeScheduler.TaggedMergeSource(mergeSource, this.directory);
    this.combinedMergeScheduler.merge(taggedMergeSource, trigger);
  }

  @Override
  public Directory wrapForMerge(MergePolicy.OneMerge merge, Directory in) {
    return this.combinedMergeScheduler.wrapForMerge(merge, in);
  }

  /** Close this scheduler for one directory/index. Called automatically by IndexWriter. */
  @Override
  public void close() throws IOException {
    this.combinedMergeScheduler.sync(this.directory);
    if (this.manageSingleton) {
      CombinedMergeScheduler.releaseSingleton();
    }
  }

  // We created this method because we cannot easily override the initialize() method
  // in ConcurrentMergeScheduler. We don't need the initDynamicDefaults() part in the
  // initialize() method, and only need the setInfoStream().
  public void setInfoStream(InfoStream infoStream) {
    this.combinedMergeScheduler.setInfoStream(infoStream);
  }

  /**
   * CombinedMergeScheduler is used internally by MultiIndexMergeScheduler to balance resources
   * across multiple indices. Normally you don't need to use this.
   *
   * <p>For testing purposes, or if partitioning of tenants into groups is needed for some reason, a
   * CombinedMergeScheduler can be provided to the MultiIndexMergeScheduler constructor.
   *
   * <p>CombinedMergeScheduler should <b><i>not</i></b> be passed directly to IndexWriter.
   */
  static class CombinedMergeScheduler extends ConcurrentMergeScheduler {
    @SuppressWarnings("NonFinalStaticField")
    private static CombinedMergeScheduler singleton = null;

    @SuppressWarnings("NonFinalStaticField")
    private static int singletonRefCount = 0;

    private static synchronized CombinedMergeScheduler acquireSingleton() {
      if (singleton == null) {
        singleton = new CombinedMergeScheduler();
      }
      singletonRefCount++;
      return singleton;
    }

    private static synchronized void releaseSingleton() throws IOException {
      if (singletonRefCount < 1) {
        throw new IllegalStateException("decrementSingletonReference() called too many times");
      }
      singletonRefCount--;
      if (singletonRefCount == 0) {
        singleton.close();
        singleton = null;
      }
    }

    public static synchronized CombinedMergeScheduler peekSingleton() {
      return singleton;
    }

    // A filter pattern
    static class TaggedMergeSource implements MergeScheduler.MergeSource {
      private final MergeScheduler.MergeSource in;
      private final Directory directory;

      TaggedMergeSource(MergeScheduler.MergeSource in, Directory directory) {
        this.in = in;
        this.directory = directory;
      }

      public Directory getDirectory() {
        return this.directory;
      }

      @Override
      public MergePolicy.OneMerge getNextMerge() {
        return this.in.getNextMerge();
      }

      @Override
      public void onMergeFinished(MergePolicy.OneMerge merge) {
        this.in.onMergeFinished(merge);
      }

      @Override
      public boolean hasPendingMerges() {
        return this.in.hasPendingMerges();
      }

      @Override
      public void merge(MergePolicy.OneMerge merge) throws IOException {
        this.in.merge(merge);
      }
    }

    public void setInfoStream(InfoStream infoStream) {
      this.infoStream = infoStream;
    }

    public void sync(Directory directory) {
      boolean interrupted = false;
      try {
        while (true) {
          MergeThread toSync = null;
          synchronized (this) {
            for (MergeThread t : this.mergeThreads) {
              // In case a merge thread is calling us, don't try to sync on
              // itself, since that will never finish!
              if (t.isAlive()
                  && t != Thread.currentThread()
                  // Only wait for merge threads for the current index to finish
                  && t.mergeSource instanceof TaggedMergeSource
                  && ((TaggedMergeSource) t.mergeSource).getDirectory().equals(directory)) {
                toSync = t;
                break;
              }
            }
          }
          if (toSync != null) {
            try {
              toSync.join();
            } catch (InterruptedException _) {
              // ignore this Exception, we will retry until all threads are dead
              interrupted = true;
            }
          } else {
            break;
          }
        }
      } finally {
        // finally, restore interrupt status:
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }
}
