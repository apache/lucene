package org.apache.lucene.index;

import java.io.IOException;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * SharedMergeScheduler is an experimental MergeScheduler that submits merge tasks to a shared
 * thread pool across IndexWriters.
 */
public class SharedMergeScheduler extends MergeScheduler {
 
  // Tracks submitted merges per writer
  private final Set<MergeTaskWrapper> mergeTasks = ConcurrentHashMap.newKeySet();

  private final AtomicInteger submittedTasks = new AtomicInteger();
  private final AtomicInteger completedTasks = new AtomicInteger();
  private final Set<String> mergeThreadNames = java.util.Collections.newSetFromMap(new ConcurrentHashMap<>());
  private final ConcurrentHashMap<IndexWriter, CopyOnWriteArraySet<MergeTaskWrapper>> writerToMerges = new ConcurrentHashMap<>();

  private final ExecutorService executor;

  public SharedMergeScheduler(ExecutorService executor) {
      this.executor = executor;
  }

  /**
   * Retrieves pending merge tasks from the given {@link MergeSource} and submits them to the shared
   * thread pool for execution.
   *
   * @param mergeSource the source of merge tasks (typically an IndexWriter)
   * @param trigger the event that triggered the merge (e.g., SEGMENT_FLUSH, EXPLICIT)
   * @throws IOException if merging fails
   */
  @Override
  public void merge(MergeSource mergeSource, MergeTrigger trigger) throws IOException {
    while (mergeSource.hasPendingMerges()) {
      final MergePolicy.OneMerge merge = mergeSource.getNextMerge();

      MergeTaskWrapper wrappedTask = new MergeTaskWrapper(this, null, merge.totalBytesSize());

      Runnable mergeRunnable = () -> {
            mergeThreadNames.add(Thread.currentThread().getName());
            try {
                if (wrappedTask.isAborted()) {
                    return;
                }
                mergeSource.merge(merge);
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                // No cleanup required here at the moment.
                }
      };

      wrappedTask.setRunnable(mergeRunnable);

      mergeTasks.add(wrappedTask);
      submittedTasks.incrementAndGet();

      Future<?> f = executor.submit(wrappedTask);
      wrappedTask.setFuture(f);

    }
  }


  //@Override
  public void close(IndexWriter writer) {
        CopyOnWriteArraySet<MergeTaskWrapper> tasks = writerToMerges.remove(writer);
        if (tasks != null) {
            for (MergeTaskWrapper task : tasks) {
                task.markAborted();
            }
        }
    }

  void onTaskFinished(MergeTaskWrapper task) {
    completedTasks.incrementAndGet();
    mergeTasks.remove(task);  // Clean up from the global task set
   }

  /* ===================== PACKAGE-PRIVATE TEST HOOKS ===================== */

    int getActiveMergeTaskCount() {
        return writerToMerges.values().stream().mapToInt(Set::size).sum();
    }

    int getSubmittedTaskCount() {
        return submittedTasks.get();
    }

    int getCompletedTaskCount() {
        return completedTasks.get();
    }

    Set<String> getMergeThreadNames() {
        return java.util.Collections.unmodifiableSet(mergeThreadNames);
    }

    boolean hasWriter(IndexWriter w) {
        return writerToMerges.containsKey(w);
    }

}

