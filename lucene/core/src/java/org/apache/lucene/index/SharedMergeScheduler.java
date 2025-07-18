package org.apache.lucene.index;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * SharedMergeScheduler is an experimental MergeScheduler that submits merge tasks to a shared
 * thread pool across IndexWriters.
 */
public class SharedMergeScheduler extends MergeScheduler {

 /**
 * Executor service provided externally to handle merge tasks.
 * Allows sharing a thread pool across IndexWriters if configured that way.
 */
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
    while (true) {
      final MergePolicy.OneMerge merge = mergeSource.getNextMerge();
      if (merge == null) break;

      Runnable mergeRunnable = () -> {
          try {
              mergeSource.merge(merge);
          } catch (IOException e) {
              throw new RuntimeException(e);
          }
      };

      MergeTaskWrapper wrappedTask = new MergeTaskWrapper(mergeRunnable, (IndexWriter) mergeSource, merge.totalBytesSize());
      executor.submit(wrappedTask.getMergeTask());
          }
  }

  /**
   * Closes the merge scheduler. This implementation is currently a no-op.
   */
  @Override
  public void close() {
      // No-op. Executor is owned by caller, not by this scheduler.
  }

}
