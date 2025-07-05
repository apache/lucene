package org.apache.lucene.index;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * SharedMergeScheduler is an experimental MergeScheduler that submits merge tasks to a shared
 * thread pool across IndexWriters.
 */
public class SharedMergeScheduler extends MergeScheduler {

  /** Shared thread pool executor used by all IndexWriters using this scheduler. */
  private static final ExecutorService sharedExecutor =
      Executors.newFixedThreadPool(4); // Adjust the number of threads as needed

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

      sharedExecutor.submit(
          () -> {
            try {
              mergeSource.merge(merge);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          });
    }
  }

  /**
   * Closes the merge scheduler. This implementation is currently a no-op. In a production-ready
   * version, the shared executor should be properly shut down.
   */
  @Override
  public void close() {
    // no-op for now; in a full version we would shut down the executor
  }
}
