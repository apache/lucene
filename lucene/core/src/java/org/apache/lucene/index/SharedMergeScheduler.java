package org.apache.lucene.index;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * SharedMergeScheduler is an experimental MergeScheduler that submits merge tasks to a shared
 * thread pool across IndexWriters.
 */
public class SharedMergeScheduler extends MergeScheduler {
 
  // Tracks submitted merges per writer
  private final ConcurrentHashMap<IndexWriter, Set<MergeTaskWrapper>> writerToMerges = new ConcurrentHashMap<>();

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

      // Registering this task under the writer 
      writerToMerges.computeIfAbsent((IndexWriter) mergeSource, k -> new CopyOnWriteArraySet<>()).add(wrappedTask);

      // Submitting task to executor
      executor.submit(() -> {
        try {
            wrappedTask.getMergeTask().run();
        } finally {
            writerToMerges.getOrDefault((IndexWriter) mergeSource, Set.of()).remove(wrappedTask);
        }
      });

    }
  }

  /**
   * Closes the merge scheduler. This implementation is currently a no-op.
   */
  //@Override
  public void close(IndexWriter writer) {  
    Set<MergeTaskWrapper> tasks = writerToMerges.remove(writer);  
    if (tasks != null) {  
        for (MergeTaskWrapper task : tasks) {  
            // Placeholder for canceling tasks if needed.  
        }  
    }  
  }

}
