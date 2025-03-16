package org.apache.lucene.index;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.io.IOException;

/**
 * A multi-tenant merge scheduler that shares a global thread pool across multiple IndexWriters.
 */
public class MultiTenantMergeScheduler extends MergeScheduler {

    // Shared global thread pool
    private static final ExecutorService MERGE_THREAD_POOL = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() / 2);

    @Override
    public void merge(MergeScheduler.MergeSource mergeSource, MergeTrigger trigger) throws IOException {
        while (true) {
            MergePolicy.OneMerge merge = mergeSource.getNextMerge();
            if (merge == null) {
                break;
            }
            // Submit merge task to the shared thread pool
            Future<?> future = MERGE_THREAD_POOL.submit(() -> {
                try {
                    mergeSource.merge(merge);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

            try {
                future.get(); // Ensure the task completes
            } catch (Exception e) {
                throw new IOException("Merge operation failed", e);
            }
        }
    }

    @Override
    public void close() {
        // Do not shut down the thread pool globally since it's shared.
    }
}
