package org.apache.lucene.index;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.io.IOException;

/**
 * A multi-tenant merge scheduler that shares a global thread pool across multiple IndexWriters.
 */
public class MultiTenantMergeScheduler extends MergeScheduler {

    // Shared global thread pool with lazy initialization
    private static class LazyHolder {
        static final ExecutorService MERGE_THREAD_POOL = 
            Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() / 2);
    }

    // Use getMergeThreadPool() instead of direct access

    @Override
    public void merge(MergeScheduler.MergeSource mergeSource, MergeTrigger trigger) throws IOException {
        while (mergeSource.hasPendingMerges()) { // Use hasPendingMerges() instead of relying on null check
            MergePolicy.OneMerge merge = mergeSource.getNextMerge();
            if (merge == null) {
                break; // Explicitly exit if no merge is available
            }
            
            // Submit merge task to the shared thread pool
            MERGE_THREAD_POOL.submit(() -> {
                try {
                    mergeSource.merge(merge);
                } catch (IOException e) {
                    throw new RuntimeException("Merge operation failed", e);
                }
            });

            // Cleanup completed merges
            activeMerges.removeIf(Future::isDone);
        }
    }

    @Override
    public void close() throws IOException {
        // Wait for all running merges to complete
        for (Future<?> future : activeMerges) {
            try {
                future.get();  // Wait for completion
            } catch (Exception e) {
                throw new IOException("Error while waiting for merges to finish", e);
            }
        }
        activeMerges.clear();
    }

    // Providing a method to shut down the global thread pool gracefully
    public static void shutdownThreadPool() {
        MERGE_THREAD_POOL.shutdown();
        try {
            if (!MERGE_THREAD_POOL.awaitTermination(60, TimeUnit.SECONDS)) {
                MERGE_THREAD_POOL.shutdownNow();
            }
        } catch (InterruptedException e) {
            MERGE_THREAD_POOL.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
