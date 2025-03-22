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
    private static final ExecutorService MERGE_THREAD_POOL = Executors.newFixedThreadPool(
        Runtime.getRuntime().availableProcessors() / 2
    );

    // Track active merges per writer
    private final List<Future<?>> activeMerges = Collections.synchronizedList(new ArrayList<>());

    @Override
    public void merge(MergeScheduler.MergeSource mergeSource, MergeTrigger trigger) throws IOException {
        while (true) {
            MergePolicy.OneMerge merge = mergeSource.getNextMerge();
            if (merge == null) break;  // No more merges
            
            // Submit merge task and track future
            Future<?> future = MERGE_THREAD_POOL.submit(() -> {
                try {
                    mergeSource.merge(merge);
                } catch (IOException e) {
                    throw new RuntimeException("Merge operation failed", e);
                }
            });

            activeMerges.add(future);
            
            // Cleanup completed merges
            activeMerges.removeIf(Future::isDone);
        }
    }

    private final ConcurrentHashMap<IndexWriter, List<Merge>> activeMerges = new ConcurrentHashMap<>();

    @Override
    public void close() throws IOException {
        IndexWriter currentWriter = getCurrentIndexWriter();  // Method to get the calling writer
        List<Merge> merges = activeMerges.getOrDefault(currentWriter, Collections.emptyList());
    
        for (Merge merge : merges) {
            merge.waitForCompletion(); // Only wait for merges related to this writer
        }
    
        activeMerges.remove(currentWriter); // Cleanup after closing
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
