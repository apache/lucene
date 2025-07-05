package org.apache.lucene.index;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * SharedMergeScheduler is an experimental MergeScheduler that submits
 * merge tasks to a shared thread pool across IndexWriters.
 */
public class SharedMergeScheduler extends MergeScheduler {

    private static final ExecutorService sharedExecutor = Executors.newFixedThreadPool(4); // Adjust as needed

    @Override
    public void merge(MergeSource mergeSource, MergeTrigger trigger) throws IOException {
        while (true) {
            final MergePolicy.OneMerge merge = mergeSource.getNextMerge();
            if (merge == null) break;

            sharedExecutor.submit(() -> {
                try {
                    mergeSource.merge(merge);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    @Override
    public void close() {
        // TODO: graceful shutdown logic if needed
    }
}
