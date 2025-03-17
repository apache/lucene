package org.apache.lucene.index;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.util.LuceneTestCase;

public class TestMultiTenantMergeScheduler extends LuceneTestCase {

    public void testMultiTenantMergeScheduler() throws Exception {
        Directory dir = new RAMDirectory();
        IndexWriterConfig config = new IndexWriterConfig(new MockAnalyzer(random()));
        MultiTenantMergeScheduler scheduler = new MultiTenantMergeScheduler();
        config.setMergeScheduler(scheduler);

        IndexWriter writer1 = new IndexWriter(dir, config);
        IndexWriter writer2 = new IndexWriter(dir, config);

        // Add documents and trigger merges
        for (int i = 0; i < 50; i++) {
            writer1.addDocument(new Document());
            writer2.addDocument(new Document());
            if (i % 10 == 0) {
                writer1.commit();
                writer2.commit();
            }
        }

        writer1.forceMerge(1);
        writer2.forceMerge(1);

        // Close writers at different times
        writer1.close();
        Thread.sleep(500);
        writer2.close();

        // Ensure scheduler is properly closed
        scheduler.close();
        MultiTenantMergeScheduler.shutdownThreadPool();

        dir.close();
    }

    public void testConcurrentMerging() throws Exception {
        Directory dir = new RAMDirectory();
        IndexWriterConfig config = new IndexWriterConfig(new MockAnalyzer(random()));
        MultiTenantMergeScheduler scheduler = new MultiTenantMergeScheduler();
        config.setMergeScheduler(scheduler);

        IndexWriter writer = new IndexWriter(dir, config);

        // Add documents
        for (int i = 0; i < 100; i++) {
            writer.addDocument(new Document());
        }
        writer.commit();

        long startTime = System.currentTimeMillis();
        writer.forceMerge(1);
        long endTime = System.currentTimeMillis();

        writer.close();
        scheduler.close();
        MultiTenantMergeScheduler.shutdownThreadPool();

        // Check if merging took less time than sequential execution would
        assertTrue("Merges did not happen concurrently!", (endTime - startTime) < 5000);

        dir.close();
    }
}
