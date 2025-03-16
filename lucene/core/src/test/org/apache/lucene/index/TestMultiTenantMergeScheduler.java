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
        IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
        config.setMergeScheduler(new MultiTenantMergeScheduler());

        IndexWriter writer = new IndexWriter(dir, config);
        
        Document doc = new Document();
        doc.add(new TextField("field", "test data", TextField.Store.YES));
        
        writer.addDocument(doc);
        writer.commit();
        writer.close();

        // Ensure that the merge scheduler didn't cause any issues
        assertTrue(dir.listAll().length > 0);
        dir.close();
    }
}
