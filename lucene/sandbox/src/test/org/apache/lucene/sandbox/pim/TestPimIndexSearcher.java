package org.apache.lucene.sandbox.pim;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestPimIndexSearcher extends LuceneTestCase {

    private static Directory directory;
    private IndexReader reader;

    @BeforeClass
    public static void beforeClass() {
        directory = newDirectory();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        directory.close();
        directory = null;
    }

    @Override
    public void tearDown() throws Exception {
        if (reader != null) {
            reader.close();
            reader = null;
        }
        super.tearDown();
    }

    public void test() throws Exception {
        PimConfig pimConfig = new PimConfig();
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(getAnalyzer())
                .setMergePolicy(NoMergePolicy.INSTANCE);
        IndexWriter writer = new PimIndexWriter(directory, indexWriterConfig, pimConfig);

        Document doc = new Document();
        doc.add(newTextField("id", "AAA", Field.Store.YES));
        doc.add(newTextField("field1", "red black yellow", Field.Store.YES));
        doc.add(newTextField("field2", "red orange white orange red", Field.Store.YES));
        writer.addDocument(doc);

        doc = new Document();
        doc.add(newTextField("id", "AAB", Field.Store.YES));
        doc.add(newTextField("field1", "yellow green blue", Field.Store.YES));
        doc.add(newTextField("field2", "green red", Field.Store.YES));
        writer.addDocument(doc);

        doc = new Document();
        doc.add(newTextField("id", "AAC", Field.Store.YES));
        doc.add(newTextField("field1", "black blue pink", Field.Store.YES));
        doc.add(newTextField("field2", "white brown", Field.Store.YES));
        writer.addDocument(doc);

        /*
        doc = new Document();
        doc.add(newTextField("id", "AAB", Field.Store.YES));
        doc.add(newTextField("field1", "yellow greenish blue", Field.Store.YES));
        doc.add(newTextField("field2", "greenish red", Field.Store.YES));
        writer.updateDocument(new Term("id", new BytesRef("AAB")), doc);*/

        System.out.println("-- CLOSE -------------------------------");
        writer.close();

        //TODO need to see how to make sure the PIM index files are kept
        // in the directory. For the moment they are dropped on writer.close
        /*
        System.out.println("-- Searching PIM index ------------------");
        System.out.println("list directory files: " + directory.toString());
        for(String s : directory.listAll()) { System.out.println(s); };
        PimIndexSearcher pimSearcher = new PimIndexSearcher(directory, pimConfig);
        var matches = pimSearcher.SearchTerm(newBytesRef("field1"), new BytesRef("yellow"));

        reader = DirectoryReader.open(directory);
        IndexSearcher searcher = new IndexSearcher(reader);

        Query query = new PhraseQuery(2, "field", "one", "five");
        ScoreDoc[] hits = searcher.search(query, 20).scoreDocs;
        assertEquals(0, hits.length);
        QueryUtils.check(random(), query, searcher);
        */
    }

    private Analyzer getAnalyzer() {
        return
                new Analyzer() {
                    @Override
                    public TokenStreamComponents createComponents(String fieldName) {
                        return new TokenStreamComponents(new MockTokenizer(MockTokenizer.WHITESPACE, false));
                    }
                };
    }
}
