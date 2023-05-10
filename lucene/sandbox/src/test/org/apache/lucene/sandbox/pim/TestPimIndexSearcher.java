package org.apache.lucene.sandbox.pim;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
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

    public void testBasic() throws Exception {
        PimConfig pimConfig = new PimConfig();
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(getAnalyzer())
                .setMergePolicy(NoMergePolicy.INSTANCE);
        IndexWriter writer = new PimIndexWriter(directory, indexWriterConfig, pimConfig);
        ((PimIndexWriter) writer).setTestName("testBasic");

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

    public void testMoreText() throws Exception {
        PimConfig pimConfig = new PimConfig();
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(getAnalyzer())
                .setMergePolicy(NoMergePolicy.INSTANCE);
        IndexWriter writer = new PimIndexWriter(directory, indexWriterConfig, pimConfig);
        ((PimIndexWriter) writer).setTestName("testMoreText");

        Document doc = new Document();
        doc.add(newTextField("title", "München", Field.Store.YES));
        doc.add(newTextField("body", "Der Name München wird üblicherweise als " +
                "„bei den Mönchen“ gedeutet. Erstmals erwähnt wird der Name als forum apud " +
                "Munichen im Augsburger Schied vom 14. Juni 1158 von Kaiser Friedrich I.[15][16] " +
                "Munichen ist der Dativ Plural von althochdeutsch munih bzw. mittelhochdeutsch mün(e)ch, " +
                "dem Vorläufer von neuhochdeutsch Mönch", Field.Store.YES));
        writer.addDocument(doc);

        doc = new Document();
        doc.add(newTextField("title", "Apache Lucene", Field.Store.YES));
        doc.add(newTextField("body", "While suitable for any application that requires full " +
                "text indexing and searching capability, Lucene is recognized for its utility in the " +
                "implementation of Internet search engines and local, single-site searching.[10][11]. " +
                "Lucene includes a feature to perform a fuzzy search based on edit distance.", Field.Store.YES));
        writer.addDocument(doc);

        doc = new Document();
        doc.add(newTextField("title", "Chartreuse", Field.Store.YES));
        doc.add(newTextField("body", "Poursuivis pendant la Révolution française, les moines " +
                "sont dispersés en 1793. La distillation de la chartreuse s'interrompt alors, mais les " +
                "chartreux réussissent à conserver la recette secrète : le manuscrit est emporté par un des " +
                "pères et une copie est conservée par le moine autorisé à garder le monastère ; lors de son " +
                "incarcération à Bordeaux, ce dernier remet sa copie à un confrère qui finit par la céder à " +
                "un pharmacien de Grenoble, un certain Liotard. ", Field.Store.YES));
        writer.addDocument(doc);

        System.out.println("-- CLOSE -------------------------------");
        writer.close();
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
