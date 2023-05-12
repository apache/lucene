package org.apache.lucene.sandbox.pim;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestRuleLimitSysouts;
import org.apache.lucene.util.BytesRef;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;

@TestRuleLimitSysouts.Limit(bytes = 1 << 14, hardLimit = 1 << 14)
public class TestPimIndexSearcher extends LuceneTestCase {

    private static Directory directory;
    private static Directory pimDirectory;
    private IndexReader reader;

    @BeforeClass
    public static void beforeClass() {

        directory = newDirectory();
        pimDirectory = newDirectory();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        directory.close();
        directory = null;
        pimDirectory.close();
        pimDirectory = null;
    }

    @Override
    public void tearDown() throws Exception {
        if (reader != null) {
            reader.close();
            reader = null;
        }
        super.tearDown();
    }

    public void testTermBasic() throws Exception {
        PimConfig pimConfig = new PimConfig();
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(getAnalyzer())
                .setMergePolicy(NoMergePolicy.INSTANCE);
        IndexWriter writer = new PimIndexWriter(directory, pimDirectory, indexWriterConfig, pimConfig);

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

        System.out.println("-- CLOSE -------------------------------");
        writer.close();

        System.out.println("\nTEST PIM INDEX SEARCH (BASIC)");
        PimIndexSearcher pimSearcher = new PimIndexSearcher(directory, pimDirectory, pimConfig);

        var matches = pimSearcher.SearchTerm(new BytesRef("field1"), new BytesRef("yellow"));
        System.out.println("\nSearching for field1:yellow: found " + matches.size() + " results");
        matches.forEach((m) -> {
            System.out.println("Doc:" + m.docId + " freq:" + m.score);
        });
        ArrayList<PimMatch> expectedMatches = new ArrayList<>();
        expectedMatches.add(new PimMatch(0, 1));
        expectedMatches.add(new PimMatch(1, 1));
        assert matches.equals(expectedMatches);

        matches = pimSearcher.SearchTerm(new BytesRef("field1"), new BytesRef("green"));
        System.out.println("\nSearching for field1:green found " + matches.size() + " results");
        matches.forEach((m) -> {
            System.out.println("Doc:" + m.docId + " freq:" + m.score);
        });
        expectedMatches = new ArrayList<>();
        expectedMatches.add(new PimMatch(1, 1));
        assert matches.equals(expectedMatches);

        matches = pimSearcher.SearchTerm(new BytesRef("field2"), new BytesRef("green"));
        System.out.println("\nSearching for field2:green found " + matches.size() + " results");
        matches.forEach((m) -> {
            System.out.println("Doc:" + m.docId + " freq:" + m.score);
        });
        expectedMatches = new ArrayList<>();
        expectedMatches.add(new PimMatch(1, 1));
        assert matches.equals(expectedMatches);

        matches = pimSearcher.SearchTerm(new BytesRef("field2"), new BytesRef("orange"));
        System.out.println("\nSearching for field2:orange found " + matches.size() + " results");
        matches.forEach((m) -> {
            System.out.println("Doc:" + m.docId + " freq:" + m.score);
        });
        expectedMatches = new ArrayList<>();
        expectedMatches.add(new PimMatch(0, 2));
        assert matches.equals(expectedMatches);

        matches = pimSearcher.SearchTerm(new BytesRef("field2"), new BytesRef("yellow"));
        System.out.println("\nSearching for field2:yellow found " + matches.size() + " results");
        matches.forEach((m) -> {
            System.out.println("Doc:" + m.docId + " freq:" + m.score);
        });
        expectedMatches = new ArrayList<>();
        assert matches.equals(expectedMatches);

        matches = pimSearcher.SearchTerm(new BytesRef("id"), new BytesRef("AAC"));
        System.out.println("\nSearching for id:AAC found " + matches.size() + " results");
        matches.forEach((m) -> {
            System.out.println("Doc:" + m.docId + " freq:" + m.score);
        });
        expectedMatches = new ArrayList<>();
        expectedMatches.add(new PimMatch(2, 1));
        assert matches.equals(expectedMatches);
        System.out.println("");

        pimSearcher.close();
    }

    public void testTermMoreText() throws Exception {

        PimConfig pimConfig = new PimConfig();
        writeFewWikiText(pimConfig);

        System.out.println("\nTEST PIM INDEX SEARCH (MORE TEXT)");
        PimIndexSearcher pimSearcher = new PimIndexSearcher(directory, pimDirectory, pimConfig);

        var matches = pimSearcher.SearchTerm(new BytesRef("title"), new BytesRef("Apache"));
        System.out.println("\nSearching for title:Apache: found " + matches.size() + " results");
        matches.forEach((m) -> {
            System.out.println("Doc:" + m.docId + " freq:" + m.score);
        });
        ArrayList<PimMatch> expectedMatches = new ArrayList<>();
        expectedMatches.add(new PimMatch(1, 1));
        assert matches.equals(expectedMatches);

        matches = pimSearcher.SearchTerm(new BytesRef("title"), new BytesRef("München"));
        System.out.println("\nSearching for title:München found " + matches.size() + " results");
        matches.forEach((m) -> {
            System.out.println("Doc:" + m.docId + " freq:" + m.score);
        });
        expectedMatches = new ArrayList<>();
        expectedMatches.add(new PimMatch(0, 1));
        assert matches.equals(expectedMatches);

        matches = pimSearcher.SearchTerm(new BytesRef("body"), new BytesRef("manuscrit"));
        System.out.println("\nSearching for body:manuscrit found " + matches.size() + " results");
        matches.forEach((m) -> {
            System.out.println("Doc:" + m.docId + " freq:" + m.score);
        });
        expectedMatches = new ArrayList<>();
        expectedMatches.add(new PimMatch(2, 1));
        assert matches.equals(expectedMatches);

        matches = pimSearcher.SearchTerm(new BytesRef("body"), new BytesRef("copie"));
        System.out.println("\nSearching for body:copie found " + matches.size() + " results");
        matches.forEach((m) -> {
            System.out.println("Doc:" + m.docId + " freq:" + m.score);
        });
        expectedMatches = new ArrayList<>();
        expectedMatches.add(new PimMatch(2, 2));
        assert matches.equals(expectedMatches);

        matches = pimSearcher.SearchTerm(new BytesRef("body"), new BytesRef("wird"));
        System.out.println("\nSearching for body:wird found " + matches.size() + " results");
        matches.forEach((m) -> {
            System.out.println("Doc:" + m.docId + " freq:" + m.score);
        });
        expectedMatches = new ArrayList<>();
        expectedMatches.add(new PimMatch(0, 2));
        assert matches.equals(expectedMatches);

        matches = pimSearcher.SearchTerm(new BytesRef("body"), new BytesRef("Dativ"));
        System.out.println("\nSearching for body:Dativ found " + matches.size() + " results");
        matches.forEach((m) -> {
            System.out.println("Doc:" + m.docId + " freq:" + m.score);
        });
        expectedMatches = new ArrayList<>();
        expectedMatches.add(new PimMatch(0, 1));
        assert matches.equals(expectedMatches);

        matches = pimSearcher.SearchTerm(new BytesRef("body"), new BytesRef("conservé"));
        System.out.println("\nSearching for body:conservé found " + matches.size() + " results");
        matches.forEach((m) -> {
            System.out.println("Doc:" + m.docId + " freq:" + m.score);
        });
        expectedMatches = new ArrayList<>();
        assert matches.equals(expectedMatches);

        matches = pimSearcher.SearchPhrase(new PimPhraseQuery("title", "Apache", "Lucene"));
        System.out.println("\nSearching for title:[Apache Lucene] found " + matches.size() + " results");
        matches.forEach((m) -> {
            System.out.println("Doc:" + m.docId + " freq:" + m.score);
        });
        expectedMatches = new ArrayList<>();
        expectedMatches.add(new PimMatch(1, 0));
        assert matches.equals(expectedMatches);

        System.out.println("");
        pimSearcher.close();
    }

    public void testPhraseMoreText() throws Exception {

        PimConfig pimConfig = new PimConfig();
        writeFewWikiText(pimConfig);

        System.out.println("\nTEST PIM INDEX SEARCH (PHRASE MORE TEXT)");
        PimIndexSearcher pimSearcher = new PimIndexSearcher(directory, pimDirectory, pimConfig);

        var matches = pimSearcher.SearchPhrase(new PimPhraseQuery("title", "Apache", "Lucene"));
        System.out.println("\nSearching for title:[Apache Lucene] found " + matches.size() + " results");
        matches.forEach((m) -> {
            System.out.println("Doc:" + m.docId + " freq:" + m.score);
        });
        var expectedMatches = new ArrayList<>();
        expectedMatches.add(new PimMatch(1, 0));
        assert matches.equals(expectedMatches);

        matches = pimSearcher.SearchPhrase(new PimPhraseQuery("body", "recette", "secrète"));
        System.out.println("\nSearching for body:[recette secrète] found " + matches.size() + " results");
        matches.forEach((m) -> {
            System.out.println("Doc:" + m.docId + " freq:" + m.score);
        });
        expectedMatches = new ArrayList<>();
        expectedMatches.add(new PimMatch(2, 25));
        assert matches.equals(expectedMatches);

        matches = pimSearcher.SearchPhrase(new PimPhraseQuery("body", "dem", "Vorläufer", "von", "neuhochdeutsch"));
        System.out.println("\nSearching for body:[dem Vorläufer von neuhochdeutsch] found " + matches.size() + " results");
        matches.forEach((m) -> {
            System.out.println("Doc:" + m.docId + " freq:" + m.score);
        });
        expectedMatches = new ArrayList<>();
        expectedMatches.add(new PimMatch(0, 41));
        assert matches.equals(expectedMatches);

        matches = pimSearcher.SearchPhrase(new PimPhraseQuery("body", "fuzzy", "search"));
        System.out.println("\nSearching for body:[fuzzy search] found " + matches.size() + " results");
        matches.forEach((m) -> {
            System.out.println("Doc:" + m.docId + " freq:" + m.score);
        });
        expectedMatches = new ArrayList<>();
        expectedMatches.add(new PimMatch(1, 37));
        expectedMatches.add(new PimMatch(3, 2));
        assert matches.equals(expectedMatches);

        matches = pimSearcher.SearchPhrase(new PimPhraseQuery("body", "edit", "distance."));
        System.out.println("\nSearching for body:[edit distance] found " + matches.size() + " results");
        matches.forEach((m) -> {
            System.out.println("Doc:" + m.docId + " freq:" + m.score);
        });
        expectedMatches = new ArrayList<>();
        expectedMatches.add(new PimMatch(1, 41));
        expectedMatches.add(new PimMatch(3, 47));
        assert matches.equals(expectedMatches);

        matches = pimSearcher.SearchPhrase(new PimPhraseQuery("body", "fuzzy", "search", "based", "on", "edit", "distance."));
        System.out.println("\nSearching for body:[fuzzy search based on edit distance.] found " + matches.size() + " results");
        matches.forEach((m) -> {
            System.out.println("Doc:" + m.docId + " freq:" + m.score);
        });
        expectedMatches = new ArrayList<>();
        expectedMatches.add(new PimMatch(1, 37));
        //assert matches.equals(expectedMatches);

        matches = pimSearcher.SearchPhrase(new PimPhraseQuery("body", "fuzzy", "search", "based", "on", "Levenshtein", "distance."));
        System.out.println("\nSearching for body:[fuzzy search based on Levenshtein distance.] found " + matches.size() + " results");
        matches.forEach((m) -> {
            System.out.println("Doc:" + m.docId + " freq:" + m.score);
        });
        expectedMatches = new ArrayList<>();
        assert matches.equals(expectedMatches);

        matches = pimSearcher.SearchPhrase(new PimPhraseQuery("body", "Lucene", "is",
                "recognized", "for", "its", "utility", "in", "the", "implementation", "of",
                "Internet", "search", "engines"));
        System.out.println("\nSearching for body:[Lucene is recognized for its utility in the" +
                " implementation of Internet search engines] found " + matches.size() + " results");
        matches.forEach((m) -> {
            System.out.println("Doc:" + m.docId + " freq:" + m.score);
        });
        expectedMatches = new ArrayList<>();
        expectedMatches.add(new PimMatch(1, 13));
        assert matches.equals(expectedMatches);

        System.out.println("");
        pimSearcher.close();
    }

    public void testPhraseCornerCases() throws Exception {

        // note pim config is using 2 DPUs unless one won't have any files
        // and thus an empty index
        PimConfig pimConfig = new PimConfig(2);
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(getAnalyzer())
                .setMergePolicy(NoMergePolicy.INSTANCE);
        IndexWriter writer = new PimIndexWriter(directory, pimDirectory, indexWriterConfig, pimConfig);

        Document doc = new Document();
        doc.add(newTextField("title", "blah", Field.Store.YES));
        doc.add(newTextField("body", "blah blah blah blah blah youpi blah blah blah", Field.Store.YES));
        writer.addDocument(doc);
        writer.close();

        System.out.println("\nTEST PIM INDEX SEARCH (PHRASE CORNER CASES)");
        PimIndexSearcher pimSearcher = new PimIndexSearcher(directory, pimDirectory, pimConfig);

        var matches = pimSearcher.SearchPhrase(new PimPhraseQuery("body", "blah", "blah"));
        System.out.println("\nSearching for body:[blah blah] found " + matches.size() + " results");
        matches.forEach((m) -> {
            System.out.println("Doc:" + m.docId + " freq:" + m.score);
        });
        var expectedMatches = new ArrayList<>();
        expectedMatches.add(new PimMatch(0, 0));
        expectedMatches.add(new PimMatch(0, 1));
        expectedMatches.add(new PimMatch(0, 2));
        expectedMatches.add(new PimMatch(0, 3));
        expectedMatches.add(new PimMatch(0, 6));
        expectedMatches.add(new PimMatch(0, 7));
        assert matches.equals(expectedMatches);

        matches = pimSearcher.SearchPhrase(new PimPhraseQuery("body", "blah", "blah", "blah"));
        System.out.println("\nSearching for body:[blah blah blah] found " + matches.size() + " results");
        matches.forEach((m) -> {
            System.out.println("Doc:" + m.docId + " freq:" + m.score);
        });
        expectedMatches = new ArrayList<>();
        expectedMatches.add(new PimMatch(0, 0));
        expectedMatches.add(new PimMatch(0, 1));
        expectedMatches.add(new PimMatch(0, 2));
        expectedMatches.add(new PimMatch(0, 6));
        assert matches.equals(expectedMatches);

        matches = pimSearcher.SearchPhrase(new PimPhraseQuery("body", "blah", "youpi", "blah"));
        System.out.println("\nSearching for body:[blah youpi blah] found " + matches.size() + " results");
        matches.forEach((m) -> {
            System.out.println("Doc:" + m.docId + " freq:" + m.score);
        });
        expectedMatches = new ArrayList<>();
        expectedMatches.add(new PimMatch(0, 4));
        assert matches.equals(expectedMatches);

        System.out.println("");
        pimSearcher.close();
    }

    void writeFewWikiText(PimConfig pimConfig) throws IOException {

        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(getAnalyzer())
                .setMergePolicy(NoMergePolicy.INSTANCE);
        IndexWriter writer = new PimIndexWriter(directory, pimDirectory, indexWriterConfig, pimConfig);

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

        writer.commit();

        doc = new Document();
        doc.add(newTextField("title", "Chartreuse", Field.Store.YES));
        doc.add(newTextField("body", "Poursuivis pendant la Révolution française, les moines " +
                "sont dispersés en 1793. La distillation de la chartreuse s'interrompt alors, mais les " +
                "chartreux réussissent à conserver la recette secrète : le manuscrit est emporté par un des " +
                "pères et une copie est conservée par le moine autorisé à garder le monastère ; lors de son " +
                "incarcération à Bordeaux, ce dernier remet sa copie à un confrère qui finit par la céder à " +
                "un pharmacien de Grenoble, un certain Liotard. ", Field.Store.YES));
        writer.addDocument(doc);

        doc = new Document();
        doc.add(newTextField("title", "FuzzyQuery", Field.Store.YES));
        doc.add(newTextField("body", "Implements the fuzzy search query. The similarity " +
                "measurement is based on the Damerau-Levenshtein (optimal string alignment) algorithm, though" +
                " you can explicitly choose classic Levenshtein by passing false to the transpositions parameter.\n" +
                "This query uses MultiTermQuery.TopTermsBlendedFreqScoringRewrite as default. So terms will " +
                "be collected and scored according to their edit distance. Only the top terms are used for building " +
                "the BooleanQuery. It is not recommended to change the rewrite mode for fuzzy queries.", Field.Store.YES));
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
