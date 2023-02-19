package org.apache.lucene.queryparser.util;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.store.BaseDirectoryWrapper;
import org.apache.lucene.tests.util.LuceneTestCase;

import java.io.IOException;
import java.util.Set;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestMultiwordSynonymParsing extends LuceneTestCase {

    private DirectoryReader reader;
    private IndexSearcher searcher;
    private BaseDirectoryWrapper directory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        MockAnalyzer analyzer = new MockAnalyzer(random());
        directory = newDirectory();
        IndexWriter w = new IndexWriter(directory, newIndexWriterConfig(analyzer));
        indexDoc(w, "guinea pork");
        indexDoc(w, "guinea pig");
        indexDoc(w, "cavy");
        w.close();
        reader = DirectoryReader.open(directory);
        searcher = newSearcher(reader);
    }

    private static void indexDoc(IndexWriter w, String text) throws IOException {
        Document doc = new Document();
        doc.add(newTextField("field", text, Field.Store.YES));
        w.addDocument(doc);
    }

    @Override
    public void tearDown() throws Exception {
        reader.close();
        directory.close();
        super.tearDown();
    }

    public void testNoSynonym() throws Exception {
        final QueryParser parser = new QueryParser("field", new MockAnalyzer(random()));
        parser.setSplitOnWhitespace(false);
        assertEquals(Set.of("guinea pork", "guinea pig"), search(parser.parse("foo guinea fur")));
    }

    public void testMutiSynonymASIS() throws Exception {
        final QueryParser parser = new QueryParser("field", new QueryParserTestBase.Analyzer1());
        parser.setSplitOnWhitespace(false);
        assertEquals("where's 'guinea pork'?? ", Set.of("guinea pig", "cavy"), search(parser.parse("foo guinea pig")));
    }

    public void testMutiSynonymRegression() throws Exception {
        final QueryParser parser = new QueryParser("field", new QueryParserTestBase.Analyzer1());
        parser.setSplitOnWhitespace(false);
        assertEquals("Synonym prevent pork from matching!",
                Set.of("guinea pig", "cavy", "guinea pork"), search(parser.parse("foo guinea pig")));
    }

    private Set<Object> search(Query query) throws IOException {
        final StoredFields storedFields = reader.storedFields();
        IntFunction<String> docField = i -> {
            try {
                return storedFields.document(i).get("field");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
        return Stream.of(
                        searcher.search(query, 100).scoreDocs).map(scoreDoc -> docField.apply(scoreDoc.doc))
                .collect(Collectors.toSet());
    }
}
