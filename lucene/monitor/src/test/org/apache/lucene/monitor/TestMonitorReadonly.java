package org.apache.lucene.monitor;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.FSDirectory;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;

public class TestMonitorReadonly extends MonitorTestBase{
    private static final Analyzer ANALYZER = new WhitespaceAnalyzer();

    @Test
    public void testSettingCustomDirectory() throws IOException {
        Path indexDirectory = createTempDir();
        Document doc = new Document();
        doc.add(newTextField(FIELD, "This is a test document", Field.Store.NO));

        MonitorConfiguration writeConfig =
                new MonitorConfiguration()
                        .setDirectoryProvider(() -> FSDirectory.open(indexDirectory));

        try (Monitor writeMonitor = new Monitor(ANALYZER, writeConfig)) {
            TermQuery query = new TermQuery(new Term(FIELD, "test"));
            writeMonitor.register(new MonitorQuery("query1", query, query.toString(), Collections.emptyMap()));

            MatchingQueries<QueryMatch> matches = writeMonitor.match(doc, QueryMatch.SIMPLE_MATCHER);
            assertNotNull(matches.getMatches());
            assertEquals(1, matches.getMatchCount());
            assertNotNull(matches.matches("query1"));
        }
    }

    public void testMonitorReadOnlyCouldReadOnTheSameIndex() throws IOException {
        Path indexDirectory = createTempDir();
        Document doc = new Document();
        doc.add(newTextField(FIELD, "This is a test document", Field.Store.NO));

        MonitorConfiguration writeConfig =
                new MonitorConfiguration()
                        .setIndexPath(
                                indexDirectory, MonitorQuerySerializer.fromParser(MonitorTestBase::parse));

        try (Monitor writeMonitor = new Monitor(ANALYZER, writeConfig)) {
            TermQuery query = new TermQuery(new Term(FIELD, "test"));
            writeMonitor.register(new MonitorQuery("query1", query, query.toString(), Collections.emptyMap()));
        }

        MonitorConfiguration readConfig =
                new MonitorConfiguration()
                        .setReadOnly(true)
                        .setIndexPath(
                                indexDirectory, MonitorQuerySerializer.fromParser(MonitorTestBase::parse));

        try (Monitor readMonitor1 = new Monitor(ANALYZER, readConfig)) {
            MatchingQueries<QueryMatch> matches = readMonitor1.match(doc, QueryMatch.SIMPLE_MATCHER);
            assertNotNull(matches.getMatches());
            assertEquals(1, matches.getMatchCount());
            assertNotNull(matches.matches("query1"));
        }

        try (Monitor readMonitor2 = new Monitor(ANALYZER, readConfig)) {
            MatchingQueries<QueryMatch> matches = readMonitor2.match(doc, QueryMatch.SIMPLE_MATCHER);
            assertNotNull(matches.getMatches());
            assertEquals(1, matches.getMatchCount());
            assertNotNull(matches.matches("query1"));

            assertThrows(IllegalStateException.class, () -> {
                TermQuery query = new TermQuery(new Term(FIELD, "test"));
                readMonitor2.register(new MonitorQuery("query1", query, query.toString(), Collections.emptyMap()));
            });
        }
    }
}
