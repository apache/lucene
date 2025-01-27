package org.apache.lucene.sandbox.facet;

import org.apache.lucene.document.Document;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.sandbox.facet.utils.FacetBuilder;
import org.apache.lucene.sandbox.facet.utils.FacetOrchestrator;
import org.apache.lucene.sandbox.facet.utils.TaxonomyFacetBuilder;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.IOUtils;

public class TestFacetOrchestrator extends SandboxFacetTestCase {
    public void testTaxonomyFacets() throws Exception {
        Directory dir = newDirectory();
        Directory taxoDir = newDirectory();

        // Writes facet ords to a separate directory from the
        // main index:
        DirectoryTaxonomyWriter taxoWriter =
                new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);

        FacetsConfig config = new FacetsConfig();
        config.setHierarchical("Publish Date", true);

        RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

        Document doc = new Document();
        doc.add(new FacetField("Author", "Bob"));
        doc.add(new FacetField("Publish Date", "2010", "10", "15"));
        writer.addDocument(config.build(taxoWriter, doc));

        doc = new Document();
        doc.add(new FacetField("Author", "Lisa"));
        doc.add(new FacetField("Publish Date", "2010", "10", "20"));
        writer.addDocument(config.build(taxoWriter, doc));

        doc = new Document();
        doc.add(new FacetField("Author", "Lisa"));
        doc.add(new FacetField("Publish Date", "2012", "1", "1"));
        writer.addDocument(config.build(taxoWriter, doc));

        doc = new Document();
        doc.add(new FacetField("Author", "Susan"));
        doc.add(new FacetField("Publish Date", "2012", "1", "7"));
        writer.addDocument(config.build(taxoWriter, doc));

        doc = new Document();
        doc.add(new FacetField("Author", "Frank"));
        doc.add(new FacetField("Publish Date", "1999", "5", "5"));
        writer.addDocument(config.build(taxoWriter, doc));

        // NRT open
        IndexSearcher searcher = newSearcher(writer.getReader());

        // NRT open
        TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);

        Query query = new MatchAllDocsQuery();

        FacetBuilder authorTop0Builder = new TaxonomyFacetBuilder(config, taxoReader, "Author").withTopN(0);
        FacetBuilder authorTop10Builder = new TaxonomyFacetBuilder(config, taxoReader, "Author").withTopN(10);
        FacetBuilder authorAllBuilder = new TaxonomyFacetBuilder(config, taxoReader, "Author").withTopN(10);
        FacetBuilder publishDateTop10Builder = new TaxonomyFacetBuilder(config, taxoReader, "Publish Date").withTopN(10);
        FacetBuilder publishDateAllBuilder = new TaxonomyFacetBuilder(config, taxoReader, "Publish Date");

        FacetOrchestrator.start()
                .addBuilder(authorTop0Builder)
                .addBuilder(authorTop10Builder)
                .addBuilder(authorAllBuilder)
                .addBuilder(publishDateTop10Builder)
                .addBuilder(publishDateAllBuilder)
                        .collect(query, searcher);

        expectThrows(
                IllegalArgumentException.class,
                () -> {
                    authorTop0Builder.getResult();
                });

        // Retrieve & verify results:
        assertEquals(
                "dim=Publish Date path=[] value=5 childCount=3\n  2010 (2)\n  2012 (2)\n  1999 (1)\n",
                publishDateTop10Builder.getResult().toString());
        assertEquals(
                "dim=Author path=[] value=5 childCount=4\n  Lisa (2)\n  Bob (1)\n  Susan (1)\n  Frank (1)\n",
                authorTop10Builder.getResult().toString());

        assertFacetResult(
                publishDateAllBuilder.getResult(),
                "Publish Date",
                new String[0],
                3,
                5,
                new LabelAndValue("1999", 1), new LabelAndValue("2010", 2), new LabelAndValue("2012", 2));

        assertFacetResult(
                authorAllBuilder.getResult(),
                "Author",
                new String[0],
                4,
                5,
                new LabelAndValue("Bob", 1),
                new LabelAndValue("Frank", 1),
                new LabelAndValue("Lisa", 2),
                new LabelAndValue("Susan", 1));

        // Now user drills down on Publish Date/2010:
        /* TK
        DrillDownQuery q2 = new DrillDownQuery(config);
        q2.add("Publish Date", "2010");
        final CountFacetRecorder countRecorder2 = new CountFacetRecorder();
        collectorManager = new FacetFieldCollectorManager<>(defaultTaxoCutter, countRecorder2);
        searcher.search(q2, collectorManager);

        assertEquals(
                "dim=Author path=[] value=-2147483648 childCount=2\n  Bob (1)\n  Lisa (1)\n",
                getTopChildrenByCount(countRecorder2, taxoReader, 10, "Author").toString());

        assertEquals(1, getSpecificValue(countRecorder2, taxoReader, "Author", "Lisa"));

        assertArrayEquals(
                new int[] {1, 1},
                getCountsForRecordedCandidates(
                        countRecorder2,
                        taxoReader,
                        new FacetLabel[] {
                                new FacetLabel("Author", "Lisa"),
                                new FacetLabel("Author", "Susan"), // 0 count, filtered out
                                new FacetLabel("Author", "DoesNotExist"), // Doesn't exist in the index, filtered out
                                new FacetLabel("Author", "Bob"),
                        }));

        expectThrows(
                AssertionError.class,
                () -> {
                    getTopChildrenByCount(countRecorder2, taxoReader, 10, "Non exitent dim");
                });

         */

        writer.close();
        IOUtils.close(taxoWriter, searcher.getIndexReader(), taxoReader, taxoDir, dir);
    }
}
