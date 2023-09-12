/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.demo.facet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.DrillDownQuery;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.AssociationAggregationFunction;
import org.apache.lucene.facet.taxonomy.FloatAssociationFacetField;
import org.apache.lucene.facet.taxonomy.IntAssociationFacetField;
import org.apache.lucene.facet.taxonomy.IntPerLabelAssociationFacetField;
import org.apache.lucene.facet.taxonomy.TaxonomyFacetFloatAssociations;
import org.apache.lucene.facet.taxonomy.TaxonomyFacetIntAssociations;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;

/** Shows example usage of category associations. */
public class PerLabelAssociationsFacetsExample {

    private final Directory indexDir = new ByteBuffersDirectory();
    private final Directory taxoDir = new ByteBuffersDirectory();
    private final FacetsConfig config;

    /** Empty constructor */
    public PerLabelAssociationsFacetsExample() {
        config = new FacetsConfig();
    }

    /** Build the example index. */
    private void index() throws IOException {
        IndexWriterConfig iwc =
                new IndexWriterConfig(new WhitespaceAnalyzer()).setOpenMode(OpenMode.CREATE);
        IndexWriter indexWriter = new IndexWriter(indexDir, iwc);

        DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);

        Document doc = new Document();
        doc.add(new StringField("title", "The Silmarillion", Field.Store.NO));
        // We will associate a popularity score to each author. Tolkie's score is 8.
        doc.add(new IntPerLabelAssociationFacetField(8, "author", "Tolkien"));
        indexWriter.addDocument(config.build(taxoWriter, doc));

        doc = new Document();
        doc.add(new StringField("title", "The Lord of the Rings", Field.Store.NO));
        // The association is fixed to 8. Tolkien's score is not changed to 9.
        doc.add(new IntPerLabelAssociationFacetField(9, "author", "Tolkien"));
        indexWriter.addDocument(config.build(taxoWriter, doc));

        doc = new Document();
        doc.add(new StringField("title", "The Chronicles of Narnia", Field.Store.NO));
        // Lewis does not have a score yet, we can assign 6.
        doc.add(new IntPerLabelAssociationFacetField(6, "author", "Lewis"));
        indexWriter.addDocument(config.build(taxoWriter, doc));

        IOUtils.close(indexWriter, taxoWriter);
    }

    /** User runs a query and aggregates facets by summing their association values. */
    private List<FacetResult> maxAssociations() throws IOException {
        DirectoryReader indexReader = DirectoryReader.open(indexDir);
        IndexSearcher searcher = new IndexSearcher(indexReader);
        TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);

        FacetsCollector fc = new FacetsCollector();

        FacetsCollector.search(searcher, new MatchAllDocsQuery(), 10, fc);

        Facets popularity =
                new TaxonomyFacetIntAssociations(
                        FacetsConfig.DEFAULT_INDEX_FIELD_NAME, taxoReader, config, fc, AssociationAggregationFunction.MAX);

        // Retrieve results
        List<FacetResult> results = new ArrayList<>();
        results.add(popularity.getTopChildren(10, "author"));

        IOUtils.close(indexReader, taxoReader);

        return results;
    }

    /** Runs max association example. */
    public List<FacetResult> runMaxAssociations() throws IOException {
        index();
        return maxAssociations();
    }

    /** Runs the example and prints the results. */
    public static void main(String[] args) throws Exception {
        System.out.println("Max associations example:");
        System.out.println("-------------------------");
        List<FacetResult> results = new PerLabelAssociationsFacetsExample().runMaxAssociations();
        // We get a value of 8 for Tolkien. The score of 9 in the second doc
        // was not considered because we had already assigned a score of 8.
        System.out.println("author: " + results.get(0));
    }
}
