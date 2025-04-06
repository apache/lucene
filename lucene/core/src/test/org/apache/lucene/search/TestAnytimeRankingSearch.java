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
package org.apache.lucene.search;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.BinScoreReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.BinScoreUtil;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.NamedThreadFactory;
import org.junit.Test;

public class TestAnytimeRankingSearch extends LuceneTestCase {
    private void addDocument(IndexWriter writer, String content, int docID) throws IOException {
        FieldType type = new FieldType();
        type.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        type.setTokenized(true);
        type.setStored(true);
        type.setStoreTermVectors(true);
        type.putAttribute("doBinning", "true"); // 👈 enable binning
        type.freeze();

        Document doc = new Document();
        doc.add(new IntPoint("docID", docID));
        doc.add(new StoredField("docID", docID));
        doc.add(new Field("content", content, type)); // 👈 use the custom FieldType

        writer.addDocument(doc);
    }

    public void testAnytimeRanking() throws Exception {
        String indexPath = Files.createTempDirectory("testindex1").toAbsolutePath().toString();

        try (Directory directory = FSDirectory.open(Paths.get(indexPath));
             IndexWriter writer =
                     new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer()))) {

            for (int i = 1; i <= 10000; i++) {
                String content =
                        "Lucene document "
                                + i
                                + " ranking relevance retrieval performance precision recall diversity feedback tuning"
                                .split(" ")[i % 10];
                addDocument(writer, content, i);
            }

            writer.commit();
        }

        try (IndexReader wrapped = BinScoreUtil.wrap(DirectoryReader.open(FSDirectory.open(Paths.get(indexPath))))) {
            IndexSearcher searcher = newSearcher(wrapped);
            searcher.setSimilarity(new BM25Similarity());
            Query query = new TermQuery(new Term("content", "lucene"));

            Map<Integer, Double> rangeScores = new HashMap<>();
            for (int i = 1; i <= 10; i++) {
                rangeScores.put(i, random().nextDouble());
            }

            AnytimeRankingSearcher anytimeSearcher =
                    new AnytimeRankingSearcher(searcher, 10, 50, "content");
            int cpus = Runtime.getRuntime().availableProcessors();
            ExecutorService executor =
                    Executors.newFixedThreadPool(cpus, new NamedThreadFactory("hunspellStemming-"));
            List<Future<TopDocs>> futures = new ArrayList<>();

            searcher.setSimilarity(new BM25Similarity(5.0f, 0.2f)); // Stronger term frequency weighting

            long startTime = System.nanoTime();
            for (int i = 0; i < 10; i++) {
                futures.add(
                        executor.submit(() -> anytimeSearcher.search(query)));
            }

            for (Future<TopDocs> future : futures) {
                TopDocs results = future.get();
                assertNotNull("Results should not be null", results);
                assertTrue("At least one result should be retrieved", results.scoreDocs.length > 0);
            }
            executor.shutdown();
            long duration = (System.nanoTime() - startTime) / 1_000_000;
            System.out.println("Total query execution time: " + duration + " ms");
        }
    }

    public void testDifferentQueries() throws Exception {
        String indexPath = Files.createTempDirectory("testindex2").toAbsolutePath().toString();

        try (Directory directory = FSDirectory.open(Paths.get(indexPath));
             IndexWriter writer =
                     new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer()))) {

            for (int i = 1; i <= 10000; i++) {
                String content =
                        "Lucene document "
                                + i
                                + " "
                                + " ranking relevance "
                                + TestUtil.randomSimpleString(random());
                addDocument(writer, content, i);
            }

            writer.commit();
        }

        try (IndexReader wrapped = BinScoreUtil.wrap(DirectoryReader.open(FSDirectory.open(Paths.get(indexPath))))) {
            IndexSearcher searcher = newSearcher(wrapped);
            searcher.setSimilarity(new BM25Similarity());

            Query[] queries = {
                    new TermQuery(new Term("content", "lucene")),
                    new TermQuery(new Term("content", "document")),
                    new TermQuery(new Term("content", "relevance"))
            };

            for (Query query : queries) {
                AnytimeRankingSearcher anytimeSearcher =
                        new AnytimeRankingSearcher(searcher, 10, 100, "content");
                TopDocs results = anytimeSearcher.search(query);
                assertNotNull("Results should not be null", results);
                assertTrue("At least one result should be retrieved", results.scoreDocs.length > 0);
            }
        }
    }

    public void testEmptyIndex() throws Exception {
        String indexPath = Files.createTempDirectory("testindex3").toAbsolutePath().toString();

        try (Directory directory = FSDirectory.open(Paths.get(indexPath));
             IndexWriter writer =
                     new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer()))) {
            writer.commit(); // Commit empty index
        }

        try (IndexReader wrapped = BinScoreUtil.wrap(DirectoryReader.open(FSDirectory.open(Paths.get(indexPath))))) {
            IndexSearcher searcher = newSearcher(wrapped);
            searcher.setSimilarity(new BM25Similarity());
            Query query = new TermQuery(new Term("content", "lucene"));

            AnytimeRankingSearcher anytimeSearcher =
                    new AnytimeRankingSearcher(searcher, 10, 50, "content");
            TopDocs results = anytimeSearcher.search(query);
            assertNotNull("Results should not be null", results);
            assertEquals("No results should be found in an empty index", 0, results.totalHits.value());
        }
    }

    public void testConcurrentQueries() throws Exception {
        String indexPath = Files.createTempDirectory("testindex4").toAbsolutePath().toString();

        try (Directory directory = FSDirectory.open(Paths.get(indexPath));
             IndexWriter writer =
                     new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer()))) {

            for (int i = 1; i <= 10000; i++) {
                String content =
                        "Lucene document "
                                + i
                                + " "
                                + " ranking relevance "
                                + " "
                                + TestUtil.randomSimpleString(random());
                addDocument(writer, content, i);
            }

            writer.commit();
        }

        try (IndexReader wrapped = BinScoreUtil.wrap(DirectoryReader.open(FSDirectory.open(Paths.get(indexPath))))) {
            IndexSearcher searcher = newSearcher(wrapped);
            searcher.setSimilarity(new BM25Similarity());

            Query[] queries = {
                    new TermQuery(new Term("content", "lucene")),
                    new TermQuery(new Term("content", "document")),
                    new TermQuery(new Term("content", "ranking"))
            };

            AnytimeRankingSearcher anytimeSearcher =
                    new AnytimeRankingSearcher(searcher, 10, 50, "content");
            int cpus = Runtime.getRuntime().availableProcessors();
            ExecutorService executor =
                    Executors.newFixedThreadPool(cpus, new NamedThreadFactory("hunspellStemming-"));
            List<Future<TopDocs>> futures = new ArrayList<>();

            for (Query query : queries) {
                futures.add(
                        executor.submit(() -> anytimeSearcher.search(query)));
            }

            for (Future<TopDocs> future : futures) {
                TopDocs results = future.get();
                assertNotNull("Results should not be null", results);
                System.out.println(
                        "Concurrent query executed with " + results.totalHits.value() + " results.");
            }
            executor.shutdown();
        }
    }

    @Test
    public void testSlaCutoffTriggersEarlyTermination() throws Exception {
        String indexPath = Files.createTempDirectory("testindex5").toAbsolutePath().toString();

        try (Directory directory = FSDirectory.open(Paths.get(indexPath));
             IndexWriter writer =
                     new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer()))) {

            for (int i = 1; i <= 10000; i++) {
                String content =
                        "Lucene document "
                                + i
                                + " "
                                + " ranking relevance "
                                + " "
                                + TestUtil.randomSimpleString(random());
                addDocument(writer, content, i);
            }

            writer.commit();
        }

        try (IndexReader wrapped = BinScoreUtil.wrap(DirectoryReader.open(FSDirectory.open(Paths.get(indexPath))))) {
            IndexSearcher searcher = newSearcher(wrapped);
            searcher.setSimilarity(new BM25Similarity());

            Query query = new TermQuery(new Term("content", "lucene"));
            AnytimeRankingSearcher anytimeSearcher =
                    new AnytimeRankingSearcher(searcher, 10, 1, "content"); // Intentionally tight SLA
            TopDocs results = anytimeSearcher.search(query);

            assertNotNull(results);
            assertTrue("Expect partial results due to SLA cutoff", results.scoreDocs.length > 0);
        }
    }

    public void testSlaCutoffOnMultiSegment() throws Exception {
        try (Directory dir = newDirectory()) {
            IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());
            iwc.setMaxBufferedDocs(3); // Force small segment flushes
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                for (int i = 0; i < 100; i++) {
                    Document doc = new Document();
                    doc.add(new IntPoint("docID", i));
                    doc.add(new StoredField("docID", i));
                    doc.add(new TextField("content", "Lucene fast early termination test", Field.Store.NO));
                    writer.addDocument(doc);
                }
                writer.commit();
            }

            try (IndexReader wrapped = BinScoreUtil.wrap(DirectoryReader.open(dir))) {
                IndexSearcher searcher = newSearcher(wrapped);
                searcher.setSimilarity(new BM25Similarity());

                AnytimeRankingSearcher anytimeSearcher = new AnytimeRankingSearcher(searcher, 10, 1, "content");
                TopDocs results = anytimeSearcher.search(new TermQuery(new Term("content", "lucene")));

                assertNotNull(results);
                assertTrue("Should return partial results under SLA cutoff", results.scoreDocs.length > 0);
            }
        }
    }

    public void testTopDocsFromAllSegments() throws Exception {
        try (Directory dir = newDirectory()) {
            IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());
            iwc.setMaxBufferedDocs(4);
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                for (int i = 0; i < 40; i++) {
                    Document doc = new Document();
                    doc.add(new IntPoint("docID", i));
                    doc.add(new StoredField("docID", i));
                    doc.add(new TextField("content", "Lucene document " + i, Field.Store.NO));
                    writer.addDocument(doc);
                }
                writer.commit();
            }

            try (IndexReader wrapped = BinScoreUtil.wrap(DirectoryReader.open(dir))) {
                IndexSearcher searcher = newSearcher(wrapped);
                searcher.setSimilarity(new BM25Similarity());

                AnytimeRankingSearcher anytimeSearcher = new AnytimeRankingSearcher(searcher, 10, 100, "content");
                TopDocs topDocs = anytimeSearcher.search(new TermQuery(new Term("content", "lucene")));

                assertNotNull(topDocs);
                assertTrue("TopDocs should contain results", topDocs.scoreDocs.length > 0);

                int[] segmentDocCounts = new int[wrapped.leaves().size()];
                for (ScoreDoc sd : topDocs.scoreDocs) {
                    for (int i = 0; i < wrapped.leaves().size(); i++) {
                        LeafReaderContext ctx = wrapped.leaves().get(i);
                        if (sd.doc >= ctx.docBase && sd.doc < ctx.docBase + ctx.reader().maxDoc()) {
                            segmentDocCounts[i]++;
                            break;
                        }
                    }
                }

                int segmentsWithHits = 0;
                for (int count : segmentDocCounts) {
                    if (count > 0) {
                        segmentsWithHits++;
                    }
                }

                assertTrue("Results should span across multiple segments", segmentsWithHits > 1);
            }
        }
    }

    public void testBoostedBinHasHigherRankShare() throws Exception {
        try (Directory dir = newDirectory()) {
            IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());
            iwc.setMaxBufferedDocs(10);
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                FieldType ft = new FieldType();
                ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
                ft.setTokenized(true);
                ft.setStored(false);
                ft.setStoreTermVectors(true);
                ft.setStoreTermVectorPositions(true);
                ft.setStoreTermVectorOffsets(true);
                ft.setStoreTermVectorPayloads(true);
                ft.putAttribute("postingsFormat", "Lucene101");
                ft.putAttribute("doBinning", "true");
                ft.putAttribute("bin.count", "2");
                ft.freeze();

                // Bin 0 documents: should dominate
                for (int i = 0; i < 20; i++) {
                    Document doc = new Document();
                    doc.add(new IntPoint("docID", i));
                    doc.add(new StoredField("docID", i));
                    doc.add(new Field("content", "lucene scoring relevance boost", ft));
                    writer.addDocument(doc);
                }

                // Bin 1+ documents: lower term relevance
                for (int i = 20; i < 100; i++) {
                    Document doc = new Document();
                    doc.add(new IntPoint("docID", i));
                    doc.add(new StoredField("docID", i));
                    doc.add(new Field("content", "generic filler content text data", ft));
                    writer.addDocument(doc);
                }

                writer.commit();
            }

            IndexReader base = DirectoryReader.open(dir);
            IndexReader reader = BinScoreUtil.wrap(base);
            try {
                IndexSearcher searcher = newSearcher(reader);
                searcher.setSimilarity(new BM25Similarity());

                AnytimeRankingSearcher s = new AnytimeRankingSearcher(searcher, 10, 50, "content");
                TopDocs topDocs = s.search(new TermQuery(new Term("content", "lucene")));

                assertNotNull(topDocs);
                assertTrue("Expected some results", topDocs.scoreDocs.length > 0);

                int boostedBinHits = 0;
                int checked = 0;

                for (ScoreDoc sd : topDocs.scoreDocs) {
                    int docID = sd.doc;
                    for (LeafReaderContext ctx : reader.leaves()) {
                        if (docID >= ctx.docBase && docID < ctx.docBase + ctx.reader().maxDoc()) {
                            int segDoc = docID - ctx.docBase;
                            BinScoreReader binScoreReader = BinScoreUtil.getBinScoreReader(ctx.reader());
                            if (binScoreReader != null) {
                                int bin = binScoreReader.getBinForDoc(segDoc);
                                if (bin == 0) {
                                    boostedBinHits++;
                                }
                                checked++;
                            }
                            break;
                        }
                    }
                }

                assertTrue("Bin 0 should dominate top ranks, got " + boostedBinHits + " of " + checked,
                        checked > 0 && boostedBinHits >= (checked / 2));
            } finally {
                IOUtils.close(reader, base); // ensure both are closed
            }
        }
    }

    public void testRankingAcrossMultipleSegmentsWithBinBoosts() throws Exception {
        try (Directory dir = newDirectory()) {
            IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());
            iwc.setMaxBufferedDocs(5); // force multiple small segments

            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                FieldType ft = new FieldType();
                ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
                ft.setTokenized(true);
                ft.setStored(false);
                ft.setStoreTermVectors(true);
                ft.setStoreTermVectorPositions(true);
                ft.setStoreTermVectorOffsets(true);
                ft.setStoreTermVectorPayloads(true);
                ft.putAttribute("postingsFormat", "Lucene101");
                ft.putAttribute("doBinning", "true");
                ft.putAttribute("bin.count", "4");
                ft.freeze();

                // Bin 0 documents — relevant term present
                for (int i = 0; i < 15; i++) {
                    Document doc = new Document();
                    doc.add(new IntPoint("docID", i));
                    doc.add(new StoredField("docID", i));
                    doc.add(new Field("content", "lucene relevant document boosting", ft));
                    writer.addDocument(doc);
                }

                // Bin 1 documents — filler
                for (int i = 15; i < 45; i++) {
                    Document doc = new Document();
                    doc.add(new IntPoint("docID", i));
                    doc.add(new StoredField("docID", i));
                    doc.add(new Field("content", "generic text without relevance", ft));
                    writer.addDocument(doc);
                }

                writer.commit();
            }

            IndexReader base = DirectoryReader.open(dir);
            IndexReader wrapped = BinScoreUtil.wrap(base);

            try {
                IndexSearcher searcher = newSearcher(wrapped);
                searcher.setSimilarity(new BM25Similarity());

                AnytimeRankingSearcher rankingSearcher = new AnytimeRankingSearcher(searcher, 10, 50, "content");
                TopDocs topDocs = rankingSearcher.search(new TermQuery(new Term("content", "lucene")));

                assertNotNull(topDocs);
                assertTrue("Expected some results", topDocs.scoreDocs.length > 0);

                int bin0Hits = 0;
                int total = 0;
                for (ScoreDoc sd : topDocs.scoreDocs) {
                    int docID = sd.doc;
                    for (LeafReaderContext ctx : wrapped.leaves()) {
                        if (docID >= ctx.docBase && docID < ctx.docBase + ctx.reader().maxDoc()) {
                            int segDoc = docID - ctx.docBase;
                            BinScoreReader reader = BinScoreUtil.getBinScoreReader(ctx.reader());
                            if (reader != null) {
                                int bin = reader.getBinForDoc(segDoc);
                                if (bin == 0) {
                                    bin0Hits++;
                                }
                                total++;
                            }
                            break;
                        }
                    }
                }

                assertTrue("Bin 0 should dominate results across segments, got " + bin0Hits + " of " + total,
                        total > 0 && bin0Hits >= (total / 2));
            } finally {
                IOUtils.close(wrapped, base);
            }
        }
    }

    public void testSparseBinDistribution() throws Exception {
        try (Directory dir = newDirectory()) {
            IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());
            iwc.setMaxBufferedDocs(5); // force multiple small segments
            iwc.setMergePolicy(NoMergePolicy.INSTANCE);

            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                FieldType ft = new FieldType();
                ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
                ft.setTokenized(true);
                ft.setStored(false);
                ft.setStoreTermVectors(true);
                ft.setStoreTermVectorPositions(true);
                ft.setStoreTermVectorOffsets(true);
                ft.setStoreTermVectorPayloads(true);
                ft.putAttribute("postingsFormat", "Lucene101");
                ft.putAttribute("doBinning", "true");
                ft.putAttribute("bin.count", "4");
                ft.freeze();

                for (int i = 0; i < 64; i++) {
                    Document doc = new Document();
                    doc.add(new IntPoint("docID", i));
                    doc.add(new StoredField("docID", i));
                    String content = (i % 8 == 0) ? "lucene boost" : "noise filler content";
                    doc.add(new Field("content", content, ft));
                    writer.addDocument(doc);
                }

                writer.commit();
            }

            IndexReader base = DirectoryReader.open(dir);
            IndexReader wrapped = BinScoreUtil.wrap(base);

            try {
                IndexSearcher searcher = newSearcher(wrapped);
                searcher.setSimilarity(new BM25Similarity());

                AnytimeRankingSearcher rankingSearcher = new AnytimeRankingSearcher(searcher, 10, 100, "content");
                TopDocs topDocs = rankingSearcher.search(new TermQuery(new Term("content", "lucene")));

                assertNotNull(topDocs);
                assertTrue("Expected some results", topDocs.scoreDocs.length > 0);

                int bin0Hits = 0;
                int total = 0;
                for (ScoreDoc sd : topDocs.scoreDocs) {
                    int docID = sd.doc;
                    for (LeafReaderContext ctx : wrapped.leaves()) {
                        if (docID >= ctx.docBase && docID < ctx.docBase + ctx.reader().maxDoc()) {
                            int segDoc = docID - ctx.docBase;
                            BinScoreReader binReader = BinScoreUtil.getBinScoreReader(ctx.reader());
                            if (binReader != null) {
                                int bin = binReader.getBinForDoc(segDoc);
                                if (bin == 0) {
                                    bin0Hits++;
                                }
                                total++;
                            }
                            break;
                        }
                    }
                }

                assertTrue("Bin 0 should dominate in sparse distribution, got " + bin0Hits + " of " + total,
                        total > 0 && bin0Hits >= (total / 2));
            } finally {
                IOUtils.close(wrapped, base);
            }
        }
    }
}
