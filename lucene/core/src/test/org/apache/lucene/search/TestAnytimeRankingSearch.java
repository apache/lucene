/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lucene.search;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene103.Lucene103Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.BinScoreReader;
import org.apache.lucene.index.BinScoreUtil;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.NamedThreadFactory;
import org.junit.Test;

public class TestAnytimeRankingSearch extends LuceneTestCase {

  private FieldType newBinningFieldType() {
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
    return ft;
  }

  private Document newDoc(int docID, String content, FieldType ft) {
    Document doc = new Document();
    doc.add(new IntPoint("docID", docID));
    doc.add(new StoredField("docID", docID));
    doc.add(new Field("content", content, ft));
    return doc;
  }

  @Test
  public void testSlaCutoffTriggersEarlyTermination() throws Exception {
    Codec.setDefault(new Lucene103Codec());

    Path indexPath = Files.createTempDirectory("testindex_sla_cutoff");
    try (Directory dir = FSDirectory.open(indexPath);
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(new StandardAnalyzer()))) {

      FieldType ft = newBinningFieldType();
      for (int i = 0; i < 500; i++) {
        writer.addDocument(newDoc(i, "lucene boost fast search scoring ranking", ft));
      }
      writer.commit();

      IndexReader base = DirectoryReader.open(dir);
      IndexReader wrapped = BinScoreUtil.wrap(base);
      try {
        IndexSearcher searcher = newSearcher(wrapped);
        searcher.setSimilarity(new BM25Similarity());
        try (AnytimeRankingSearcher s = new AnytimeRankingSearcher(base, 10, 1, "content")) {

          TopDocs docs = s.search(new TermQuery(new Term("content", "lucene")));
          assertNotNull(docs);
          assertTrue("Expect partial results under tight SLA", docs.scoreDocs.length > 0);
        }
      } finally {
        IOUtils.close(wrapped, base);
      }
    }
  }

  @Test
  public void testMultiThreadedQueries() throws Exception {
    Path path = Files.createTempDirectory("multiThreadedQuery");
    try (Directory dir = FSDirectory.open(path);
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(new StandardAnalyzer()))) {

      FieldType ft = newBinningFieldType();
      for (int i = 0; i < 1000; i++) {
        String content =
            "lucene relevance scoring performance content " + TestUtil.randomSimpleString(random());
        writer.addDocument(newDoc(i, content, ft));
      }
      writer.commit();

      IndexReader base = DirectoryReader.open(dir);
      IndexReader wrapped = BinScoreUtil.wrap(base);
      try {
        IndexSearcher searcher = newSearcher(wrapped);
        searcher.setSimilarity(new BM25Similarity(2.0f, 0.2f));
        try (AnytimeRankingSearcher anytimeSearcher =
            new AnytimeRankingSearcher(base, 10, 100, "content")) {

          ExecutorService exec =
              Executors.newFixedThreadPool(4, new NamedThreadFactory("test-search"));
          List<Future<TopDocs>> futures = new ArrayList<>();

          Query query = new TermQuery(new Term("content", "lucene"));
          for (int i = 0; i < 4; i++) {
            futures.add(exec.submit(() -> anytimeSearcher.search(query)));
          }

          for (Future<TopDocs> f : futures) {
            TopDocs d = f.get();
            assertNotNull(d);
            assertTrue(d.scoreDocs.length > 0);
          }
          exec.shutdown();
        }
      } finally {
        IOUtils.close(base);
      }
    }
  }
}
