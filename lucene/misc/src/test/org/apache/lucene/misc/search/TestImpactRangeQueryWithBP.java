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
package org.apache.lucene.misc.search;

import java.io.IOException;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SlowCodecReaderWrapper;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.misc.index.BPIndexReorderer;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestImpactRangeQueryWithBP extends LuceneTestCase {

  public void testImpactRangeWithBPReordering() throws IOException {
    Directory dir = newDirectory();

    // Create initial index with documents
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());

    // Add documents with varying content to simulate different document clusters
    // Documents about "lucene search" (high relevance)
    for (int i = 0; i < 100; i++) {
      Document doc = new Document();
      doc.add(new Field("id", String.valueOf(i), TextField.TYPE_STORED));
      doc.add(new Field("content", "lucene search engine optimization", TextField.TYPE_NOT_STORED));
      doc.add(new Field("category", "search", TextField.TYPE_STORED));
      writer.addDocument(doc);
    }

    // Documents about "database search" (medium relevance)
    for (int i = 100; i < 200; i++) {
      Document doc = new Document();
      doc.add(new Field("id", String.valueOf(i), TextField.TYPE_STORED));
      doc.add(new Field("content", "database search query", TextField.TYPE_NOT_STORED));
      doc.add(new Field("category", "database", TextField.TYPE_STORED));
      writer.addDocument(doc);
    }

    // Documents with just "search" (lower relevance)
    for (int i = 200; i < 300; i++) {
      Document doc = new Document();
      doc.add(new Field("id", String.valueOf(i), TextField.TYPE_STORED));
      doc.add(new Field("content", "search", TextField.TYPE_NOT_STORED));
      doc.add(new Field("category", "basic", TextField.TYPE_STORED));
      writer.addDocument(doc);
    }

    // Documents without search term (no relevance)
    for (int i = 300; i < 400; i++) {
      Document doc = new Document();
      doc.add(new Field("id", String.valueOf(i), TextField.TYPE_STORED));
      doc.add(new Field("content", "other content without the term", TextField.TYPE_NOT_STORED));
      doc.add(new Field("category", "other", TextField.TYPE_STORED));
      writer.addDocument(doc);
    }

    writer.commit();

    writer.close();

    // Apply BP reordering to cluster similar documents
    DirectoryReader reader = DirectoryReader.open(dir);
    LeafReader leafReader = reader.leaves().get(0).reader();
    CodecReader codecReader = SlowCodecReaderWrapper.wrap(leafReader);

    BPIndexReorderer reorderer = new BPIndexReorderer();
    reorderer.setMinPartitionSize(32); // Use default BP partition size
    reorderer.setMaxIters(20); // Use default iterations
    reorderer.setMinDocFreq(2); // Minimum doc frequency for terms to be considered

    // Reorder the index using BP algorithm
    Directory tempDir = newDirectory();
    CodecReader reordered = reorderer.reorder(codecReader, tempDir, null);

    // Write the reordered documents back to the index
    IndexWriter reorderedWriter = new IndexWriter(dir, new IndexWriterConfig());
    reorderedWriter.deleteAll();
    reorderedWriter.addIndexes(reordered);
    reorderedWriter.close();

    reader.close();
    tempDir.close();

    // Search with ImpactRangeQuery on BP-reordered index
    reader = DirectoryReader.open(dir);
    IndexSearcher searcher = new IndexSearcher(reader);

    Query baseQuery = new TermQuery(new org.apache.lucene.index.Term("content", "search"));

    // Test with range size aligned to BP partition size
    Query impactQuery32 = new ImpactRangeQuery(baseQuery, 32);
    TopDocs results32 = searcher.search(impactQuery32, 10);

    // Test with larger range size (multiple partitions)
    Query impactQuery100 = new ImpactRangeQuery(baseQuery, 100);
    TopDocs results100 = searcher.search(impactQuery100, 10);

    // Verify results are consistent
    assertEquals("Should find all matching documents", 300, results32.totalHits.value());
    assertEquals("Should find all matching documents", 300, results100.totalHits.value());

    // The top results should be from the high-relevance group
    // After BP reordering, similar documents are clustered together
    System.out.println("Top 5 results with range size 32:");
    for (int i = 0; i < Math.min(5, results32.scoreDocs.length); i++) {
      StoredFields storedFields = searcher.storedFields();
      Document doc = storedFields.document(results32.scoreDocs[i].doc);
      System.out.println(
          "  Doc "
              + doc.get("id")
              + " (category: "
              + doc.get("category")
              + ") score="
              + results32.scoreDocs[i].score);
    }

    reader.close();
    dir.close();
  }

  public void testRangePrioritiesWithBP() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());

    // Create distinct document clusters that BP will group together
    // Cluster 1: Technical documents with high term frequency
    for (int i = 0; i < 50; i++) {
      Document doc = new Document();
      doc.add(
          new Field(
              "content",
              "search algorithm search implementation search optimization",
              TextField.TYPE_NOT_STORED));
      writer.addDocument(doc);
    }

    // Cluster 2: Simple documents with low term frequency
    for (int i = 0; i < 50; i++) {
      Document doc = new Document();
      doc.add(new Field("content", "basic search functionality", TextField.TYPE_NOT_STORED));
      writer.addDocument(doc);
    }

    // Cluster 3: Documents without search term
    for (int i = 0; i < 50; i++) {
      Document doc = new Document();
      doc.add(new Field("content", "unrelated content", TextField.TYPE_NOT_STORED));
      writer.addDocument(doc);
    }

    writer.commit();

    writer.close();

    // Apply BP reordering
    DirectoryReader reader = DirectoryReader.open(dir);
    LeafReader leafReader = reader.leaves().get(0).reader();
    CodecReader codecReader = SlowCodecReaderWrapper.wrap(leafReader);

    BPIndexReorderer reorderer = new BPIndexReorderer();
    reorderer.setMinPartitionSize(25); // Smaller partitions for this test
    reorderer.setMinDocFreq(1); // Low threshold for this small test

    Directory tempDir = newDirectory();
    CodecReader reordered = reorderer.reorder(codecReader, tempDir, null);

    // Write the reordered documents back
    IndexWriter reorderedWriter = new IndexWriter(dir, new IndexWriterConfig());
    reorderedWriter.deleteAll();
    reorderedWriter.addIndexes(reordered);
    reorderedWriter.close();

    reader.close();
    tempDir.close();

    // Search and verify range analysis is working
    reader = DirectoryReader.open(dir);
    IndexSearcher searcher = new IndexSearcher(reader);

    Query baseQuery = new TermQuery(new org.apache.lucene.index.Term("content", "search"));
    Query impactQuery = new ImpactRangeQuery(baseQuery, 25);

    TopDocs results = searcher.search(impactQuery, 10);
    assertEquals("Should find documents from first two clusters", 100, results.totalHits.value());

    // Top results should have positive scores
    assertTrue("First result should have positive score", results.scoreDocs[0].score > 0.0f);

    // Optional: Print scores to understand the scoring
    System.out.println("Top scores after BP reordering:");
    for (int i = 0; i < Math.min(5, results.scoreDocs.length); i++) {
      System.out.println(
          "  Doc " + results.scoreDocs[i].doc + " score=" + results.scoreDocs[i].score);
    }

    reader.close();
    dir.close();
  }
}
