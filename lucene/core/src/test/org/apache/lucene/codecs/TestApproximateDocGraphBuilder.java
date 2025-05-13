/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.lucene.codecs;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestApproximateDocGraphBuilder extends LuceneTestCase {

  public void testGraphConnectivity() throws Exception {
    Path path = createTempDir("approx-doc-graph");
    Directory dir = FSDirectory.open(path);

    FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
    ft.setTokenized(true);
    ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
    ft.freeze();

    try (IndexWriter writer =
        new IndexWriter(dir, new IndexWriterConfig(new MockAnalyzer(random())))) {
      writer.addDocument(singleFieldDoc("alpha beta gamma", ft)); // doc 0
      writer.addDocument(singleFieldDoc("beta gamma delta", ft)); // doc 1
      writer.addDocument(singleFieldDoc("zeta eta theta", ft)); // doc 2
      writer.addDocument(singleFieldDoc("alpha delta epsilon", ft)); // doc 3
      writer.addDocument(singleFieldDoc("rho sigma phi", ft)); // doc 4
      writer.commit();
    }

    try (DirectoryReader reader = DirectoryReader.open(dir)) {
      SparseEdgeGraph graph =
          new ApproximateDocGraphBuilder("field", 5 /* maxEdges */, false, 0.5f)
              .build(reader.leaves().get(0).reader());

      assertEquals("All docs should be present in graph", 5, graph.size());

      for (int doc = 0; doc < 5; doc++) {
        int[] neighbors = graph.getNeighbors(doc);
        float[] weights = graph.getWeights(doc);

        assertEquals(
            "Mismatch in neighbors and weights array length", neighbors.length, weights.length);

        for (int i = 0; i < neighbors.length; i++) {
          assertNotEquals("Self-loop detected at doc " + doc, doc, neighbors[i]);
          assertTrue("Edge weight must be > 0", weights[i] > 0f);
        }
      }

      Set<Integer> connectedTo0 =
          Arrays.stream(graph.getNeighbors(0)).boxed().collect(Collectors.toSet());

      assertTrue(
          "Doc 0 should connect to either doc 1 or doc 3 based on shared terms",
          connectedTo0.contains(1) || connectedTo0.contains(3));
    }

    dir.close();
  }

  public void testDisconnectedDocsStillPresent() throws Exception {
    Path path = createTempDir("approx-doc-graph-disconnected");
    Directory dir = FSDirectory.open(path);

    FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
    ft.setTokenized(true);
    ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
    ft.freeze();

    try (IndexWriter writer =
        new IndexWriter(dir, new IndexWriterConfig(new MockAnalyzer(random())))) {
      writer.addDocument(singleFieldDoc("foo bar", ft));
      writer.addDocument(singleFieldDoc("baz qux", ft));
      writer.addDocument(singleFieldDoc("alpha beta", ft));
      writer.addDocument(singleFieldDoc("delta epsilon", ft));
      writer.addDocument(singleFieldDoc("isolatedterms onlyhere", ft));
      writer.commit();
    }

    try (DirectoryReader reader = DirectoryReader.open(dir)) {
      ApproximateDocGraphBuilder builder = new ApproximateDocGraphBuilder("field", 2);
      SparseEdgeGraph graph = builder.build(reader.leaves().get(0).reader());

      assertEquals("All documents should appear in graph", 5, graph.size());
      assertNotNull("Doc 4 (isolated) should be represented", graph.getNeighbors(4));
    }

    dir.close();
  }

  private static Document singleFieldDoc(String content, FieldType type) {
    Document doc = new Document();
    doc.add(new Field("field", content, type));
    return doc;
  }
}
