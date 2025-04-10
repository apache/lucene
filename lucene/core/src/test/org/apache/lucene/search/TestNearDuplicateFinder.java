/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package org.apache.lucene.search;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.store.MockDirectoryWrapper;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestNearDuplicateFinder extends LuceneTestCase {

  private FieldType buildTextType() {
    FieldType type = new FieldType(TextField.TYPE_STORED);
    type.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    type.setStoreTermVectors(true);
    type.setStoreTermVectorPositions(true);
    type.freeze();
    return type;
  }

  private Directory buildIndex(String... docs) throws IOException {
    Directory dir = new MockDirectoryWrapper(random(), new MMapDirectory(createTempDir("TestNearDuplicateFinder")));
    IndexWriterConfig config = new IndexWriterConfig(new MockAnalyzer(random()));
    try (IndexWriter writer = new IndexWriter(dir, config)) {
      FieldType fieldType = buildTextType();
      for (String content : docs) {
        Document doc = new Document();
        doc.add(new Field("body", content, fieldType));
        writer.addDocument(doc);
      }
    }
    return dir;
  }

  public void testExactDuplicates() throws IOException {
    Directory dir = buildIndex("alpha beta gamma", "alpha beta gamma", "delta epsilon");
    try (DirectoryReader reader = DirectoryReader.open(dir)) {
      NearDuplicateFinder finder = new NearDuplicateFinder("body", 0.95f);
      List<DuplicateCluster> clusters = finder.findNearDuplicates(reader);
      assertEquals(1, clusters.size());
      Set<Integer> dup = clusters.get(0).docIds();
      assertTrue(dup.contains(0));
      assertTrue(dup.contains(1));
    }
  }

  public void testPartialOverlap() throws IOException {
    Directory dir = buildIndex("quick brown fox", "quick brown dog", "lazy dog sleeps");
    try (DirectoryReader reader = DirectoryReader.open(dir)) {
      NearDuplicateFinder finder = new NearDuplicateFinder("body", 0.5f);
      List<DuplicateCluster> clusters = finder.findNearDuplicates(reader);
      assertEquals(1, clusters.size());
    }
  }

  public void testThresholdSensitivity() throws IOException {
    Directory dir = buildIndex("a b c d e", "a b c", "x y z");
    try (DirectoryReader reader = DirectoryReader.open(dir)) {
      NearDuplicateFinder finderHigh = new NearDuplicateFinder("body", 0.9f);
      NearDuplicateFinder finderLow = new NearDuplicateFinder("body", 0.3f);
      List<DuplicateCluster> high = finderHigh.findNearDuplicates(reader);
      List<DuplicateCluster> low = finderLow.findNearDuplicates(reader);
      assertEquals(0, high.size());
      assertTrue(low.size() >= 1);
    }
  }

  public void testEmptyDocs() throws IOException {
    Directory dir = buildIndex("", "", "filled text content");
    try (DirectoryReader reader = DirectoryReader.open(dir)) {
      NearDuplicateFinder finder = new NearDuplicateFinder("body", 0.9f);
      List<DuplicateCluster> clusters = finder.findNearDuplicates(reader);
      assertEquals(1, clusters.size());
    }
  }

  public void testMultipleClusters() throws IOException {
    Directory dir = buildIndex("apple orange", "apple orange", "car bus", "car bus", "train");
    try (DirectoryReader reader = DirectoryReader.open(dir)) {
      NearDuplicateFinder finder = new NearDuplicateFinder("body", 0.95f);
      List<DuplicateCluster> clusters = finder.findNearDuplicates(reader);
      assertEquals(2, clusters.size());
    }
  }

  public void testNoDuplicates() throws IOException {
    Directory dir = buildIndex("one", "two", "three", "four");
    try (DirectoryReader reader = DirectoryReader.open(dir)) {
      NearDuplicateFinder finder = new NearDuplicateFinder("body", 0.95f);
      List<DuplicateCluster> clusters = finder.findNearDuplicates(reader);
      assertEquals(0, clusters.size());
    }
  }
}