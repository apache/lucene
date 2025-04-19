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

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestApproximateDocBinner extends LuceneTestCase {

  public void testBinAssignmentPowerOfTwoConstraint() throws IOException {
    InMemorySparseEdgeGraph graph = new InMemorySparseEdgeGraph();
    graph.addEdge(0, 1, 0.5f);
    graph.ensureVertex(0);
    graph.ensureVertex(1);

    expectThrows(
        IllegalArgumentException.class,
        () -> {
          ApproximateDocBinner.assign(graph, 2, 3); // not power of 2
        });
  }

  public void testAllDocsAssignedToSomeBin() throws IOException {
    InMemorySparseEdgeGraph graph = new InMemorySparseEdgeGraph();
    int maxDoc = 8;
    for (int i = 0; i < maxDoc - 1; i++) {
      graph.addEdge(i, i + 1, 0.75f);
    }
    for (int i = 0; i < maxDoc; i++) {
      graph.ensureVertex(i);
    }

    int[] bins = ApproximateDocBinner.assign(graph, maxDoc, 4);
    assertEquals("Must assign bins to all docs", maxDoc, bins.length);
    for (int i = 0; i < bins.length; i++) {
      assertTrue("Bin must be in range", bins[i] >= 0 && bins[i] < 4);
    }
  }

  public void testBinDiversity() throws IOException {
    InMemorySparseEdgeGraph graph = new InMemorySparseEdgeGraph();
    int maxDoc = 16;
    for (int i = 0; i < maxDoc; i++) {
      graph.ensureVertex(i);
    }

    // Create more varied connections with different weights
    for (int i = 0; i < maxDoc; i++) {
      for (int j = i + 1; j < maxDoc; j++) {
        if ((i ^ j) % 3 == 0) { // semi-random condition to vary structure
          float weight = (i + j) % 5 + 1.0f;
          graph.addEdge(i, j, weight);
        }
      }
    }

    int numBins = 8;
    int[] bins = ApproximateDocBinner.assign(graph, maxDoc, numBins);

    Set<Integer> uniqueBins = new HashSet<>();
    for (int bin : bins) {
      uniqueBins.add(bin);
    }

    assertTrue("Expected multiple unique bins", uniqueBins.size() > 1);
    assertTrue("Bin count should not exceed configured", uniqueBins.size() <= numBins);
  }

  public void testStableHashingProducesConsistentBins() throws IOException {
    InMemorySparseEdgeGraph graph = new InMemorySparseEdgeGraph();
    int maxDoc = 4;
    for (int i = 0; i < maxDoc; i++) {
      graph.ensureVertex(i);
    }
    graph.addEdge(0, 1, 0.9f);
    graph.addEdge(1, 2, 0.9f);
    graph.addEdge(2, 3, 0.9f);
    graph.addEdge(3, 0, 0.9f);

    int[] bins1 = ApproximateDocBinner.assign(graph, maxDoc, 2);
    int[] bins2 = ApproximateDocBinner.assign(graph, maxDoc, 2);

    for (int i = 0; i < maxDoc; i++) {
      assertEquals("Hash-based binning should be stable", bins1[i], bins2[i]);
    }
  }
}
