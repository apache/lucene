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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.codecs;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestDocBinningGraphBuilder extends LuceneTestCase {

  public void testSimpleLinearGraph() throws IOException {
    final int maxDoc = 8;
    final int numBins = 4;

    SparseEdgeGraph graph = new InMemorySparseEdgeGraph();
    for (int i = 0; i < maxDoc - 1; i++) {
      graph.addEdge(i, i + 1, 1.0f);
    }

    int[] docToBin = DocBinningGraphBuilder.computeBins(graph, maxDoc, numBins);
    assertEquals(maxDoc, docToBin.length);

    Set<Integer> seenBins = new HashSet<>();
    for (int bin : docToBin) {
      assertTrue("Bin must be non-negative", bin >= 0);
      assertTrue("Bin must be less than numBins", bin < numBins);
      seenBins.add(bin);
    }

    assertTrue("Expected to use multiple bins", seenBins.size() > 1);
  }

  public void testSingleBinAssignment() throws IOException {
    final int maxDoc = 5;
    final int numBins = 1;

    SparseEdgeGraph graph = new InMemorySparseEdgeGraph();
    for (int i = 0; i < maxDoc - 1; i++) {
      graph.addEdge(i, i + 1, 1.0f);
    }

    int[] docToBin = DocBinningGraphBuilder.computeBins(graph, maxDoc, numBins);
    for (int bin : docToBin) {
      assertEquals(0, bin);
    }
  }

  public void testDisconnectedGraph() throws IOException {
    final int maxDoc = 6;
    final int numBins = 2;

    SparseEdgeGraph graph = new InMemorySparseEdgeGraph(); // no edges
    for (int i = 0; i < maxDoc; i++) {
      graph.ensureVertex(i);
    }

    int[] docToBin = DocBinningGraphBuilder.computeBins(graph, maxDoc, numBins);
    assertEquals(maxDoc, docToBin.length);
    for (int bin : docToBin) {
      assertTrue(bin >= 0 && bin < numBins);
    }
  }

  public void testMinimalDocsSplit() throws IOException {
    final int maxDoc = 2;
    final int numBins = 2;

    SparseEdgeGraph graph = new InMemorySparseEdgeGraph();
    graph.addEdge(0, 1, 1.0f);

    int[] docToBin = DocBinningGraphBuilder.computeBins(graph, maxDoc, numBins);
    assertEquals(2, docToBin.length);
    assertNotEquals(docToBin[0], docToBin[1]);
  }

  public void testInvalidBinCount() {
    SparseEdgeGraph graph = new InMemorySparseEdgeGraph();
    expectThrows(
        IllegalArgumentException.class, () -> DocBinningGraphBuilder.computeBins(graph, 10, 3));
  }

  public void testZeroDocsFails() {
    SparseEdgeGraph graph = new InMemorySparseEdgeGraph();
    expectThrows(
        IllegalArgumentException.class, () -> DocBinningGraphBuilder.computeBins(graph, 0, 2));
  }
}
