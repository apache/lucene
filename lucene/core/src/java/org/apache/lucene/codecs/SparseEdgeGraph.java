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

/**
 * Interface for a sparse directed graph of documents. Used to represent approximate similarity
 * relationships between documents.
 */
public interface SparseEdgeGraph {
  /**
   * Adds a directed edge from {@code fromDocID} to {@code toDocID} with the given weight.
   *
   * @param fromDocID source document ID
   * @param toDocID destination document ID
   * @param weight edge weight representing similarity
   */
  void addEdge(int fromDocID, int toDocID, float weight);

  /**
   * Returns the list of neighboring document IDs for a given source document.
   *
   * @param docID source document ID
   * @return array of neighboring document IDs
   */
  int[] getNeighbors(int docID);

  /**
   * Returns the edge weights associated with the neighbors of the given document.
   *
   * @param docID source document ID
   * @return array of weights corresponding to {@link #getNeighbors(int)}
   */
  float[] getWeights(int docID);

  /** Returns the number of vertices in the graph. */
  int size();

  /**
   * Ensures internal data structures can accommodate the given document ID.
   *
   * @param docID document ID to reserve space for
   */
  void ensureVertex(int docID);
}
