/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.lucene.codecs;

/**
 * Interface for a sparse directed graph over documents.
 *
 * <p>This is used to represent approximate similarity relationships between documents. Each
 * document is identified by its Lucene docID, and edges have associated float weights.
 */
public interface SparseEdgeGraph {

  /**
   * Adds a directed edge from {@code fromDocID} to {@code toDocID} with the given weight.
   *
   * <p>Implementations may choose to add symmetric or asymmetric edges depending on usage.
   *
   * @param fromDocID source document ID
   * @param toDocID destination document ID
   * @param weight edge weight, typically in [0,1]
   */
  void addEdge(int fromDocID, int toDocID, float weight);

  /**
   * Returns all neighbors of the given document.
   *
   * <p>If the document has no edges, an empty array is returned.
   *
   * @param docID document ID to query
   * @return array of neighbor docIDs
   */
  int[] getNeighbors(int docID);

  /**
   * Returns weights for the edges associated with the given document.
   *
   * <p>The result corresponds one-to-one with {@link #getNeighbors(int)}.
   *
   * @param docID document ID to query
   * @return array of weights
   */
  float[] getWeights(int docID);

  /**
   * Ensures the graph can store edges for the given document ID.
   *
   * <p>This is useful for preallocating or marking isolated nodes.
   *
   * @param docID the document ID
   */
  void ensureVertex(int docID);

  /**
   * Returns the number of documents represented in this graph.
   *
   * <p>This includes isolated documents (those with no outgoing edges).
   */
  int size();
}
