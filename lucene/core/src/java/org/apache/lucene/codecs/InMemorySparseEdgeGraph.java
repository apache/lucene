/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.lucene.codecs;

import org.apache.lucene.util.ArrayUtil;

/**
 * In-memory implementation of {@link SparseEdgeGraph} for use in binning and similarity tasks.
 *
 * <p>This graph stores undirected edges between documents, where each edge is represented by a
 * symmetric pair of directed connections (A→B and B→A).
 *
 * <p>Internally, it maintains expandable arrays per docID for neighbors and weights. Thread safety
 * is ensured via lock striping.
 */
public final class InMemorySparseEdgeGraph implements SparseEdgeGraph {

  private static final int INITIAL_CAPACITY = 4;
  private static final int DEFAULT_INITIAL_SIZE = 1024;
  private static final int LOCK_STRIPES = 32;
  private static final int LOCK_MASK = LOCK_STRIPES - 1;

  private DocEdges[] edgeTable;
  private int maxDocHint = DEFAULT_INITIAL_SIZE;
  private final Object[] locks;

  /** Creates a new graph with default capacity. */
  public InMemorySparseEdgeGraph() {
    this.locks = new Object[LOCK_STRIPES];
    for (int i = 0; i < LOCK_STRIPES; i++) {
      locks[i] = new Object();
    }
  }

  /**
   * Creates a new graph with initial allocation hint.
   *
   * @param maxDocHint expected number of documents (used to size internal table)
   */
  public InMemorySparseEdgeGraph(int maxDocHint) {
    this.maxDocHint = Math.max(maxDocHint, DEFAULT_INITIAL_SIZE);
    this.locks = new Object[LOCK_STRIPES];
    for (int i = 0; i < LOCK_STRIPES; i++) {
      locks[i] = new Object();
    }
  }

  @Override
  public void addEdge(int docA, int docB, float weight) {
    if (docA == docB) {
      return; // Prevent self-loop
    }

    int maxID = Math.max(docA, docB);
    ensureCapacity(maxID);

    Object lockA = lockForDoc(docA);
    Object lockB = lockForDoc(docB);

    if (lockA == lockB) {
      synchronized (lockA) {
        addDirectedEdge(docA, docB, weight);
        addDirectedEdge(docB, docA, weight);
      }
    } else if (System.identityHashCode(lockA) < System.identityHashCode(lockB)) {
      synchronized (lockA) {
        synchronized (lockB) {
          addDirectedEdge(docA, docB, weight);
          addDirectedEdge(docB, docA, weight);
        }
      }
    } else {
      synchronized (lockB) {
        synchronized (lockA) {
          addDirectedEdge(docA, docB, weight);
          addDirectedEdge(docB, docA, weight);
        }
      }
    }
  }

  private void addDirectedEdge(int fromDoc, int toDoc, float weight) {
    DocEdges edges = edgeTable[fromDoc];
    if (edges == null || edges == DocEdges.EMPTY) {
      edges = new DocEdges(INITIAL_CAPACITY);
      edgeTable[fromDoc] = edges;
    }
    edges.add(toDoc, weight);
  }

  @Override
  public int[] getNeighbors(int docID) {
    if (edgeTable == null || docID >= edgeTable.length) {
      return new int[0];
    }
    DocEdges edges = edgeTable[docID];
    return (edges == null || edges.size == 0)
        ? new int[0]
        : ArrayUtil.copyOfSubArray(edges.neighbors, 0, edges.size);
  }

  @Override
  public float[] getWeights(int docID) {
    if (edgeTable == null || docID >= edgeTable.length) {
      return new float[0];
    }
    DocEdges edges = edgeTable[docID];
    return (edges == null || edges.size == 0)
        ? new float[0]
        : ArrayUtil.copyOfSubArray(edges.weights, 0, edges.size);
  }

  @Override
  public void ensureVertex(int docID) {
    ensureCapacity(docID);
    if (edgeTable[docID] == null) {
      Object lock = lockForDoc(docID);
      synchronized (lock) {
        if (edgeTable[docID] == null) {
          edgeTable[docID] = DocEdges.EMPTY;
        }
      }
    }
  }

  @Override
  public int size() {
    if (edgeTable == null) {
      return 0;
    }
    int count = 0;
    for (DocEdges edges : edgeTable) {
      if (edges != null) {
        count++;
      }
    }
    return count;
  }

  private void ensureCapacity(int docID) {
    if (edgeTable == null) {
      edgeTable = new DocEdges[Math.max(docID + 1, maxDocHint)];
    } else if (docID >= edgeTable.length) {
      synchronized (this) {
        if (docID >= edgeTable.length) {
          int newSize = Math.max(edgeTable.length << 1, docID + 1);
          edgeTable = ArrayUtil.grow(edgeTable, newSize);
        }
      }
    }
  }

  private Object lockForDoc(int docID) {
    return locks[docID & LOCK_MASK];
  }

  private static final class DocEdges {
    static final DocEdges EMPTY = new DocEdges(true);

    int[] neighbors;
    float[] weights;
    int size;

    /**
     * Internal structure for storing a document’s adjacency list. Stores neighbor docIDs and
     * corresponding edge weights.
     */
    DocEdges(int capacity) {
      neighbors = new int[Math.max(1, capacity)];
      weights = new float[Math.max(1, capacity)];
    }

    private DocEdges(boolean empty) {
      neighbors = new int[0];
      weights = new float[0];
      size = 0;
    }

    void add(int neighbor, float weight) {
      if (this == EMPTY) {
        throw new UnsupportedOperationException("Cannot add to EMPTY DocEdges");
      }
      if (size == neighbors.length) {
        neighbors = ArrayUtil.grow(neighbors, size + 1);
        weights = ArrayUtil.grow(weights, size + 1);
      }
      neighbors[size] = neighbor;
      weights[size] = weight;
      size++;
    }
  }
}
