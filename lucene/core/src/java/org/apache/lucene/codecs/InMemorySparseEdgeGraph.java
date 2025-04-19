/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.lucene.codecs;

import java.util.Arrays;

/**
 * Optimized in-memory implementation of {@link SparseEdgeGraph}. All edges are symmetric.
 * Internally uses expandable per-doc arrays.
 */
public final class InMemorySparseEdgeGraph implements SparseEdgeGraph {

  private static final int INITIAL_CAPACITY = 4;
  private static final int DEFAULT_INITIAL_SIZE = 1024;
  private static final int LOCK_STRIPES = 32;
  private static final int LOCK_MASK = LOCK_STRIPES - 1;

  private DocEdges[] edgeTable;
  private int maxDocHint = DEFAULT_INITIAL_SIZE;
  private final Object[] locks;

  /** Default constructor. Table grows as needed. */
  public InMemorySparseEdgeGraph() {
    this.locks = new Object[LOCK_STRIPES];
    for (int i = 0; i < LOCK_STRIPES; i++) {
      locks[i] = new Object();
    }
  }

  /** Constructor that optionally preallocates space for known document count. */
  public InMemorySparseEdgeGraph(int maxDocHint) {
    this.maxDocHint = Math.max(maxDocHint, DEFAULT_INITIAL_SIZE);
    this.locks = new Object[LOCK_STRIPES];
    for (int i = 0; i < LOCK_STRIPES; i++) {
      locks[i] = new Object();
    }
  }

  @Override
  public void addEdge(int docA, int docB, float weight) {
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
        : Arrays.copyOf(edges.neighbors, edges.size);
  }

  @Override
  public float[] getWeights(int docID) {
    if (edgeTable == null || docID >= edgeTable.length) {
      return new float[0];
    }
    DocEdges edges = edgeTable[docID];
    return (edges == null || edges.size == 0)
        ? new float[0]
        : Arrays.copyOf(edges.weights, edges.size);
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
          edgeTable = Arrays.copyOf(edgeTable, newSize);
        }
      }
    }
  }

  private Object lockForDoc(int docID) {
    return locks[docID & LOCK_MASK];
  }

  /** Per-document adjacency structure. */
  private static final class DocEdges {
    static final DocEdges EMPTY = new DocEdges(true);

    int[] neighbors;
    float[] weights;
    int size;

    DocEdges(int capacity) {
      int cap = Math.max(1, capacity);
      this.neighbors = new int[cap];
      this.weights = new float[cap];
    }

    private DocEdges(boolean empty) {
      this.neighbors = new int[0];
      this.weights = new float[0];
      this.size = 0;
    }

    void add(int neighbor, float weight) {
      if (this == EMPTY) {
        throw new UnsupportedOperationException("Cannot add to EMPTY DocEdges");
      }
      if (size == neighbors.length) {
        int newCap = size << 1;
        neighbors = Arrays.copyOf(neighbors, newCap);
        weights = Arrays.copyOf(weights, newCap);
      }
      neighbors[size] = neighbor;
      weights[size] = weight;
      size++;
    }
  }
}
