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

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Approximates a sparse similarity graph from token co-occurrence without exact vector comparison.
 */
public final class ApproximateDocGraphBuilder {

  /** Default number of edges retained per document in the similarity graph. */
  public static final int DEFAULT_MAX_EDGES = 10;

  private final String field;
  private final int maxEdgesPerDoc;

  /**
   * Creates a new graph builder for the specified field.
   *
   * @param field the indexed field to use for term overlap computation
   * @param maxEdgesPerDoc maximum number of outgoing edges to retain per document
   */
  public ApproximateDocGraphBuilder(String field, int maxEdgesPerDoc) {
    this.field = field;
    this.maxEdgesPerDoc = maxEdgesPerDoc;
  }

  /**
   * Constructs the similarity graph for documents in the provided reader using term co-occurrence.
   *
   * <p>Documents that share terms in the specified field are connected with an edge, with edge
   * weights based on normalized overlap count. The number of outgoing edges per document is limited
   * by {@code maxEdgesPerDoc}.
   *
   * @param reader a leaf reader over a single Lucene segment
   * @return a sparse document similarity graph based on token overlap
   * @throws IOException if an I/O error occurs during term enumeration or postings access
   */
  public SparseEdgeGraph build(LeafReader reader) throws IOException {
    final int maxDoc = reader.maxDoc();
    final InMemorySparseEdgeGraph graph = new InMemorySparseEdgeGraph();
    @SuppressWarnings("unchecked")
    final Set<String>[] docTokens = (Set<String>[]) new Set<?>[maxDoc];
    final Map<String, BitSet> tokenBitsets = new HashMap<>();

    Terms terms = reader.terms(field);
    if (terms == null) {
      return graph;
    }

    TermsEnum termsEnum = terms.iterator();
    BytesRef term;
    while ((term = termsEnum.next()) != null) {
      PostingsEnum postings = termsEnum.postings(null, PostingsEnum.NONE);
      if (postings == null) {
        continue;
      }

      final BitSet seen = new BitSet(maxDoc);
      while (postings.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
        int docID = postings.docID();
        seen.set(docID);
        Set<String> tokens = docTokens[docID];
        if (tokens == null) {
          tokens = new HashSet<>();
          docTokens[docID] = tokens;
        }
        tokens.add(term.utf8ToString());
      }
      tokenBitsets.put(term.utf8ToString(), seen);
    }

    for (int docA = 0; docA < maxDoc; docA++) {
      Set<String> tokensA = docTokens[docA];
      if (tokensA == null || tokensA.isEmpty()) {
        continue;
      }

      final BitSet candidates = new BitSet(maxDoc);
      for (String token : tokensA) {
        BitSet seen = tokenBitsets.get(token);
        if (seen != null) {
          candidates.or(seen);
        }
      }

      int added = 0;
      for (int docB = candidates.nextSetBit(0);
          docB >= 0 && added < maxEdgesPerDoc;
          docB = candidates.nextSetBit(docB + 1)) {
        if (docA == docB) {
          continue;
        }

        Set<String> tokensB = docTokens[docB];
        if (tokensB == null || tokensB.isEmpty()) {
          continue;
        }

        int overlap = 0;
        for (String token : tokensA) {
          if (tokensB.contains(token)) {
            overlap++;
          }
        }

        if (overlap > 0) {
          float weight = (float) overlap / Math.max(tokensA.size(), tokensB.size());
          graph.addEdge(docA, docB, weight);
          added++;
        }
      }
    }

    return graph;
  }
}
