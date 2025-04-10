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
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;

/**
 * Approximates a sparse similarity graph from token co-occurrence without exact vector comparison.
 */
public final class ApproximateDocGraphBuilder {

  public static final int DEFAULT_MAX_EDGES = 10;

  private final String field;
  private final int maxEdgesPerDoc;

  public ApproximateDocGraphBuilder(String field, int maxEdgesPerDoc) {
    this.field = field;
    this.maxEdgesPerDoc = maxEdgesPerDoc;
  }

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

      final BitSet seen = new java.util.BitSet(maxDoc);
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

      final BitSet candidates = new java.util.BitSet(maxDoc);
      for (String token : tokensA) {
        BitSet seen = tokenBitsets.get(token);
        if (seen != null) {
          candidates.or(seen);
        }
      }

      int added = 0;
      for (int docB = candidates.nextSetBit(0); docB >= 0 && added < maxEdgesPerDoc; docB = candidates.nextSetBit(docB + 1)) {
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