/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;

/**
 * Approximates a sparse similarity graph from token co-occurrence using compact token ID
 * representations and edge pruning heuristics.
 *
 * <p>This builder avoids full term vector reconstruction and uses BitSets to identify candidate
 * overlaps efficiently. Token frequency pruning and early edge rejection are used to scale to large
 * corpora.
 */
public final class ApproximateDocGraphBuilder {

  /** Default number of edges retained per document in the similarity graph. */
  public static final int DEFAULT_MAX_EDGES = 10;

  /** Minimum shared token overlap required to retain an edge. */
  private static final int MIN_TOKEN_OVERLAP = 2;

  /** Skip tokens with DF ratio above this threshold (i.e., stopword-like). */
  private static final float MAX_DOC_FREQ_RATIO = 0.05f;

  private final String field;
  private final int maxEdgesPerDoc;
  private final boolean useParallel;

  /**
   * Creates a new graph builder for the specified field.
   *
   * @param field the indexed field to use for term overlap computation
   * @param maxEdgesPerDoc maximum number of outgoing edges to retain per document
   */
  public ApproximateDocGraphBuilder(String field, int maxEdgesPerDoc) {
    this(field, maxEdgesPerDoc, false);
  }

  /**
   * Creates a new graph builder with optional parallel edge generation.
   *
   * @param field the indexed field
   * @param maxEdgesPerDoc maximum edges per doc
   * @param useParallel whether to parallelize edge construction
   */
  public ApproximateDocGraphBuilder(String field, int maxEdgesPerDoc, boolean useParallel) {
    this.field = field;
    this.maxEdgesPerDoc = maxEdgesPerDoc;
    this.useParallel = useParallel;
  }

  /**
   * Constructs a sparse document similarity graph from token co-occurrence.
   *
   * <p>Documents that share tokens are considered candidates for similarity, and edges are added
   * based on normalized token overlap.
   *
   * @param reader a leaf reader over a single Lucene segment
   * @return a sparse document similarity graph based on token overlap
   * @throws IOException if an I/O error occurs
   */
  @SuppressWarnings("unchecked")
  public SparseEdgeGraph build(LeafReader reader) throws IOException {
    final int maxDoc = reader.maxDoc();
    final InMemorySparseEdgeGraph graph = new InMemorySparseEdgeGraph();
    final Set<Integer>[] docTokens = (Set<Integer>[]) new Set<?>[maxDoc];
    final Map<Integer, BitSet> tokenBitsets = new HashMap<>();
    final Map<BytesRef, Integer> tokenIdMap = new HashMap<>();
    final AtomicInteger nextTokenId = new AtomicInteger();

    Terms terms = reader.terms(field);
    if (terms == null) {
      return graph;
    }

    TermsEnum termsEnum = terms.iterator();
    BytesRef term;
    while ((term = termsEnum.next()) != null) {
      int docFreq = termsEnum.docFreq();
      if (docFreq > maxDoc * MAX_DOC_FREQ_RATIO) {
        continue;
      }

      PostingsEnum postings = termsEnum.postings(null, PostingsEnum.NONE);
      if (postings == null) {
        continue;
      }

      int tokenId =
          tokenIdMap.computeIfAbsent(BytesRef.deepCopyOf(term), _ -> nextTokenId.getAndIncrement());
      BitSet seen = new BitSet(maxDoc);

      while (postings.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
        int docID = postings.docID();
        seen.set(docID);
        Set<Integer> tokens = docTokens[docID];
        if (tokens == null) {
          tokens = new HashSet<>();
          docTokens[docID] = tokens;
        }
        tokens.add(tokenId);
      }

      tokenBitsets.put(tokenId, seen);
    }

    IntStream docStream = IntStream.range(0, maxDoc);
    if (useParallel) {
      docStream = docStream.parallel();
    }

    docStream.forEach(
        docA -> {
          Set<Integer> tokensA = docTokens[docA];
          if (tokensA == null || tokensA.isEmpty()) {
            return;
          }

          BitSet candidates = new BitSet(maxDoc);
          for (int tokenId : tokensA) {
            BitSet seen = tokenBitsets.get(tokenId);
            if (seen != null && candidates.cardinality() <= maxEdgesPerDoc * 2) {
              candidates.or(seen);
            }
          }

          if (candidates.cardinality() > maxEdgesPerDoc * 4) {
            return;
          }

          int added = 0;
          for (int docB = candidates.nextSetBit(0);
              docB >= 0 && added < maxEdgesPerDoc;
              docB = candidates.nextSetBit(docB + 1)) {

            if (docA == docB) {
              continue;
            }

            Set<Integer> tokensB = docTokens[docB];
            if (tokensB == null || tokensB.isEmpty()) {
              continue;
            }

            int overlap = 0;
            for (int token : tokensA) {
              if (tokensB.contains(token)) {
                overlap++;
              }
            }

            if (overlap >= MIN_TOKEN_OVERLAP) {
              float weight = (float) overlap / Math.max(tokensA.size(), tokensB.size());
              synchronized (graph) {
                graph.addEdge(docA, docB, weight);
              }
              added++;
            }
          }
        });

    for (int docID = 0; docID < maxDoc; docID++) {
      graph.ensureVertex(docID);
    }

    return graph;
  }
}
