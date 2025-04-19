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
 * Approximates a sparse document similarity graph based on token co-occurrence patterns.
 *
 * <p>This builder avoids full term vector reconstruction and uses BitSets and edge pruning
 * heuristics to efficiently compute sparse edges for each document. Tokens are internally remapped
 * to compact integer IDs. Terms with very high document frequency are skipped to avoid noisy or
 * global tokens.
 */
public final class ApproximateDocGraphBuilder {

  /** Default number of edges retained per document in the similarity graph. */
  public static final int DEFAULT_MAX_EDGES = 10;

  /** Minimum shared token overlap required to retain an edge. */
  private static final int MIN_TOKEN_OVERLAP = 2;

  private final String field;
  private final int maxEdgesPerDoc;
  private final boolean useParallel;
  private final float maxDocFreqRatio;

  /**
   * Constructs a builder using the default document frequency pruning threshold.
   *
   * @param field indexed field name
   * @param maxEdgesPerDoc maximum number of edges per document
   */
  public ApproximateDocGraphBuilder(String field, int maxEdgesPerDoc) {
    this(field, maxEdgesPerDoc, false, 1.0f /* Disable pruning high frequency terms */);
  }

  /**
   * Constructs a builder with parallelism and frequency filtering options.
   *
   * @param field indexed field name
   * @param maxEdgesPerDoc maximum number of edges per document
   * @param useParallel whether to parallelize graph construction
   * @param maxDocFreqRatio max allowed DF as a fraction of total docs
   */
  public ApproximateDocGraphBuilder(
      String field, int maxEdgesPerDoc, boolean useParallel, float maxDocFreqRatio) {
    this.field = field;
    this.maxEdgesPerDoc = maxEdgesPerDoc;
    this.useParallel = useParallel;
    this.maxDocFreqRatio = maxDocFreqRatio;
  }

  /**
   * Builds a sparse document similarity graph using token co-occurrence statistics. Each document
   * is connected to up to {@code maxEdgesPerDoc} other documents that share at least {@code
   * MIN_TOKEN_OVERLAP} tokens. Edge weights are computed as: {@code overlap / max(tokensA.size(),
   * tokensB.size())}.
   *
   * @param reader a leaf reader for a single segment
   * @return a sparse similarity graph over the segment
   * @throws IOException on low-level I/O error
   */
  @SuppressWarnings("unchecked")
  public SparseEdgeGraph build(LeafReader reader) throws IOException {
    final int maxDoc = reader.maxDoc();
    final InMemorySparseEdgeGraph graph = new InMemorySparseEdgeGraph();

    final Set<Integer>[] docTokens = (Set<Integer>[]) new Set<?>[maxDoc];
    final Map<Integer, BitSet> tokenToDocs = new HashMap<>();
    final Map<BytesRef, Integer> tokenIds = new HashMap<>();
    final AtomicInteger nextTokenId = new AtomicInteger();

    Terms terms = reader.terms(field);
    if (terms == null) {
      return graph;
    }

    TermsEnum termsEnum = terms.iterator();
    BytesRef term;
    while ((term = termsEnum.next()) != null) {
      final int docFreq = termsEnum.docFreq();
      if (maxDocFreqRatio < 1.0f && docFreq > maxDoc * maxDocFreqRatio) {
        continue;
      }

      PostingsEnum postings = termsEnum.postings(null, PostingsEnum.NONE);
      if (postings == null) {
        continue;
      }

      final int tokenId =
          tokenIds.computeIfAbsent(BytesRef.deepCopyOf(term), _ -> nextTokenId.getAndIncrement());
      final BitSet seen = new BitSet(maxDoc);

      while (postings.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
        final int docID = postings.docID();
        seen.set(docID);
        Set<Integer> tokens = docTokens[docID];
        if (tokens == null) {
          tokens = new HashSet<>();
          docTokens[docID] = tokens;
        }
        tokens.add(tokenId);
      }

      tokenToDocs.put(tokenId, seen);
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

          final BitSet candidates = new BitSet(maxDoc);
          for (int tokenId : tokensA) {
            BitSet seen = tokenToDocs.get(tokenId);
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
              final float weight = (float) overlap / Math.max(tokensA.size(), tokensB.size());
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
