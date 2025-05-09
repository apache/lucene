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
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;

/**
 * Builds a sparse similarity graph over documents using sampled token co-occurrence statistics.
 *
 * <p>Tokens are mapped to compact integer IDs. For each document, candidate neighbors are retrieved
 * by sampling a subset of its tokens, then selecting neighbors based on token overlap threshold.
 */
public final class ApproximateDocGraphBuilder {

  /** Maximum edges allowed */
  public static final int DEFAULT_MAX_EDGES = 10;

  /* Minimum token overlap to be considered for an edge */
  private static final int MIN_TOKEN_OVERLAP = 2;

  /** Maximum samples to be used for token set creation */
  private static final int MAX_SAMPLE_TOKENS = 12;

  /** Number of lock stripes to be used */
  private static final int LOCK_STRIPES = 32;

  private final String field;
  private final int maxEdgesPerDoc;
  private final boolean useParallel;
  private final float maxDocFreqRatio;
  private final Object[] locks;

  /** Constructor that disables edge pruning and disables parallel edge construction */
  public ApproximateDocGraphBuilder(String field, int maxEdgesPerDoc) {
    this(field, maxEdgesPerDoc, false, 1.0f);
  }

  /** Default constructor */
  public ApproximateDocGraphBuilder(
      String field, int maxEdgesPerDoc, boolean useParallel, float maxDocFreqRatio) {
    this.field = field;
    this.maxEdgesPerDoc = maxEdgesPerDoc;
    this.useParallel = useParallel;
    this.maxDocFreqRatio = maxDocFreqRatio;
    this.locks = new Object[LOCK_STRIPES];
    for (int i = 0; i < LOCK_STRIPES; i++) {
      locks[i] = new Object();
    }
  }

  /** Construct a graph based on document token overlap using LSH and sampling based techniques */
  @SuppressWarnings("unchecked")
  public SparseEdgeGraph build(LeafReader reader) throws IOException {
    final int maxDoc = reader.maxDoc();
    final InMemorySparseEdgeGraph graph = new InMemorySparseEdgeGraph(maxDoc);
    final Set<Integer>[] docTokens = (Set<Integer>[]) new Set<?>[maxDoc];
    final Map<BytesRef, Integer> tokenIds = new ConcurrentHashMap<>();
    final Map<Integer, BitSet> tokenToDocs = new ConcurrentHashMap<>();
    final AtomicInteger nextTokenId = new AtomicInteger();

    Terms terms = reader.terms(field);
    if (terms == null) {
      return graph;
    }

    List<BytesRef> allTerms = new ArrayList<>();
    TermsEnum termsEnum = terms.iterator();
    BytesRef term;
    while ((term = termsEnum.next()) != null) {
      allTerms.add(BytesRef.deepCopyOf(term));
    }

    IntStream termStream = IntStream.range(0, allTerms.size());
    if (useParallel) {
      termStream = termStream.parallel();
    }

    termStream.forEach(
        i -> {
          BytesRef termRef = allTerms.get(i);
          try {
            TermsEnum te = terms.iterator();
            if (!te.seekExact(termRef)) {
              return;
            }
            int df = te.docFreq();
            if (maxDocFreqRatio < 1.0f && df > maxDoc * maxDocFreqRatio) {
              return;
            }

            int tokenId = tokenIds.computeIfAbsent(termRef, _ -> nextTokenId.getAndIncrement());
            BitSet seen = new BitSet(maxDoc);
            PostingsEnum postings = te.postings(null, PostingsEnum.NONE);
            if (postings == null) {
              return;
            }

            while (postings.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
              int docID = postings.docID();
              seen.set(docID);
              synchronized (docTokens) {
                Set<Integer> tokens = docTokens[docID];
                if (tokens == null) {
                  tokens = new HashSet<>();
                  docTokens[docID] = tokens;
                }
                tokens.add(tokenId);
              }
            }

            tokenToDocs.put(tokenId, seen);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });

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
          List<Integer> sortedTokens = new ArrayList<>(tokensA);
          sortedTokens.sort(Comparator.naturalOrder());
          int sampleCount = 0;

          for (int tokenId : sortedTokens) {
            BitSet seen = tokenToDocs.get(tokenId);
            if (seen != null) {
              candidates.or(seen);
            }
            if (sampleCount++ >= MAX_SAMPLE_TOKENS) {
              break;
            }
          }

          int added = 0;
          for (int docB = candidates.nextSetBit(0);
              docB >= 0 && added < maxEdgesPerDoc;
              docB = candidates.nextSetBit(docB + 1)) {

            if (docB == docA) {
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
              Object lock = locks[docA % LOCK_STRIPES];
              synchronized (lock) {
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
