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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;

/**
 * Builds a sparse document similarity graph from postings.
 */
public final class DocGraphBuilder {
    /**
     * Default number of maximum edges retained per document
     */
    public static final int DEFAULT_MAX_EDGES = 10;

    private final String field;
    private final int maxEdgesPerDoc;

    /**
     * Create a new builder for the specified field.
     *
     * @param field          field to use for similarity
     * @param maxEdgesPerDoc maximum outgoing edges per document
     */
    public DocGraphBuilder(String field, int maxEdgesPerDoc) {
        this.field = field;
        this.maxEdgesPerDoc = maxEdgesPerDoc;
    }

    private static double cosine(Map<String, Integer> a, Map<String, Integer> b) {
        double dot = 0.0;
        double normA = 0.0;
        double normB = 0.0;

        for (Map.Entry<String, Integer> entry : a.entrySet()) {
            int tfA = entry.getValue();
            normA += tfA * tfA;
            Integer tfB = b.get(entry.getKey());
            if (tfB != null) {
                dot += tfA * tfB;
            }
        }

        for (int tf : b.values()) {
            normB += tf * tf;
        }

        if (normA == 0 || normB == 0) {
            return 0.0;
        }

        return dot / (Math.sqrt(normA) * Math.sqrt(normB));
    }

    /**
     * Constructs a sparse similarity graph for the given segment.
     */
    public SparseEdgeGraph build(LeafReader reader) throws IOException {
        final int maxDoc = reader.maxDoc();
        final SparseEdgeGraph graph = new InMemorySparseEdgeGraph();
        final Map<Integer, Map<String, Integer>> docTermFreqs = new HashMap<>();
        final Map<String, Set<Integer>> termToDocs = new HashMap<>();

        Terms terms = reader.terms(field);
        if (terms == null) {
            return graph;
        }

        TermsEnum termsEnum = terms.iterator();
        BytesRef term;
        while ((term = termsEnum.next()) != null) {
            PostingsEnum postings = termsEnum.postings(null, PostingsEnum.FREQS);
            if (postings == null) {
                continue;
            }
            while (postings.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                int docID = postings.docID();
                int freq = postings.freq();
                String termStr = term.utf8ToString();

                docTermFreqs.computeIfAbsent(docID, k -> new HashMap<>()).put(termStr, freq);
                termToDocs.computeIfAbsent(termStr, k -> new HashSet<>()).add(docID);
            }
        }

        for (int docA : docTermFreqs.keySet()) {
            final Map<String, Integer> vecA = docTermFreqs.get(docA);
            final PriorityQueue<Neighbor> queue = new PriorityQueue<>(maxEdgesPerDoc);
            final Set<Integer> candidates = new HashSet<>();

            for (String termKey : vecA.keySet()) {
                Set<Integer> docsWithTerm = termToDocs.get(termKey);
                if (docsWithTerm != null) {
                    candidates.addAll(docsWithTerm);
                }
            }

            for (int docB : candidates) {
                if (docA == docB) {
                    continue;
                }
                final Map<String, Integer> vecB = docTermFreqs.get(docB);
                if (vecB == null) {
                    continue;
                }
                final double score = cosine(vecA, vecB);
                if (score > 0) {
                    queue.offer(new Neighbor(docB, (float) score));
                    if (queue.size() > maxEdgesPerDoc) {
                        queue.poll();
                    }
                }
            }

            while (!queue.isEmpty()) {
                Neighbor neighbor = queue.poll();
                graph.addEdge(docA, neighbor.docID, neighbor.weight);
            }
        }

        return graph;
    }

    private static class Neighbor implements Comparable<Neighbor> {
        final int docID;
        final float weight;

        Neighbor(int docID, float weight) {
            this.docID = docID;
            this.weight = weight;
        }

        @Override
        public int compareTo(Neighbor other) {
            return Float.compare(this.weight, other.weight);
        }
    }
}