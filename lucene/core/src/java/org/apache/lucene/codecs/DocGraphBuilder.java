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
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;

/**
 * Builds a sparse document similarity graph from term vectors.
 */
public final class DocGraphBuilder {
    /** Default number of maximum edges retained per document */
  public static final int DEFAULT_MAX_EDGES = 10;

  private final String field;
  private final int maxEdgesPerDoc;

  /**
   * Create a new builder for the specified field.
   *
   * @param field field to use for similarity
   * @param maxEdgesPerDoc maximum outgoing edges per document
   */
  public DocGraphBuilder(String field, int maxEdgesPerDoc) {
    this.field = field;
    this.maxEdgesPerDoc = maxEdgesPerDoc;
  }

  /**
   * Constructs a sparse similarity graph for the given segment.
   */
  public SparseEdgeGraph build(LeafReader reader) throws IOException {
    final int maxDoc = reader.maxDoc();
    final SparseEdgeGraph graph = new InMemorySparseEdgeGraph();
    final Map<Integer, Map<String, Integer>> vectors = new HashMap<>();

    for (int docID = 0; docID < maxDoc; docID++) {
      Terms terms = reader.terms(field);
      if (terms == null) {
        continue;
      }
      TermsEnum termsEnum = terms.iterator();
      BytesRef term;
      Map<String, Integer> freqs = new HashMap<>();
      while ((term = termsEnum.next()) != null) {
        PostingsEnum postings = termsEnum.postings(null, PostingsEnum.FREQS);
        if (postings != null && postings.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
          freqs.put(term.utf8ToString(), postings.freq());
        }
      }
      if (!freqs.isEmpty()) {
        vectors.put(docID, freqs);
      }
    }

    for (int docA : vectors.keySet()) {
      final Map<String, Integer> vecA = vectors.get(docA);
      final PriorityQueue<Neighbor> queue = new PriorityQueue<>(maxEdgesPerDoc);

      for (int docB : vectors.keySet()) {
        if (docA == docB) {
          continue;
        }
        final double score = cosine(vecA, vectors.get(docB));
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