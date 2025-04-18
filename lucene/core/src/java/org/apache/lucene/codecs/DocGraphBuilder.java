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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;

/** Builds a sparse document similarity graph from postings lists. */
public final class DocGraphBuilder {

  /** Default maximum outgoing edges per document */
  public static final int DEFAULT_MAX_EDGES = 10;

  private final String field;
  private final int maxEdgesPerDoc;

  /** Reusable vector store for term frequencies */
  private final Map<Integer, IntVector> docTermVectors = new HashMap<>();

  /**
   * Constructs a new graph builder.
   *
   * @param field field to use for binning
   * @param maxEdgesPerDoc number of outgoing edges to retain per document
   */
  public DocGraphBuilder(String field, int maxEdgesPerDoc) {
    this.field = field;
    this.maxEdgesPerDoc = maxEdgesPerDoc;
  }

  /** Constructs the sparse similarity graph using term overlap. */
  public SparseEdgeGraph build(LeafReader reader) throws IOException {
    final SparseEdgeGraph graph = new InMemorySparseEdgeGraph();
    final Map<String, ArrayList<Integer>> termToDocList = new HashMap<>();

    Terms terms = reader.terms(field);
    if (terms == null) {
      return graph;
    }

    TermsEnum termsEnum = terms.iterator();
    BytesRef term;
    while ((term = termsEnum.next()) != null) {
      String termStr = term.utf8ToString();
      PostingsEnum postings = termsEnum.postings(null, PostingsEnum.FREQS);
      if (postings == null) {
        continue;
      }
      while (postings.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
        int docID = postings.docID();
        int freq = postings.freq();

        docTermVectors.computeIfAbsent(docID, _ -> new IntVector()).add(termStr, freq);
        termToDocList.computeIfAbsent(termStr, _ -> new ArrayList<>()).add(docID);
      }
    }

    for (Map.Entry<Integer, IntVector> entryA : docTermVectors.entrySet()) {
      int docA = entryA.getKey();
      IntVector vecA = entryA.getValue();

      PriorityQueue<Neighbor> topK = new PriorityQueue<>(maxEdgesPerDoc);
      final Map<Integer, Float> candidates = new HashMap<>();

      for (String termKey : vecA.keys()) {
        for (int docB : termToDocList.getOrDefault(termKey, new ArrayList<>())) {
          if (docA == docB) {
            continue;
          }
          candidates.putIfAbsent(docB, 0f);
        }
      }

      for (Map.Entry<Integer, Float> candidate : candidates.entrySet()) {
        int docB = candidate.getKey();
        IntVector vecB = docTermVectors.get(docB);
        if (vecB == null) {
          continue;
        }
        float sim = cosine(vecA, vecB);
        if (sim > 0) {
          topK.offer(new Neighbor(docB, sim));
          if (topK.size() > maxEdgesPerDoc) {
            topK.poll();
          }
        }
      }

      while (!topK.isEmpty()) {
        Neighbor neighbor = topK.poll();
        graph.addEdge(docA, neighbor.docID, neighbor.score);
      }
    }

    // Ensure all documents are represented in the graph, even if they have no edges
    for (int docID = 0; docID < reader.maxDoc(); docID++) {
      graph.ensureVertex(docID);
    }

    return graph;
  }

  private static float cosine(IntVector a, IntVector b) {
    float dot = 0f, normA = 0f, normB = 0f;

    for (Map.Entry<String, Integer> entry : a.entries()) {
      int tfA = entry.getValue();
      normA += tfA * tfA;
      Integer tfB = b.get(entry.getKey());
      if (tfB != null) {
        dot += tfA * tfB;
      }
    }

    for (int tfB : b.values()) {
      normB += tfB * tfB;
    }

    if (normA == 0f || normB == 0f) {
      return 0f;
    }

    return dot / (float) (Math.sqrt(normA) * Math.sqrt(normB));
  }

  private static final class Neighbor implements Comparable<Neighbor> {
    final int docID;
    final float score;

    Neighbor(int docID, float score) {
      this.docID = docID;
      this.score = score;
    }

    @Override
    public int compareTo(Neighbor other) {
      return Float.compare(this.score, other.score);
    }
  }

  /** Lightweight term frequency vector with string keys and int values. */
  private static final class IntVector {
    private final Map<String, Integer> tf = new HashMap<>();

    void add(String key, int freq) {
      tf.merge(key, freq, Integer::sum);
    }

    Integer get(String key) {
      return tf.get(key);
    }

    Set<String> keys() {
      return tf.keySet();
    }

    Iterable<Map.Entry<String, Integer>> entries() {
      return tf.entrySet();
    }

    Iterable<Integer> values() {
      return tf.values();
    }
  }
}
