/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lucene.util.hnsw;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.TopKnnCollector;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.SparseFixedBitSet;

/**
 * Searches an HNSW graph to find nearest neighbors to a query vector. For more background on the
 * search algorithm, see {@link HnswGraph}.
 *
 * @param <T> the type of query vector
 */
public class HnswGraphSearcher<T> {
  private final VectorSimilarityFunction similarityFunction;
  private final VectorEncoding vectorEncoding;

  /**
   * Scratch data structures that are used in each {@link #searchLevel} call. These can be expensive
   * to allocate, so they're cleared and reused across calls.
   */
  private final NeighborQueue candidates;

  private BitSet visited;

  /**
   * Creates a new graph searcher.
   *
   * @param similarityFunction the similarity function to compare vectors
   * @param candidates max heap that will track the candidate nodes to explore
   * @param visited bit set that will track nodes that have already been visited
   */
  public HnswGraphSearcher(
      VectorEncoding vectorEncoding,
      VectorSimilarityFunction similarityFunction,
      NeighborQueue candidates,
      BitSet visited) {
    this.vectorEncoding = vectorEncoding;
    this.similarityFunction = similarityFunction;
    this.candidates = candidates;
    this.visited = visited;
  }

  /**
   * Searches HNSW graph for the nearest neighbors of a query vector.
   *
   * @param query search query vector
   * @param topK the number of nodes to be returned
   * @param vectors the vector values
   * @param similarityFunction the similarity function to compare vectors
   * @param graph the graph values. May represent the entire graph, or a level in a hierarchical
   *     graph.
   * @param acceptOrds {@link Bits} that represents the allowed document ordinals to match, or
   *     {@code null} if they are all allowed to match.
   * @param visitedLimit the maximum number of nodes that the search is allowed to visit
   * @return a set of collected vectors holding the nearest neighbors found
   */
  public static KnnCollector search(
      float[] query,
      int topK,
      RandomAccessVectorValues<float[]> vectors,
      VectorEncoding vectorEncoding,
      VectorSimilarityFunction similarityFunction,
      HnswGraph graph,
      Bits acceptOrds,
      int visitedLimit)
      throws IOException {
    KnnCollector knnCollector = new TopKnnCollector(topK, visitedLimit);
    search(query, knnCollector, vectors, vectorEncoding, similarityFunction, graph, acceptOrds);
    return knnCollector;
  }

  /**
   * Searches HNSW graph for the nearest neighbors of a query vector.
   *
   * @param query search query vector
   * @param knnCollector a collector of top knn results to be returned
   * @param vectors the vector values
   * @param similarityFunction the similarity function to compare vectors
   * @param graph the graph values. May represent the entire graph, or a level in a hierarchical
   *     graph.
   * @param acceptOrds {@link Bits} that represents the allowed document ordinals to match, or
   *     {@code null} if they are all allowed to match.
   */
  public static void search(
      float[] query,
      KnnCollector knnCollector,
      RandomAccessVectorValues<float[]> vectors,
      VectorEncoding vectorEncoding,
      VectorSimilarityFunction similarityFunction,
      HnswGraph graph,
      Bits acceptOrds)
      throws IOException {
    if (query.length != vectors.dimension()) {
      throw new IllegalArgumentException(
          "vector query dimension: "
              + query.length
              + " differs from field dimension: "
              + vectors.dimension());
    }
    HnswGraphSearcher<float[]> graphSearcher =
        new HnswGraphSearcher<>(
            vectorEncoding,
            similarityFunction,
            new NeighborQueue(knnCollector.k(), true),
            new SparseFixedBitSet(vectors.size()));
    search(query, knnCollector, vectors, graph, graphSearcher, acceptOrds);
  }

  /**
   * Search {@link OnHeapHnswGraph}, this method is thread safe, for parameters please refer to
   * {@link #search(float[], int, RandomAccessVectorValues, VectorEncoding,
   * VectorSimilarityFunction, HnswGraph, Bits, int)}
   */
  public static KnnCollector search(
      float[] query,
      int topK,
      RandomAccessVectorValues<float[]> vectors,
      VectorEncoding vectorEncoding,
      VectorSimilarityFunction similarityFunction,
      OnHeapHnswGraph graph,
      Bits acceptOrds,
      int visitedLimit)
      throws IOException {
    KnnCollector knnCollector = new TopKnnCollector(topK, visitedLimit);
    OnHeapHnswGraphSearcher<float[]> graphSearcher =
        new OnHeapHnswGraphSearcher<>(
            vectorEncoding,
            similarityFunction,
            new NeighborQueue(topK, true),
            new SparseFixedBitSet(vectors.size()));
    search(query, knnCollector, vectors, graph, graphSearcher, acceptOrds);
    return knnCollector;
  }

  /**
   * Searches HNSW graph for the nearest neighbors of a query vector.
   *
   * @param query search query vector
   * @param topK the number of nodes to be returned
   * @param vectors the vector values
   * @param similarityFunction the similarity function to compare vectors
   * @param graph the graph values. May represent the entire graph, or a level in a hierarchical
   *     graph.
   * @param acceptOrds {@link Bits} that represents the allowed document ordinals to match, or
   *     {@code null} if they are all allowed to match.
   * @param visitedLimit the maximum number of nodes that the search is allowed to visit
   * @return a set of collected vectors holding the nearest neighbors found
   */
  public static KnnCollector search(
      byte[] query,
      int topK,
      RandomAccessVectorValues<byte[]> vectors,
      VectorEncoding vectorEncoding,
      VectorSimilarityFunction similarityFunction,
      HnswGraph graph,
      Bits acceptOrds,
      int visitedLimit)
      throws IOException {
    KnnCollector collector = new TopKnnCollector(topK, visitedLimit);
    search(query, collector, vectors, vectorEncoding, similarityFunction, graph, acceptOrds);
    return collector;
  }

  /**
   * Searches HNSW graph for the nearest neighbors of a query vector.
   *
   * @param query search query vector
   * @param knnCollector a collector of top knn results to be returned
   * @param vectors the vector values
   * @param similarityFunction the similarity function to compare vectors
   * @param graph the graph values. May represent the entire graph, or a level in a hierarchical
   *     graph.
   * @param acceptOrds {@link Bits} that represents the allowed document ordinals to match, or
   *     {@code null} if they are all allowed to match.
   */
  public static void search(
      byte[] query,
      KnnCollector knnCollector,
      RandomAccessVectorValues<byte[]> vectors,
      VectorEncoding vectorEncoding,
      VectorSimilarityFunction similarityFunction,
      HnswGraph graph,
      Bits acceptOrds)
      throws IOException {
    if (query.length != vectors.dimension()) {
      throw new IllegalArgumentException(
          "vector query dimension: "
              + query.length
              + " differs from field dimension: "
              + vectors.dimension());
    }
    HnswGraphSearcher<byte[]> graphSearcher =
        new HnswGraphSearcher<>(
            vectorEncoding,
            similarityFunction,
            new NeighborQueue(knnCollector.k(), true),
            new SparseFixedBitSet(vectors.size()));
    search(query, knnCollector, vectors, graph, graphSearcher, acceptOrds);
  }

  /**
   * Search {@link OnHeapHnswGraph}, this method is thread safe, for parameters please refer to
   * {@link #search(byte[], int, RandomAccessVectorValues, VectorEncoding, VectorSimilarityFunction,
   * HnswGraph, Bits, int)}
   */
  public static KnnCollector search(
      byte[] query,
      int topK,
      RandomAccessVectorValues<byte[]> vectors,
      VectorEncoding vectorEncoding,
      VectorSimilarityFunction similarityFunction,
      OnHeapHnswGraph graph,
      Bits acceptOrds,
      int visitedLimit)
      throws IOException {
    OnHeapHnswGraphSearcher<byte[]> graphSearcher =
        new OnHeapHnswGraphSearcher<>(
            vectorEncoding,
            similarityFunction,
            new NeighborQueue(topK, true),
            new SparseFixedBitSet(vectors.size()));
    KnnCollector collector = new TopKnnCollector(topK, visitedLimit);
    search(query, collector, vectors, graph, graphSearcher, acceptOrds);
    return collector;
  }

  private static <T> void search(
      T query,
      KnnCollector knnCollector,
      RandomAccessVectorValues<T> vectors,
      HnswGraph graph,
      HnswGraphSearcher<T> graphSearcher,
      Bits acceptOrds)
      throws IOException {
    int initialEp = graph.entryNode();
    if (initialEp == -1) {
      return;
    }
    int[] epAndVisited =
        graphSearcher.findBestEntryPoint(query, vectors, graph, knnCollector.visitLimit());
    int numVisited = epAndVisited[1];
    int ep = epAndVisited[0];
    if (ep == -1) {
      knnCollector.incVisitedCount(numVisited);
      return;
    }
    KnnCollector results = new OrdinalTranslatedKnnCollector(knnCollector, vectors::ordToDoc);
    results.incVisitedCount(numVisited);
    graphSearcher.searchLevel(results, query, 0, new int[] {ep}, vectors, graph, acceptOrds);
  }

  /**
   * Searches for the nearest neighbors of a query vector in a given level.
   *
   * <p>If the search stops early because it reaches the visited nodes limit, then the results will
   * be marked incomplete through {@link NeighborQueue#incomplete()}.
   *
   * @param query search query vector
   * @param topK the number of nearest to query results to return
   * @param level level to search
   * @param eps the entry points for search at this level expressed as level 0th ordinals
   * @param vectors vector values
   * @param graph the graph values
   * @return a set of collected vectors holding the nearest neighbors found
   */
  public HnswGraphBuilder.GraphBuilderKnnCollector searchLevel(
      // Note: this is only public because Lucene91HnswGraphBuilder needs it
      T query,
      int topK,
      int level,
      final int[] eps,
      RandomAccessVectorValues<T> vectors,
      HnswGraph graph)
      throws IOException {
    HnswGraphBuilder.GraphBuilderKnnCollector results =
        new HnswGraphBuilder.GraphBuilderKnnCollector(topK);
    searchLevel(results, query, level, eps, vectors, graph, null);
    return results;
  }

  /**
   * Function to find the best entry point from which to search the zeroth graph layer.
   *
   * @param query vector query with which to search
   * @param vectors random access vector values
   * @param graph the HNSWGraph
   * @param visitLimit How many vectors are allowed to be visited
   * @return An integer array whose first element is the best entry point, and second is the number
   *     of candidates visited. Entry point of `-1` indicates visitation limit exceed
   * @throws IOException When accessing the vector fails
   */
  private int[] findBestEntryPoint(
      T query, RandomAccessVectorValues<T> vectors, HnswGraph graph, long visitLimit)
      throws IOException {
    int size = graph.size();
    int visitedCount = 1;
    prepareScratchState(vectors.size());
    int currentEp = graph.entryNode();
    float currentScore = compare(query, vectors, currentEp);
    boolean foundBetter;
    for (int level = graph.numLevels() - 1; level >= 1; level--) {
      foundBetter = true;
      visited.set(currentEp);
      // Keep searching the given level until we stop finding a better candidate entry point
      while (foundBetter) {
        foundBetter = false;
        graphSeek(graph, level, currentEp);
        int friendOrd;
        while ((friendOrd = graphNextNeighbor(graph)) != NO_MORE_DOCS) {
          assert friendOrd < size : "friendOrd=" + friendOrd + "; size=" + size;
          if (visited.getAndSet(friendOrd)) {
            continue;
          }
          if (visitedCount >= visitLimit) {
            return new int[] {-1, visitedCount};
          }
          float friendSimilarity = compare(query, vectors, friendOrd);
          visitedCount++;
          if (friendSimilarity > currentScore
              || (friendSimilarity == currentScore && friendOrd < currentEp)) {
            currentScore = friendSimilarity;
            currentEp = friendOrd;
            foundBetter = true;
          }
        }
      }
    }
    return new int[] {currentEp, visitedCount};
  }

  /**
   * Add the closest neighbors found to a priority queue (heap). These are returned in REVERSE
   * proximity order -- the most distant neighbor of the topK found, i.e. the one with the lowest
   * score/comparison value, will be at the top of the heap, while the closest neighbor will be the
   * last to be popped.
   */
  void searchLevel(
      KnnCollector results,
      T query,
      int level,
      final int[] eps,
      RandomAccessVectorValues<T> vectors,
      HnswGraph graph,
      Bits acceptOrds)
      throws IOException {

    int size = graph.size();
    prepareScratchState(vectors.size());

    for (int ep : eps) {
      if (visited.getAndSet(ep) == false) {
        if (results.earlyTerminated()) {
          break;
        }
        float score = compare(query, vectors, ep);
        results.incVisitedCount(1);
        candidates.add(ep, score);
        if (acceptOrds == null || acceptOrds.get(ep)) {
          results.collect(ep, score);
        }
      }
    }

    // A bound that holds the minimum similarity to the query vector that a candidate vector must
    // have to be considered.
    float minAcceptedSimilarity = results.minCompetitiveSimilarity();
    while (candidates.size() > 0 && results.earlyTerminated() == false) {
      // get the best candidate (closest or best scoring)
      float topCandidateSimilarity = candidates.topScore();
      if (topCandidateSimilarity < minAcceptedSimilarity) {
        break;
      }

      int topCandidateNode = candidates.pop();
      graphSeek(graph, level, topCandidateNode);
      int friendOrd;
      while ((friendOrd = graphNextNeighbor(graph)) != NO_MORE_DOCS) {
        assert friendOrd < size : "friendOrd=" + friendOrd + "; size=" + size;
        if (visited.getAndSet(friendOrd)) {
          continue;
        }

        if (results.earlyTerminated()) {
          break;
        }
        float friendSimilarity = compare(query, vectors, friendOrd);
        results.incVisitedCount(1);
        if (friendSimilarity >= minAcceptedSimilarity) {
          candidates.add(friendOrd, friendSimilarity);
          if (acceptOrds == null || acceptOrds.get(friendOrd)) {
            if (results.collect(friendOrd, friendSimilarity)) {
              minAcceptedSimilarity = results.minCompetitiveSimilarity();
            }
          }
        }
      }
    }
  }

  private float compare(T query, RandomAccessVectorValues<T> vectors, int ord) throws IOException {
    if (vectorEncoding == VectorEncoding.BYTE) {
      return similarityFunction.compare((byte[]) query, (byte[]) vectors.vectorValue(ord));
    } else {
      return similarityFunction.compare((float[]) query, (float[]) vectors.vectorValue(ord));
    }
  }

  private void prepareScratchState(int capacity) {
    candidates.clear();
    if (visited.length() < capacity) {
      visited = FixedBitSet.ensureCapacity((FixedBitSet) visited, capacity);
    }
    visited.clear();
  }

  /**
   * Seek a specific node in the given graph. The default implementation will just call {@link
   * HnswGraph#seek(int, int)}
   *
   * @throws IOException when seeking the graph
   */
  void graphSeek(HnswGraph graph, int level, int targetNode) throws IOException {
    graph.seek(level, targetNode);
  }

  /**
   * Get the next neighbor from the graph, you must call {@link #graphSeek(HnswGraph, int, int)}
   * before calling this method. The default implementation will just call {@link
   * HnswGraph#nextNeighbor()}
   *
   * @return see {@link HnswGraph#nextNeighbor()}
   * @throws IOException when advance neighbors
   */
  int graphNextNeighbor(HnswGraph graph) throws IOException {
    return graph.nextNeighbor();
  }

  /**
   * This class allows {@link OnHeapHnswGraph} to be searched in a thread-safe manner by avoiding
   * the unsafe methods (seek and nextNeighbor, which maintain state in the graph object) and
   * instead maintaining the state in the searcher object.
   *
   * <p>Note the class itself is NOT thread safe, but since each search will create a new Searcher,
   * the search methods using this class are thread safe.
   */
  private static class OnHeapHnswGraphSearcher<C> extends HnswGraphSearcher<C> {

    private NeighborArray cur;
    private int upto;

    private OnHeapHnswGraphSearcher(
        VectorEncoding vectorEncoding,
        VectorSimilarityFunction similarityFunction,
        NeighborQueue candidates,
        BitSet visited) {
      super(vectorEncoding, similarityFunction, candidates, visited);
    }

    @Override
    void graphSeek(HnswGraph graph, int level, int targetNode) {
      cur = ((OnHeapHnswGraph) graph).getNeighbors(level, targetNode);
      upto = -1;
    }

    @Override
    int graphNextNeighbor(HnswGraph graph) {
      if (++upto < cur.size()) {
        return cur.node[upto];
      }
      return NO_MORE_DOCS;
    }
  }
}
