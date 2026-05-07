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

package org.apache.lucene.search.join;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.util.BitSet;

/**
 * DiversifyingNearestChildrenKnnCollectorManager responsible for creating {@link
 * DiversifyingNearestChildrenKnnCollector} instances.
 */
public class DiversifyingNearestChildrenKnnCollectorManager implements KnnCollectorManager {

  // Cache keyed by (segment core cache key → (field name → docToOrd array)).
  // Entries are evicted automatically when the segment is closed via addClosedListener.
  private static final ConcurrentHashMap<IndexReader.CacheKey, ConcurrentHashMap<String, int[]>>
      DOC_TO_ORD_CACHE = new ConcurrentHashMap<>();

  // the number of docs to collect
  private final int k;
  // filter identifying the parent documents.
  private final BitSetProducer parentsFilter;
  // vector field name; used to build the docId-to-ordinal mapping for sibling expansion
  private final String field;

  /**
   * Constructor
   *
   * @param k - the number of top k vectors to collect
   * @param parentsFilter Filter identifying the parent documents.
   * @param field the vector field name
   */
  public DiversifyingNearestChildrenKnnCollectorManager(
      int k, BitSetProducer parentsFilter, IndexSearcher indexSearcher, String field) {
    this.k = k;
    this.parentsFilter = parentsFilter;
    this.field = field;
  }

  /**
   * Return a new {@link DiversifyingNearestChildrenKnnCollector} instance.
   *
   * @param visitedLimit the maximum number of nodes that the search is allowed to visit
   * @param context the leaf reader context
   */
  @Override
  public KnnCollector newCollector(
      int visitedLimit, KnnSearchStrategy searchStrategy, LeafReaderContext context)
      throws IOException {
    BitSet parentBitSet = parentsFilter.getBitSet(context);
    if (parentBitSet == null) {
      return null;
    }
    int[] docToOrd = getCachedDocToOrd(context);
    return new DiversifyingNearestChildrenKnnCollector(
        k, visitedLimit, searchStrategy, parentBitSet, docToOrd);
  }

  @Override
  public KnnCollector newOptimisticCollector(
      int visitedLimit, KnnSearchStrategy searchStrategy, LeafReaderContext context, int k)
      throws IOException {
    BitSet parentBitSet = parentsFilter.getBitSet(context);
    if (parentBitSet == null) {
      return null;
    }
    int[] docToOrd = getCachedDocToOrd(context);
    return new DiversifyingNearestChildrenKnnCollector(
        k, visitedLimit, searchStrategy, parentBitSet, docToOrd);
  }

  @Override
  public boolean isOptimistic() {
    return true;
  }

  /**
   * Returns the docId-to-ordinal array for the given leaf, building and caching it on first access.
   * The cached array is evicted automatically when the segment closes.
   */
  private int[] getCachedDocToOrd(LeafReaderContext context) throws IOException {
    IndexReader.CacheHelper cacheHelper = context.reader().getCoreCacheHelper();
    if (cacheHelper == null) {
      return buildDocToOrd(context);
    }
    IndexReader.CacheKey cacheKey = cacheHelper.getKey();
    ConcurrentHashMap<String, int[]> fieldMap = new ConcurrentHashMap<>();
    ConcurrentHashMap<String, int[]> existing = DOC_TO_ORD_CACHE.putIfAbsent(cacheKey, fieldMap);
    if (existing == null) {
      // We inserted the new entry — register cleanup when the segment closes
      cacheHelper.addClosedListener(DOC_TO_ORD_CACHE::remove);
    } else {
      fieldMap = existing;
    }
    int[] cached = fieldMap.get(field);
    if (cached != null) {
      return cached;
    }
    int[] built = buildDocToOrd(context);
    int[] race = fieldMap.putIfAbsent(field, built);
    return race != null ? race : built;
  }

  /**
   * Builds a docId-to-ordinal array for the given leaf, mapping each docId to its vector ordinal.
   *
   * <p>Returns an empty array if the field has no vector values in this segment at all — sibling
   * expansion will be disabled for this leaf.
   *
   * <p>Otherwise returns an array of size {@code maxDoc} where each entry is the vector ordinal for
   * that docId, or {@code -1} if that specific document has no vector (sparse indexing).
   */
  // Step 1 — Index time: ordinals are assigned by insertion order
  // In Lucene99FlatVectorsWriter.addValue(), each vector is appended to an ArrayList (vectors.add(copy)) and its docId
  // is recorded in docsWithField. Documents are always added in ascending docId order (enforced by assert docID >
  // lastDocID). So ordinal 0 = first doc with a vector, ordinal 1 = second, etc.
  //
  // Step 2 — Index time: ordToDoc mapping is written in the same order
  // In OrdToDocDISIReaderConfiguration.writeStoredMeta(), docsWithField.iterator() is iterated in ascending docId
  // order, and each docId is written to DirectMonotonicWriter sequentially. The i-th value written becomes ordinal
  // i — so the ordToDoc array stored on disk is exactly: ordToDoc[0] = first docId, ordToDoc[1] = second docId, ...
  //
  // Step 3 — Query time: buildDocToOrd inverts the same ordering
  // getFloatVectorValues(field).iterator() also yields docIds in ascending order (same set, same order as
  // docsWithField at index time). The loop:
  // while (iter.nextDoc() != NO_MORE_DOCS) {
  //   docToOrd[iter.docID()] = ord++;
  // }
  // assigns ord = 0 to the first docId, ord = 1 to the second — exactly inverting the ordToDoc array written at step 2.
  //
  // Step 4 — The HNSW graph uses these same ordinals as node IDs
  // HNSW nodes are identified by their ordinal (the position in the flat vector store). So when the searcher returns
  // ordinal k as a graph node, docToOrd[docId] = k being correct means docIdToOrdinal will find the right HNSW node
  // for any sibling docId.
  private int[] buildDocToOrd(LeafReaderContext context) throws IOException {
    FieldInfo fi = context.reader().getFieldInfos().fieldInfo(field);
    // fi = null if the field doesn't exist in this segment at all.
    // fi.getVectorDimension() = 0 if the field exist in the segment but was not indexed as a vector
    // field.
    if (fi == null || fi.getVectorDimension() == 0) {
      return new int[0];
    }
    DocIdSetIterator iter =
        switch (fi.getVectorEncoding()) {
          case FLOAT32 -> context.reader().getFloatVectorValues(field).iterator();
          case BYTE -> context.reader().getByteVectorValues(field).iterator();
        };
    int maxDoc = context.reader().maxDoc();
    int[] docToOrd = new int[maxDoc];
    Arrays.fill(docToOrd, -1);
    int ord = 0;
    while (iter.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
      docToOrd[iter.docID()] = ord++;
    }
    return docToOrd;
  }
}
