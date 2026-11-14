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
package org.apache.lucene.codecs.hnsw;

import java.io.IOException;
import java.util.List;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.index.MergeState;

/**
 * Implemented by a {@link KnnVectorsReader} that is a view over the vectors of other readers under
 * different doc IDs, such as the views that {@link org.apache.lucene.index.IndexWriter} builds when
 * a merge reorders documents. Such a view has no HNSW graph of its own, but the readers it wraps
 * may, so an {@link org.apache.lucene.util.hnsw.HnswGraphMerger} can add those readers in place of
 * the view and reuse their graphs instead of rebuilding the merged graph from scratch. A view that
 * also implements {@link HnswGraphProvider} is still replaced by its sources.
 *
 * <p>Which documents are live is decided by the view: a document of a wrapped reader is live if the
 * document it maps to is live in the view.
 *
 * @lucene.experimental
 */
public interface HnswGraphMergeSources {

  /**
   * A reader whose vectors the view exposes.
   *
   * @param reader the wrapped reader
   * @param docMap maps every doc ID of {@code reader} to the corresponding doc ID of the view
   * @param maxDoc the number of documents of {@code reader}
   */
  record Source(KnnVectorsReader reader, MergeState.DocMap docMap, int maxDoc) {}

  /**
   * Return the readers that hold the vectors of the given field, with the mapping of their doc IDs
   * to the doc IDs of this view. Together they must cover every vector of the field exactly once.
   *
   * @param field the field being merged
   * @throws IOException when the readers cannot be accessed
   */
  List<Source> getMergeSources(String field) throws IOException;
}
