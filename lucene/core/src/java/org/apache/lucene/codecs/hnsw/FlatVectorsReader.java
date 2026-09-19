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
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.store.VectorBatch;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.hnsw.RandomVectorScorer;

/**
 * Reads vectors from an index. When searching this reader, it iterates every vector in the index
 * and scores them
 *
 * <p>This class is useful when:
 *
 * <ul>
 *   <li>the number of vectors is small
 *   <li>when used along side some additional indexing structure that can be used to better search
 *       the vectors (like HNSW).
 * </ul>
 *
 * @lucene.experimental
 */
public abstract class FlatVectorsReader extends KnnVectorsReader implements Accountable {

  /**
   * Returns a {@link FlatVectorsScorer} for the given field.
   *
   * @param field the field to search
   */
  public abstract FlatVectorsScorer getFlatVectorScorer(String field) throws IOException;

  @Override
  public void search(String field, float[] target, KnnCollector knnCollector, AcceptDocs acceptDocs)
      throws IOException {
    // don't scan stored field data. If we didn't index it, produce no search results
  }

  @Override
  public void search(String field, byte[] target, KnnCollector knnCollector, AcceptDocs acceptDocs)
      throws IOException {
    // don't scan stored field data. If we didn't index it, produce no search results
  }

  @Override
  public void search(String field, short[] target, KnnCollector knnCollector, AcceptDocs acceptDocs)
      throws IOException {
    // don't scan stored field data. If we didn't index it, produce no search results
  }

  /**
   * Returns a {@link RandomVectorScorer} for the given field and target vector.
   *
   * @param field the field to search
   * @param target the target vector
   * @return a {@link RandomVectorScorer} for the given field and target vector.
   * @throws IOException if an I/O error occurs when reading from the index.
   */
  public abstract RandomVectorScorer getRandomVectorScorer(String field, float[] target)
      throws IOException;

  /**
   * Returns a {@link RandomVectorScorer} for the given field and target vector.
   *
   * @param field the field to search
   * @param target the target vector
   * @return a {@link RandomVectorScorer} for the given field and target vector.
   * @throws IOException if an I/O error occurs when reading from the index.
   */
  public abstract RandomVectorScorer getRandomVectorScorer(String field, byte[] target)
      throws IOException;

  /**
   * Returns a {@link RandomVectorScorer} for the given field and target vector.
   *
   * @param field the field to search
   * @param target the target vector
   * @return a {@link RandomVectorScorer} for the given field and target vector.
   * @throws IOException if an I/O error occurs when reading from the index.
   */
  public abstract RandomVectorScorer getRandomVectorScorer(String field, short[] target)
      throws IOException;

  /**
   * Returns an instance optimized for merging. This instance may only be consumed in the thread
   * that called {@link #getMergeInstance()}.
   *
   * <p>The default implementation returns {@code this}
   */
  @Override
  public FlatVectorsReader getMergeInstance() throws IOException {
    return this;
  }

  /**
   * Opens a batch that can gather this reader's raw-vector reads together with those of sibling
   * readers over the same store, or returns {@code null} when the store cannot do that. A KNN
   * rerank shortlist is spread across every segment, so batching across readers is what turns one
   * submission per segment into one submission per query.
   *
   * @lucene.experimental
   */
  public VectorBatch newRawVectorBatch(String field) throws IOException {
    return null;
  }

  /**
   * Queues the raw float32 vectors for {@code ords} into {@code batch} instead of reading them now;
   * they are available in {@code out} once {@link VectorBatch#execute()} has run: vector {@code i}
   * occupies {@code out[i * dim]} to {@code out[i * dim + dim - 1]}, where {@code dim} is the
   * field's dimension.
   *
   * @return false if this reader cannot contribute to {@code batch}
   * @lucene.experimental
   */
  public boolean addRawVectors(String field, int[] ords, int count, float[] out, VectorBatch batch)
      throws IOException {
    return false;
  }
}
