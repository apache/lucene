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

package org.apache.lucene.codecs;

import java.io.Closeable;
import java.io.IOException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Bits;

/** Reads vectors from an index. */
public abstract class KnnVectorsReader implements Closeable, Accountable {

  /** Sole constructor */
  protected KnnVectorsReader() {}

  /**
   * Checks consistency of this reader.
   *
   * <p>Note that this may be costly in terms of I/O, e.g. may involve computing a checksum value
   * against large data files.
   *
   * @lucene.internal
   */
  public abstract void checkIntegrity() throws IOException;

  /**
   * Returns the {@link VectorValues} for the given {@code field}. The behavior is undefined if the
   * given field doesn't have KNN vectors enabled on its {@link FieldInfo}. The return value is
   * never {@code null}.
   */
  public abstract VectorValues getVectorValues(String field) throws IOException;

  /**
   * Return the k nearest neighbor documents as determined by comparison of their vector values for
   * this field, to the given vector, by the field's similarity function. The score of each document
   * is derived from the vector similarity in a way that ensures scores are positive and that a
   * larger score corresponds to a higher ranking.
   *
   * <p>The search is allowed to be approximate, meaning the results are not guaranteed to be the
   * true k closest neighbors. For large values of k (for example when k is close to the total
   * number of documents), the search may also retrieve fewer than k documents.
   *
   * <p>The returned {@link TopDocs} will contain a {@link ScoreDoc} for each nearest neighbor, in
   * order of their similarity to the query vector (decreasing scores). The {@link TotalHits}
   * contains the number of documents visited during the search. If the search stopped early because
   * it hit {@code visitedLimit}, it is indicated through the relation {@code
   * TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO}.
   *
   * <p>The behavior is undefined if the given field doesn't have KNN vectors enabled on its {@link
   * FieldInfo}. The return value is never {@code null}.
   *
   * @param field the vector field to search
   * @param target the vector-valued query
   * @param k the number of docs to return
   * @param acceptDocs {@link Bits} that represents the allowed documents to match, or {@code null}
   *     if they are all allowed to match.
   * @param visitedLimit the maximum number of nodes that the search is allowed to visit
   * @return the k nearest neighbor documents, along with their (searchStrategy-specific) scores.
   */
  public abstract TopDocs search(
      String field, float[] target, int k, Bits acceptDocs, int visitedLimit) throws IOException;

  /**
   * Returns an instance optimized for merging. This instance may only be consumed in the thread
   * that called {@link #getMergeInstance()}.
   *
   * <p>The default implementation returns {@code this}
   */
  public KnnVectorsReader getMergeInstance() {
    return this;
  }
}
