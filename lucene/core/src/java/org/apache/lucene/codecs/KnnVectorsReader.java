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
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.search.TopDocs;
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

  /** Returns the {@link VectorValues} for the given {@code field} */
  public abstract VectorValues getVectorValues(String field) throws IOException;

  /**
   * Return the k nearest neighbor documents as determined by comparison of their vector values for
   * this field, to the given vector, by the field's search strategy. If the search strategy is
   * reversed, lower values indicate nearer vectors, otherwise higher scores indicate nearer
   * vectors. Unlike relevance scores, vector scores may be negative.
   *
   * @param field the vector field to search
   * @param target the vector-valued query
   * @param k the number of docs to return
   * @param acceptDocs {@link Bits} that represents the allowed documents to match, or {@code null}
   *     if they are all allowed to match.
   * @return the k nearest neighbor documents, along with their (searchStrategy-specific) scores.
   */
  public abstract TopDocs search(String field, float[] target, int k, Bits acceptDocs)
      throws IOException;

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
