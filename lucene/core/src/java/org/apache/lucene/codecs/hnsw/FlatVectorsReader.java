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

import java.io.Closeable;
import java.io.IOException;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
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
public abstract class FlatVectorsReader implements Closeable, Accountable {

  /** Scorer for flat vectors */
  protected final FlatVectorsScorer vectorScorer;

  /** Sole constructor */
  protected FlatVectorsReader(FlatVectorsScorer vectorsScorer) {
    this.vectorScorer = vectorsScorer;
  }

  /**
   * @return the {@link FlatVectorsScorer} for this reader.
   */
  public FlatVectorsScorer getFlatVectorScorer() {
    return vectorScorer;
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
   * Checks consistency of this reader.
   *
   * <p>Note that this may be costly in terms of I/O, e.g. may involve computing a checksum value
   * against large data files.
   *
   * @lucene.internal
   */
  public abstract void checkIntegrity() throws IOException;

  /**
   * Returns the {@link FloatVectorValues} for the given {@code field}. The behavior is undefined if
   * the given field doesn't have KNN vectors enabled on its {@link FieldInfo}. The return value is
   * never {@code null}.
   */
  public abstract FloatVectorValues getFloatVectorValues(String field) throws IOException;

  /**
   * Returns the {@link ByteVectorValues} for the given {@code field}. The behavior is undefined if
   * the given field doesn't have KNN vectors enabled on its {@link FieldInfo}. The return value is
   * never {@code null}.
   */
  public abstract ByteVectorValues getByteVectorValues(String field) throws IOException;
}
