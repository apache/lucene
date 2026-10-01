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

import java.io.IOException;
import org.apache.lucene.document.column.VectorValuesCursor;
import org.apache.lucene.util.Accountable;

/**
 * Vectors writer for a field.
 *
 * @param <T> an array type; the type of vectors to be written
 */
public abstract class KnnFieldVectorsWriter<T> implements Accountable {

  /** Sole constructor */
  protected KnnFieldVectorsWriter() {}

  /**
   * Add new docID with its vector value to the given field for indexing. Doc IDs must be added in
   * increasing order.
   */
  public abstract void addValue(int docID, T vectorValue) throws IOException;

  /**
   * Add {@code values.size()} vectors for the consecutive doc IDs {@code [firstDocID, firstDocID +
   * values.size())}. {@code firstDocID} must be greater than every doc ID added so far, and every
   * vector has exactly {@code values.dimension()} elements, which matches the field's dimension.
   * Implementations must consume exactly {@code values.size()} vectors.
   *
   * <p>The cursor may throw while it is being consumed, for example when a vector fails validation.
   * In that case the documents of the whole batch are marked as deleted, but the writer must remain
   * in a consistent state: every doc ID it has recorded must have its vector. Validation runs after
   * the copy, so when {@link VectorValuesCursor#fill} throws, the destination may already hold the
   * rejected vectors; record doc IDs and vectors only after {@code fill} has returned.
   *
   * <p>The default implementation calls {@link #addValue} once per vector. Override for a more
   * efficient bulk path. A writer that wraps another field writer should forward this method to it,
   * otherwise the delegate's override is never reached.
   *
   * @lucene.experimental
   */
  public void addDenseValues(int firstDocID, VectorValuesCursor<T> values) throws IOException {
    final int size = values.size();
    for (int i = 0; i < size; i++) {
      addValue(firstDocID + i, values.next());
    }
  }

  /**
   * Used to copy values being indexed to internal storage.
   *
   * @param vectorValue an array containing the vector value to add
   * @return a copy of the value; a new array
   */
  public abstract T copyValue(T vectorValue);
}
