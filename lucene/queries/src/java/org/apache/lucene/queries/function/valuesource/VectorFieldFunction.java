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
package org.apache.lucene.queries.function.valuesource;

import java.io.IOException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.DocIdSetIterator;

/** An implementation for retrieving {@link FunctionValues} instances for knn vectors fields. */
public abstract class VectorFieldFunction extends FunctionValues {

  protected final ValueSource valueSource;
  int lastDocID;

  protected VectorFieldFunction(ValueSource valueSource) {
    this.valueSource = valueSource;
  }

  protected abstract DocIdSetIterator getVectorIterator();

  @Override
  public String toString(int doc) throws IOException {
    return valueSource.description() + strVal(doc);
  }

  @Override
  public boolean exists(int doc) throws IOException {
    if (doc < lastDocID) {
      throw new IllegalArgumentException(
          "docs were sent out-of-order: lastDocID=" + lastDocID + " vs docID=" + doc);
    }

    lastDocID = doc;

    int curDocID = getVectorIterator().docID();
    if (doc > curDocID) {
      curDocID = getVectorIterator().advance(doc);
    }
    return doc == curDocID;
  }

  /**
   * Checks the Vector Encoding of a field
   *
   * @throws IllegalStateException if {@code field} exists, but was not indexed with vectors.
   * @throws IllegalStateException if {@code field} has vectors, but using a different encoding
   * @lucene.internal
   * @lucene.experimental
   */
  static void checkField(LeafReader in, String field, VectorEncoding expectedEncoding) {
    FieldInfo fi = in.getFieldInfos().fieldInfo(field);
    if (fi != null) {
      final VectorEncoding actual = fi.hasVectorValues() ? fi.getVectorEncoding() : null;
      if (expectedEncoding != actual) {
        throw new IllegalStateException(
            "Unexpected vector encoding ("
                + actual
                + ") for field "
                + field
                + "(expected="
                + expectedEncoding
                + ")");
      }
    }
  }
}
