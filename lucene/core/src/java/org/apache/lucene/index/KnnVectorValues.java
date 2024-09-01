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
package org.apache.lucene.index;

import java.io.IOException;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;

/**
 * This class abstracts addressing of document vector values indexed as {@link KnnFloatVectorField}
 * or {@link KnnByteVectorField}. Hmm, we also need docToOrd(s)? We need to be able to retrieve a
 * vector value for a document.
 *
 * @lucene.experimental
 */
public abstract class KnnVectorValues {

  protected KnnValuesDocIterator iterator;

  /** Return the dimension of the vectors */
  public abstract int dimension();

  /**
   * Return the number of vectors for this field.
   *
   * @return the number of vectors returned by this iterator
   */
  public abstract int size();

  /** Return the docid of the document indexed with the given vector ordingl */
  public int ordToDoc(int ord) {
    throw new UnsupportedOperationException();
  }

  /** Return the vector ordinal indexed for the given document or -1 if there is none */
  public int docToOrd(int ord) {
    throw new UnsupportedOperationException();
  }

  /**
   * Creates a new copy of this {@link KnnVectorValues}. This is helpful when you need to access
   * different values at once, to avoid overwriting the underlying vector returned.
   */
  public KnnVectorValues copy() throws IOException {
    throw new UnsupportedOperationException();
  }

  /** Returns the byte length of the vector values. */
  public abstract int getVectorByteLength();

  public Bits getAcceptOrds(Bits acceptDocs) {
    // FIXME: change default to return acceptDocs and provide this impl
    // somewhere more specialized
    if (acceptDocs == null) {
      return null;
    }
    return new Bits() {
      @Override
      public boolean get(int index) {
        return acceptDocs.get(ordToDoc(index));
      }

      @Override
      public int length() {
        return size();
      }
    };
  }

  public abstract static class KnnValuesDocIterator extends DocIdSetIterator {

    /** return the value index (aka "ordinal" or "ord") corresponding to the current doc */
    public abstract int index();
  }

  public KnnValuesDocIterator iterator() {
    if (iterator == null) {
      iterator =
          new KnnValuesDocIterator() {

            int ord = -1;
            int doc = -1;

            @Override
            public int docID() {
              return doc;
            }

            @Override
            public int index() {
              return ord;
            }

            @Override
            public int nextDoc() throws IOException {
              if (ord >= size() - 1) {
                return NO_MORE_DOCS;
              } else {
                doc = docToOrd(++ord);
                return doc;
              }
            }

            @Override
            public int advance(int target) {
              throw new UnsupportedOperationException();
            }

            @Override
            public long cost() {
              return size();
            }
          };
    }
    return iterator;
  }
}
