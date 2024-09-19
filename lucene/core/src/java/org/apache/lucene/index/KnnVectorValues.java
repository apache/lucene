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
 * or {@link KnnByteVectorField}.
 *
 * @lucene.experimental
 */
public abstract class KnnVectorValues {

  /** Return the dimension of the vectors */
  public abstract int dimension();

  /**
   * Return the number of vectors for this field.
   *
   * @return the number of vectors returned by this iterator
   */
  public abstract int size();

  /**
   * Return the docid of the document indexed with the given vector ordinal. This default
   * implementation returns the argument and is appropriate for dense values implementations where
   * every doc has a single value.
   */
  public int ordToDoc(int ord) {
    return ord;
  }

  /**
   * Creates a new copy of this {@link KnnVectorValues}. This is helpful when you need to access
   * different values at once, to avoid overwriting the underlying vector returned.
   */
  public abstract KnnVectorValues copy() throws IOException;

  /** Returns the vector byte length, defaults to dimension multiplied by float byte size */
  public int getVectorByteLength() {
    return dimension() * getEncoding().byteSize;
  }

  /** The vector encoding of these values. */
  public abstract VectorEncoding getEncoding();

  /** Returns a Bits accepting docs accepted by the argument and having a vector value */
  public Bits getAcceptOrds(Bits acceptDocs) {
    // FIXME: change default to return acceptDocs and provide this impl
    // somewhere more specialized (in every non-dense impl).
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

  /** Create an iterator for this instance. */
  public DocIndexIterator iterator() {
    throw new UnsupportedOperationException();
  }

  /**
   * A DocIdSetIterator that also provides an index() method tracking a distinct ordinal for a
   * vector associated with each doc.
   */
  public abstract static class DocIndexIterator extends DocIdSetIterator {

    /** return the value index (aka "ordinal" or "ord") corresponding to the current doc */
    public abstract int index();
  }

  /**
   * Creates an iterator for instances where every doc has a value, and the value ordinals are equal
   * to the docids.
   */
  protected DocIndexIterator createDenseIterator() {
    return new DocIndexIterator() {

      int doc = -1;

      @Override
      public int docID() {
        return doc;
      }

      @Override
      public int index() {
        return doc;
      }

      @Override
      public int nextDoc() throws IOException {
        if (doc >= size() - 1) {
          return doc = NO_MORE_DOCS;
        } else {
          return ++doc;
        }
      }

      @Override
      public int advance(int target) {
        if (target >= size()) {
          return doc = NO_MORE_DOCS;
        }
        return doc = target;
      }

      @Override
      public long cost() {
        return size();
      }
    };
  }

  /**
   * Creates an iterator from a DocIdSetIterator indicating which docs have values, and for which
   * ordinals increase monotonically with docid.
   */
  protected static DocIndexIterator fromDISI(DocIdSetIterator docsWithField) {
    return new DocIndexIterator() {

      int ord = -1;

      @Override
      public int docID() {
        return docsWithField.docID();
      }

      @Override
      public int index() {
        return ord;
      }

      @Override
      public int nextDoc() throws IOException {
        if (docID() == NO_MORE_DOCS) {
          return NO_MORE_DOCS;
        }
        ord++;
        return docsWithField.nextDoc();
      }

      @Override
      public int advance(int target) throws IOException {
        return docsWithField.advance(target);
      }

      @Override
      public long cost() {
        return docsWithField.cost();
      }
    };
  }

  /**
   * Creates an iterator from this instance's ordinal-to-docid mapping which must be monotonic
   * (docid increases when ordinal does).
   */
  protected DocIndexIterator createSparseIterator() {
    return new DocIndexIterator() {
      private int ord = -1;

      @Override
      public int docID() {
        if (ord == -1) {
          return -1;
        }
        if (ord == NO_MORE_DOCS) {
          return NO_MORE_DOCS;
        }
        return ordToDoc(ord);
      }

      @Override
      public int index() {
        return ord;
      }

      @Override
      public int nextDoc() throws IOException {
        if (ord >= size() - 1) {
          ord = NO_MORE_DOCS;
        } else {
          ++ord;
        }
        return docID();
      }

      @Override
      public int advance(int target) throws IOException {
        return slowAdvance(target);
      }

      @Override
      public long cost() {
        return size();
      }
    };
  }
}
