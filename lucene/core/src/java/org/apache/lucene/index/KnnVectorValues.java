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
import org.apache.lucene.codecs.lucene90.IndexedDISI;
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

  protected DocIterator iterator;

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
   * every doc has a value.
   */
  public int ordToDoc(int ord) {
    return ord;
  }

  /**
   * Creates a new copy of this {@link KnnVectorValues}. This is helpful when you need to access
   * different values at once, to avoid overwriting the underlying vector returned.
   */
  public KnnVectorValues copy() throws IOException {
    throw new UnsupportedOperationException("by class " + getClass().getName());
  }

  /** Returns the vector byte length, defaults to dimension multiplied by float byte size */
  public int getVectorByteLength() {
    return dimension() * getEncoding().byteSize;
  }

  public abstract VectorEncoding getEncoding();

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

  public abstract static class DocIterator extends DocIdSetIterator {

    /** return the value index (aka "ordinal" or "ord") corresponding to the current doc */
    public abstract int index();

    @Override
    public int advance(int target) throws IOException {
      return slowAdvance(target);
    }

    @Override
    public long cost() {
      throw new UnsupportedOperationException("for class " + getClass().getName());
    }

    public static DocIterator fromIndexedDISI(IndexedDISI disi) {
      return new DocIterator() {
        @Override
        public int docID() {
          return disi.docID();
        }

        @Override
        public int index() {
          return disi.index();
        }

        @Override
        public int nextDoc() throws IOException {
          return disi.nextDoc();
        }

        @Override
        public int advance(int target) throws IOException {
          return disi.advance(target);
        }

        @Override
        public long cost() {
          return disi.cost();
        }
      };
    }
  }

  public DocIterator iterator() {
    if (iterator == null) {
      iterator = createIterator();
    }
    return iterator;
  }

  protected DocIterator createIterator() {
    // don't force every class to implement; some are just wrappers of other values and use their
    // iterators
    throw new UnsupportedOperationException();
  }
  ;

  protected static DocIterator createDenseIterator(KnnVectorValues values) {
    return new DocIterator() {

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
        if (doc >= values.size() - 1) {
          return doc = NO_MORE_DOCS;
        } else {
          return ++doc;
        }
      }

      @Override
      public int advance(int target) {
        if (target >= values.size()) {
          return doc = NO_MORE_DOCS;
        }
        return doc = target;
      }

      @Override
      public long cost() {
        return values.size();
      }
    };
  }
}
