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
import org.apache.lucene.document.KnnVectorField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;

/**
 * This class provides access to per-document floating point vector values indexed as {@link
 * KnnVectorField}.
 *
 * @lucene.experimental
 */
public abstract class VectorValues extends DocIdSetIterator {

  /** The maximum length of a vector */
  public static final int MAX_DIMENSIONS = 20480;

  /** Sole constructor */
  protected VectorValues() {}

  /** Return the dimension of the vectors */
  public abstract int dimension();

  /**
   * TODO: should we use cost() for this? We rely on its always being exactly the number of
   * documents having a value for this field, which is not guaranteed by the cost() contract, but in
   * all the implementations so far they are the same.
   *
   * @return the number of vectors returned by this iterator
   */
  public abstract int size();

  /**
   * Return the vector value for the current document ID. It is illegal to call this method when the
   * iterator is not positioned: before advancing, or after failing to advance. The returned array
   * may be shared across calls, re-used, and modified as the iterator advances.
   *
   * @return the vector value
   */
  public abstract float[] vectorValue() throws IOException;

  /**
   * Return the binary encoded vector value for the current document ID. These are the bytes
   * corresponding to the float array return by {@link #vectorValue}. It is illegal to call this
   * method when the iterator is not positioned: before advancing, or after failing to advance. The
   * returned storage may be shared across calls, re-used and modified as the iterator advances.
   *
   * @return the binary value
   */
  public BytesRef binaryValue() throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Represents the lack of vector values. It is returned by providers that do not support
   * VectorValues.
   */
  public static final VectorValues EMPTY =
      new VectorValues() {

        @Override
        public int size() {
          return 0;
        }

        @Override
        public int dimension() {
          return 0;
        }

        @Override
        public float[] vectorValue() {
          throw new IllegalStateException(
              "Attempt to get vectors from EMPTY values (which was not advanced)");
        }

        @Override
        public int docID() {
          throw new IllegalStateException("VectorValues is EMPTY, and not positioned on a doc");
        }

        @Override
        public int nextDoc() {
          return NO_MORE_DOCS;
        }

        @Override
        public int advance(int target) {
          return NO_MORE_DOCS;
        }

        @Override
        public long cost() {
          return 0;
        }
      };

  /** Sorting VectorValues that iterate over documents in the order of the provided sortMap */
  public static class SortingVectorValues extends VectorValues
      implements RandomAccessVectorValuesProducer {

    private final VectorValues delegate;
    private final RandomAccessVectorValues randomAccess;
    private final int[] docIdOffsets;
    private final int[] ordMap;
    private int docId = -1;

    /** Sorting VectorValues */
    public SortingVectorValues(VectorValues delegate, Sorter.DocMap sortMap) throws IOException {
      this.delegate = delegate;
      randomAccess = ((RandomAccessVectorValuesProducer) delegate).randomAccess();
      docIdOffsets = new int[sortMap.size()];

      int offset = 1; // 0 means no vector for this (field, document)
      int docID;
      while ((docID = delegate.nextDoc()) != NO_MORE_DOCS) {
        int newDocID = sortMap.oldToNew(docID);
        docIdOffsets[newDocID] = offset++;
      }

      // set up ordMap to map from new dense ordinal to old dense ordinal
      ordMap = new int[offset - 1];
      int ord = 0;
      for (int docIdOffset : docIdOffsets) {
        if (docIdOffset != 0) {
          ordMap[ord++] = docIdOffset - 1;
        }
      }
      assert ord == ordMap.length;
    }

    @Override
    public int docID() {
      return docId;
    }

    @Override
    public int nextDoc() throws IOException {
      while (docId < docIdOffsets.length - 1) {
        ++docId;
        if (docIdOffsets[docId] != 0) {
          return docId;
        }
      }
      docId = NO_MORE_DOCS;
      return docId;
    }

    @Override
    public BytesRef binaryValue() throws IOException {
      return randomAccess.binaryValue(docIdOffsets[docId] - 1);
    }

    @Override
    public float[] vectorValue() throws IOException {
      return randomAccess.vectorValue(docIdOffsets[docId] - 1);
    }

    @Override
    public int dimension() {
      return delegate.dimension();
    }

    @Override
    public int size() {
      return delegate.size();
    }

    @Override
    public int advance(int target) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long cost() {
      return size();
    }

    @Override
    public RandomAccessVectorValues randomAccess() throws IOException {

      // Must make a new delegate randomAccess so that we have our own distinct float[]
      final RandomAccessVectorValues delegateRA =
          ((RandomAccessVectorValuesProducer) SortingVectorValues.this.delegate).randomAccess();

      return new RandomAccessVectorValues() {

        @Override
        public int size() {
          return delegateRA.size();
        }

        @Override
        public int dimension() {
          return delegateRA.dimension();
        }

        @Override
        public float[] vectorValue(int targetOrd) throws IOException {
          return delegateRA.vectorValue(ordMap[targetOrd]);
        }

        @Override
        public BytesRef binaryValue(int targetOrd) {
          throw new UnsupportedOperationException();
        }
      };
    }
  }
}
