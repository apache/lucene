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
  public static final int MAX_DIMENSIONS = 1024;

  /** Sole constructor */
  protected VectorValues() {}

  /** Return the dimension of the vectors */
  public abstract int dimension();

  /**
   * Return the number of vectors for this field.
   *
   * @return the number of vectors returned by this iterator
   */
  public abstract int size();

  @Override
  public final long cost() {
    return size();
  }

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

  /** Sorting VectorValues that iterate over documents in the order of the provided sortMap */
  public static class SortingVectorValues extends VectorValues {
    private final RandomAccessVectorValues randomAccess;
    private final int[] docIdOffsets;
    private int docId = -1;

    SortingVectorValues(VectorValues delegate, Sorter.DocMap sortMap) throws IOException {
      this.randomAccess = ((RandomAccessVectorValues) delegate).copy();
      this.docIdOffsets = new int[sortMap.size()];

      int offset = 1; // 0 means no vector for this (field, document)
      int docID;
      while ((docID = delegate.nextDoc()) != NO_MORE_DOCS) {
        int newDocID = sortMap.oldToNew(docID);
        docIdOffsets[newDocID] = offset++;
      }
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
      return randomAccess.dimension();
    }

    @Override
    public int size() {
      return randomAccess.size();
    }

    @Override
    public int advance(int target) throws IOException {
      throw new UnsupportedOperationException();
    }
  }
}
