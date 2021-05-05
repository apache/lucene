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

import static org.apache.lucene.util.VectorUtil.dotProduct;
import static org.apache.lucene.util.VectorUtil.squareDistance;

import java.io.IOException;
import org.apache.lucene.codecs.VectorReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;

/**
 * This class provides access to per-document floating point vector values indexed as {@link
 * org.apache.lucene.document.VectorField}.
 *
 * @lucene.experimental
 */
public abstract class VectorValues extends DocIdSetIterator {

  /** The maximum length of a vector */
  public static int MAX_DIMENSIONS = 1024;

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

  /** Return the similarity function used to compare these vectors */
  public abstract SimilarityFunction similarityFunction();

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
   * Vector similarity function; used in search to return top K most similar vectors to a target
   * vector. This is a label describing the method used during indexing and searching of the vectors
   * in order to determine the nearest neighbors.
   */
  public enum SimilarityFunction {

    /**
     * No similarity function is provided. Note: {@link VectorReader#search(String, float[], int,
     * int)} is not supported for fields specifying this.
     */
    NONE,

    /** HNSW graph built using Euclidean distance */
    EUCLIDEAN(true),

    /** HNSW graph buit using dot product */
    DOT_PRODUCT;

    /**
     * If true, the scores associated with vector comparisons are in reverse order; that is, lower
     * scores represent more similar vectors. Otherwise, if false, higher scores represent more
     * similar vectors.
     */
    public final boolean reversed;

    SimilarityFunction(boolean reversed) {
      this.reversed = reversed;
    }

    SimilarityFunction() {
      reversed = false;
    }

    /**
     * Calculates a similarity score between the two vectors with a specified function.
     *
     * @param v1 a vector
     * @param v2 another vector, of the same dimension
     * @return the value of the similarity function applied to the two vectors
     */
    public float compare(float[] v1, float[] v2) {
      switch (this) {
        case EUCLIDEAN:
          return squareDistance(v1, v2);
        case DOT_PRODUCT:
          return dotProduct(v1, v2);
        case NONE:
        default:
          throw new IllegalStateException("Incomparable similarity function: " + this);
      }
    }
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
        public SimilarityFunction similarityFunction() {
          return SimilarityFunction.NONE;
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
}
