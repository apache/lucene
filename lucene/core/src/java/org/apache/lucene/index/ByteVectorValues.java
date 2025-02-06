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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.lucene.codecs.lucene99.MultiVectorOrdConfiguration;
import org.apache.lucene.codecs.lucene99.MultiVectorOrdConfiguration.MultiVectorMaps;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.util.hnsw.IntToIntFunction;

/**
 * This class provides access to per-document floating point vector values indexed as {@link
 * KnnByteVectorField}.
 *
 * @lucene.experimental
 */
public abstract class ByteVectorValues extends KnnVectorValues {

  /** Sole constructor */
  protected ByteVectorValues() {}

  /**
   * Return the vector value for the given vector ordinal which must be in [0, size() - 1],
   * otherwise IndexOutOfBoundsException is thrown. The returned array may be shared across calls.
   *
   * @return the vector value
   */
  public abstract byte[] vectorValue(int ord) throws IOException;

  /** Returns all vector values indexed for the document corresponding to provided ordinal */
  public List<byte[]> allVectorValues(int ord) throws IOException {
    int baseOrd = baseOrd(ord);
    int count = vectorCount(ord);
    List<byte[]> result = new ArrayList<byte[]>(count);
    for (int i = 0; i < count; i++) {
      result.add(vectorValue(baseOrd + i));
    }
    return result;
  }

  /**
   * Returns an iterator for multi-vector values, when base ordinal and count are provided. This is
   * useful when fetching all vector values from {@link KnnVectorValues#iterator()}
   */
  public Iterator<byte[]> allVectorValues(int baseOrd, int ordCount) throws IOException {
    return new Iterator<>() {
      int ord = baseOrd;
      int count = ordCount;

      @Override
      public boolean hasNext() {
        return count > 0;
      }

      @Override
      public byte[] next() {
        byte[] v = null;
        try {
          v = vectorValue(ord);
        } catch (IOException e) {
          throw new RuntimeException(e);
        } finally {
          ord++;
          count--;
        }
        return v;
      }
    };
  }

  @Override
  public abstract ByteVectorValues copy() throws IOException;

  /**
   * Checks the Vector Encoding of a field
   *
   * @throws IllegalStateException if {@code field} has vectors, but using a different encoding
   * @lucene.internal
   * @lucene.experimental
   */
  public static void checkField(LeafReader in, String field) {
    FieldInfo fi = in.getFieldInfos().fieldInfo(field);
    if (fi != null && fi.hasVectorValues() && fi.getVectorEncoding() != VectorEncoding.BYTE) {
      throw new IllegalStateException(
          "Unexpected vector encoding ("
              + fi.getVectorEncoding()
              + ") for field "
              + field
              + "(expected="
              + VectorEncoding.BYTE
              + ")");
    }
  }

  /**
   * Return a {@link VectorScorer} for the given query vector.
   *
   * @param query the query vector
   * @return a {@link VectorScorer} instance or null
   */
  public VectorScorer scorer(byte[] query) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public VectorEncoding getEncoding() {
    return VectorEncoding.BYTE;
  }

  /**
   * Creates a {@link ByteVectorValues} from a list of byte arrays.
   *
   * @param vectors the list of byte arrays
   * @param dim the dimension of the vectors
   * @param docsWithFieldSet the set of all docIds for provided vectors
   * @param docIdToVectorCount maps docId to number of vectors per document
   * @return a {@link ByteVectorValues} instance
   */
  public static ByteVectorValues fromBytes(
      List<byte[]> vectors,
      int dim,
      DocsWithFieldSet docsWithFieldSet,
      IntToIntFunction docIdToVectorCount) {
    return new ByteVectorValues() {
      int cachedDocCount = -1;
      MultiVectorMaps mvMaps = null;

      private void computeMultiVectorMaps() {
        // TODO: optimize using binary search instead of full maps
        try {
          mvMaps =
              MultiVectorOrdConfiguration.createMultiVectorMaps(
                  docsWithFieldSet.iterator(), docIdToVectorCount, size(), docCount());
        } catch (IOException e) {
          throw new IllegalStateException(
              "Unexpected IOException on creating FloatVectorValues from provided vectors:" + e);
        }
        cachedDocCount = docCount();
      }

      @Override
      public int size() {
        return vectors.size();
      }

      @Override
      public int dimension() {
        return dim;
      }

      @Override
      public byte[] vectorValue(int targetOrd) {
        return vectors.get(targetOrd);
      }

      @Override
      public int docCount() {
        return docsWithFieldSet.cardinality();
      }

      @Override
      public int ordToDoc(int ord) {
        if (docCount() == size()) {
          return ord;
        }
        if (cachedDocCount != docCount()) {
          computeMultiVectorMaps();
        }
        return mvMaps.ordToDocMap()[ord];
      }

      @Override
      public int baseOrd(int ord) {
        if (docCount() == size()) {
          return ord;
        }
        if (cachedDocCount != docCount()) {
          computeMultiVectorMaps();
        }
        return mvMaps.baseOrdMap()[ord];
      }

      @Override
      public int vectorCount(int ord) {
        if (docCount() == size()) {
          return 1;
        }
        return docIdToVectorCount.apply(ord);
      }

      @Override
      public int docIndexToBaseOrd(int index) {
        if (docCount() == size()) {
          return index;
        }
        if (cachedDocCount != docCount()) {
          computeMultiVectorMaps();
        }
        return mvMaps.docOrdFreq()[index];
      }

      @Override
      public ByteVectorValues copy() {
        return this;
      }

      @Override
      public DocIndexIterator iterator() {
        return multiValueWrappedIterator(createDenseIterator());
      }
    };
  }
}
