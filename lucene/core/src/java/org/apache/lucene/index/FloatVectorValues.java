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
import java.util.List;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.search.VectorScorer;

/**
 * This class provides access to per-document floating point vector values indexed as {@link
 * KnnFloatVectorField}.
 *
 * @lucene.experimental
 */
public abstract class FloatVectorValues extends KnnVectorValues {

  /** Sole constructor */
  protected FloatVectorValues() {}

  /** A random access (lookup by ord) provider of the vector values */
  public abstract static class Floats implements AutoCloseable {
    /**
     * Return the vector value for the given vector ordinal which must be in [0, size() - 1],
     * otherwise IndexOutOfBoundsException is thrown. The returned array may be shared across calls.
     *
     * @return the vector value
     */
    public abstract float[] get(int ord) throws IOException;

    @Override
    public void close() throws IOException {
      // by default do nothing. Some implementations do more interesting resource management.
    }

    /** A Floats containing no vectors. Throws UnsupportedOperationException if get() is called. */
    public static final Floats EMPTY =
        new Floats() {
          @Override
          public float[] get(int ord) {
            throw new UnsupportedOperationException();
          }
        };
  }

  /** Returns a random access (lookup by ord) provider of the vector values */
  public abstract Floats vectors() throws IOException;

  /**
   * Checks the Vector Encoding of a field
   *
   * @throws IllegalStateException if {@code field} has vectors, but using a different encoding
   * @lucene.internal
   * @lucene.experimental
   */
  public static void checkField(LeafReader in, String field) {
    FieldInfo fi = in.getFieldInfos().fieldInfo(field);
    if (fi != null && fi.hasVectorValues() && fi.getVectorEncoding() != VectorEncoding.FLOAT32) {
      throw new IllegalStateException(
          "Unexpected vector encoding ("
              + fi.getVectorEncoding()
              + ") for field "
              + field
              + "(expected="
              + VectorEncoding.FLOAT32
              + ")");
    }
  }

  /**
   * Return a {@link VectorScorer} for the given query vector and the current {@link
   * FloatVectorValues}.
   *
   * @param target the query vector
   * @return a {@link VectorScorer} instance or null
   */
  public VectorScorer scorer(float[] target) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public VectorEncoding getEncoding() {
    return VectorEncoding.FLOAT32;
  }

  /**
   * Creates a {@link FloatVectorValues} from a list of float arrays.
   *
   * @param vectors the list of float arrays
   * @param dim the dimension of the vectors
   * @return a {@link FloatVectorValues} instance
   */
  public static FloatVectorValues fromFloats(List<float[]> vectors, int dim) {
    return new FloatVectorValues() {
      @Override
      public int size() {
        return vectors.size();
      }

      @Override
      public int dimension() {
        return dim;
      }

      @Override
      public Floats vectors() {
        return new Floats() {
          @Override
          public float[] get(int ord) throws IOException {
            return vectors.get(ord);
          }

          @Override
          public void close() {}
        };
      }

      @Override
      public DocIndexIterator iterator() {
        return createDenseIterator();
      }
    };
  }
}
