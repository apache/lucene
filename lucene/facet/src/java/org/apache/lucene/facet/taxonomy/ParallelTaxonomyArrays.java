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
package org.apache.lucene.facet.taxonomy;

/**
 * Returns 3 arrays for traversing the taxonomy:
 *
 * <ul>
 *   <li>{@code parents}: {@code parents[i]} denotes the parent of category ordinal {@code i}.
 *   <li>{@code children}: {@code children[i]} denotes a child of category ordinal {@code i}.
 *   <li>{@code siblings}: {@code siblings[i]} denotes the sibling of category ordinal {@code i}.
 * </ul>
 *
 * To traverse the taxonomy tree, you typically start with {@code children[0]} (ordinal 0 is
 * reserved for ROOT), and then depends if you want to do DFS or BFS, you call {@code
 * children[children[0]]} or {@code siblings[children[0]]} and so forth, respectively.
 *
 * <p><b>NOTE:</b> you are not expected to modify the values of the arrays, since the arrays are
 * shared with other threads.
 *
 * @lucene.experimental
 */
public abstract class ParallelTaxonomyArrays {
  /** Abstraction that looks like an int[], but read-only. */
  public abstract static class IntArray {
    /** Sole constructor * */
    public IntArray() {}

    /**
     * Equivalent to array[i].
     *
     * @param i the index of the value to retrieve
     * @return the value at position i
     */
    public abstract int get(int i);

    /**
     * Equivalent to array.length.
     *
     * @return the allocated size of the array
     */
    public abstract int length();
  }

  /** Sole constructor. */
  public ParallelTaxonomyArrays() {}

  /**
   * Returns the parents array, where {@code parents[i]} denotes the parent of category ordinal
   * {@code i}.
   */
  public abstract IntArray parents();

  /**
   * Returns the children array, where {@code children[i]} denotes a child of category ordinal
   * {@code i}.
   */
  public abstract IntArray children();

  /**
   * Returns the siblings array, where {@code siblings[i]} denotes the sibling of category ordinal
   * {@code i}.
   */
  public abstract IntArray siblings();
}
