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
package org.apache.lucene.sandbox.facet.iterators;

/**
 * Generates {@link Comparable} for provided ordinal. For example, it can be used to find topN facet
 * ordinals.
 *
 * @param <T> something ordinals can be compared by.
 * @lucene.experimental
 */
public interface ComparableSupplier<T extends Comparable<T>> {

  /**
   * For given ordinal, get something it can be compared by.
   *
   * @param ord ordinal.
   * @param reuse object to reuse for building result. Must not be null.
   */
  void reuseComparable(int ord, T reuse);

  /**
   * For given ordinal, create something it can be compared by.
   *
   * @param ord ordinal.
   * @return Comparable.
   */
  T createComparable(int ord);
}
