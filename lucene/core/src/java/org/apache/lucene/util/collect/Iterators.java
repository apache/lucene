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

package org.apache.lucene.util.collect;

import java.util.Iterator;
import java.util.function.Function;

/**
 * This class only contains a static method that return an object of type {@code Iterator} for now.
 * We can implement other method if necessary.
 *
 * @lucene.experimental
 */
public final class Iterators {
  private Iterators() {}

  /**
   * Returns a view containing the result of applying {@code function} to each element of {@code
   * fromIterator}.
   *
   * <p>The returned iterator supports {@code remove()} if {@code fromIterator} does. After a
   * successful {@code remove()} call, {@code fromIterator} no longer contains the corresponding
   * element.
   */
  public static <F, T> Iterator<T> transform(
      Iterator<F> fromIterator, Function<? super F, ? extends T> function) {
    return new TransformedIterator<>(fromIterator) {
      @Override
      T transform(F from) {
        return function.apply(from);
      }
    };
  }
}
