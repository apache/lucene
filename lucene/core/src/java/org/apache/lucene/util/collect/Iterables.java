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
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

public final class Iterables {
  private Iterables() {}

  /**
   * Returns a view containing the result of applying {@code function} to each element of {@code
   * fromIterable}.
   *
   * <p>The returned iterable's iterator supports {@code remove()} if {@code fromIterable}'s
   * iterator does. After a successful {@code remove()} call, {@code fromIterable} no longer
   * contains the corresponding element.
   *
   * <p><b>{@code Stream} equivalent:</b> {@link Stream#map}
   */
  public static <F, T> Iterable<T> transform(
      Iterable<F> fromIterable, Function<? super F, ? extends T> function) {
    return new Iterable<>() {
      @Override
      public Iterator<T> iterator() {
        return Iterators.transform(fromIterable.iterator(), function);
      }

      @Override
      public void forEach(Consumer<? super T> action) {
        fromIterable.forEach((F f) -> action.accept(function.apply(f)));
      }

      @Override
      public Spliterator<T> spliterator() {
        return CollectSpliterators.map(fromIterable.spliterator(), function);
      }
    };
  }
}
