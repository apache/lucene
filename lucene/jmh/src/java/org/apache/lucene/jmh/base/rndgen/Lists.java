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
package org.apache.lucene.jmh.base.rndgen;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** The type Lists. */
public final class Lists {

  private Lists() {}

  /**
   * Lists of RndGen.
   *
   * @param <T> the type parameter
   * @param generator the generator
   * @param sizes the sizes
   * @return the RndGen
   */
  static <T> RndGen<List<T>> listsOf(RndGen<T> generator, RndGen<Integer> sizes) {
    return listsOf(generator, arrayList(), sizes).mix(listsOf(generator, linkedList(), sizes));
  }

  /**
   * Array list collector.
   *
   * @param <T> the type parameter
   * @return the collector
   */
  public static <T> Collector<T, List<T>, List<T>> arrayList() {
    return toList(ArrayList::new);
  }

  /**
   * Linked list collector.
   *
   * @param <T> the type parameter
   * @return the collector
   */
  public static <T> Collector<T, List<T>, List<T>> linkedList() {
    return toList(LinkedList::new);
  }

  /**
   * To list collector.
   *
   * @param <T> the type parameter
   * @param <A> the type parameter
   * @param collectionFactory the collection factory
   * @return the collector
   */
  public static <T, A extends List<T>> Collector<T, A, A> toList(Supplier<A> collectionFactory) {
    return Collector.of(
        collectionFactory,
        List::add,
        (left, right) -> {
          left.addAll(right);
          return left;
        });
  }

  /**
   * Lists of RndGen.
   *
   * @param <T> the type parameter
   * @param values the values
   * @param collector the collector
   * @param sizes the sizes
   * @return the RndGen
   */
  static <T> RndGen<List<T>> listsOf(
      RndGen<T> values, Collector<T, List<T>, List<T>> collector, RndGen<Integer> sizes) {

    RndGen<List<T>> gen =
        new RndGen<>() {
          public List<T> gen(RandomnessSource prng) {
            int size = sizes.generate(prng);
            return Stream.generate(() -> values.generate(prng)).limit(size).collect(collector);
          }
        };
    return gen.describedAs(listDescriber(values::asString));
  }

  private static <T> AsString<List<T>> listDescriber(Function<T, String> valueDescriber) {
    return list -> list.stream().map(valueDescriber).collect(Collectors.joining(", ", "[", "]"));
  }
}
