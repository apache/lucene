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

import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** The type Maps. */
public class Maps {

  private Maps() {}

  /**
   * Bounded maps of gen.
   *
   * @param <K> the type parameter
   * @param <V> the type parameter
   * @param kg the kg
   * @param vg the vg
   * @param sizes the sizes
   * @return the gen
   */
  static <K, V> RndGen<Map<K, V>> boundedMapsOf(RndGen<K> kg, RndGen<V> vg, RndGen<Integer> sizes) {
    return mapsOf(kg, vg, defaultMap(), sizes);
  }

  /**
   * Default map collector.
   *
   * @param <K> the type parameter
   * @param <V> the type parameter
   * @return the collector
   */
  public static <K, V> Collector<Map.Entry<K, V>, ?, Map<K, V>> defaultMap() {
    return Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue);
  }

  /**
   * Maps of gen.
   *
   * @param <K> the type parameter
   * @param <V> the type parameter
   * @param kg the kg
   * @param vg the vg
   * @param collector the collector
   * @param sizes the sizes
   * @return the gen
   */
  static <K, V> RndGen<Map<K, V>> mapsOf(
      RndGen<K> kg,
      RndGen<V> vg,
      Collector<Map.Entry<K, V>, ?, Map<K, V>> collector,
      RndGen<Integer> sizes) {
    RndGen<Map<K, V>> gen =
        new RndGen<>() {
          @Override
          public Map<K, V> gen(RandomnessSource prng) {
            int size = sizes.generate(prng);
            return Stream.generate(() -> kg.generate(prng))
                .distinct()
                .map(k -> mapEntry(k, vg.generate(prng)))
                .limit(size)
                .collect(collector);
          }
        };
    return gen.describedAs("Maps Of").describedAs(mapDescriber(kg::asString, vg::asString));
  }

  private static <K, V> AsString<Map<K, V>> mapDescriber(
      Function<K, String> kd, Function<V, String> vd) {
    return list ->
        list.entrySet().stream()
            .map(e -> '(' + kd.apply(e.getKey()) + ',' + vd.apply(e.getValue()) + ')')
            .collect(Collectors.joining(", ", "[", "]"));
  }

  /**
   * Map entry map . entry.
   *
   * @param <K> the type parameter
   * @param <V> the type parameter
   * @param k the k
   * @param v the v
   * @return the map . entry
   */
  static <K, V> Map.Entry<K, V> mapEntry(K k, V v) {
    return Collections.singletonMap(k, v).entrySet().iterator().next();
  }
}
