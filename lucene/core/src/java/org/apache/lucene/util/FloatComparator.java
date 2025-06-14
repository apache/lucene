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
package org.apache.lucene.util;

import java.util.Comparator;

/** A specialization of {@link Comparator} that compares {@code float} values */
@FunctionalInterface
public interface FloatComparator {

  /** Represents a function that returns a {@code float} result */
  @FunctionalInterface
  interface ToFloatFunction<T> {
    float applyAsFloat(T obj);
  }

  static <T> Comparator<T> comparing(ToFloatFunction<T> function) {
    return (a, b) -> Float.compare(function.applyAsFloat(a), function.applyAsFloat(b));
  }

  /**
   * Float-specialized {@link Comparator#compare}
   *
   * @see java.util.Comparator#compare
   */
  int compare(float f1, float f2);
}
