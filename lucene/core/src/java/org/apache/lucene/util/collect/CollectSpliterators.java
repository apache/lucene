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

import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Function;

final class CollectSpliterators {
  private CollectSpliterators() {}

  /**
   * Returns a {@code Spliterator} over the elements of {@code fromSpliterator} mapped by {@code
   * function}.
   */
  static <InElementT, OutElementT> Spliterator<OutElementT> map(
      Spliterator<InElementT> fromSpliterator,
      Function<? super InElementT, ? extends OutElementT> function) {
    return new Spliterator<>() {

      @Override
      public boolean tryAdvance(Consumer<? super OutElementT> action) {
        return fromSpliterator.tryAdvance(
            fromElement -> action.accept(function.apply(fromElement)));
      }

      @Override
      public void forEachRemaining(Consumer<? super OutElementT> action) {
        fromSpliterator.forEachRemaining(fromElement -> action.accept(function.apply(fromElement)));
      }

      @Override
      public Spliterator<OutElementT> trySplit() {
        Spliterator<InElementT> fromSplit = fromSpliterator.trySplit();
        return (fromSplit != null) ? map(fromSplit, function) : null;
      }

      @Override
      public long estimateSize() {
        return fromSpliterator.estimateSize();
      }

      @Override
      public int characteristics() {
        return fromSpliterator.characteristics()
            & ~(Spliterator.DISTINCT | Spliterator.NONNULL | Spliterator.SORTED);
      }
    };
  }
}
