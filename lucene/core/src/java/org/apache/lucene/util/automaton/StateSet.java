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

package org.apache.lucene.util.automaton;

import com.carrotsearch.hppc.BitMixer;
import com.carrotsearch.hppc.IntIntHashMap;
import com.carrotsearch.hppc.cursors.IntCursor;
import java.util.Arrays;

/** A thin wrapper of {@link com.carrotsearch.hppc.IntIntHashMap} */
final class StateSet extends IntSet {

  private final IntIntHashMap inner;
  private int hashCode;
  private boolean changed;
  private int[] arrayCache = new int[0];

  StateSet(int capacity) {
    inner = new IntIntHashMap(capacity);
  }

  // Adds this state to the set
  void incr(int num) {
    if (inner.addTo(num, 1) == 1) {
      changed = true;
    }
  }

  // Removes this state from the set, if count decrs to 0
  void decr(int num) {
    assert inner.containsKey(num);
    int keyIndex = inner.indexOf(num);
    int count = inner.indexGet(keyIndex) - 1;
    if (count == 0) {
      inner.remove(num);
      changed = true;
    } else {
      inner.indexReplace(keyIndex, count);
    }
  }

  void computeHash() {
    if (changed == false) {
      return;
    }
    hashCode = inner.size();
    for (IntCursor cursor : inner.keys()) {
      hashCode += BitMixer.mix(cursor.value);
    }
  }

  /**
   * Create a snapshot of this int set associated with a given state. The snapshot will not retain
   * any frequency information about the elements of this set, only existence.
   *
   * <p>It is the caller's responsibility to ensure that the hashCode and data are up to date via
   * the {@link #computeHash()} method before calling this method.
   *
   * @param state the state to associate with the frozen set.
   * @return A new FrozenIntSet with the same values as this set.
   */
  FrozenIntSet freeze(int state) {
    if (changed == false) {
      assert arrayCache != null;
      return new FrozenIntSet(arrayCache, hashCode, state);
    }
    return new FrozenIntSet(getArray(), hashCode, state);
  }

  @Override
  int[] getArray() {
    if (changed == false) {
      assert arrayCache != null;
      return arrayCache;
    }
    changed = false;
    arrayCache = inner.keys().toArray();
    // we need to sort this array since "equals" method depend on this
    Arrays.sort(arrayCache);
    return arrayCache;
  }

  @Override
  int size() {
    return inner.size();
  }

  @Override
  public int hashCode() {
    return hashCode;
  }
}
