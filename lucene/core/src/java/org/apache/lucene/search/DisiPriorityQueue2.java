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
package org.apache.lucene.search;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

/**
 * Specialization of {@link DisiPriorityQueue} for up to 2 entries.
 */
final class DisiPriorityQueue2 extends DisiPriorityQueue {

  private DisiWrapper top, other;
  private int size;

  public DisiPriorityQueue2() {
    size = 0;
  }

  private void rebalance() {
    if (other != null && other.doc < top.doc) {
      DisiWrapper tmp = other;
      other = top;
      top = tmp;
    }
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public DisiWrapper top() {
    return top;
  }

  @Override
  public DisiWrapper topList() {
    if (other != null && other.doc == top.doc) {
      other.next = null;
      top.next = other;
    } else {
      top.next = null;
    }
    return top;
  }

  @Override
  public DisiWrapper add(DisiWrapper entry) {
    assert entry != null;
    assert size <= 1;
    if (top == null) {
      top = entry;
    } else {
      other = entry;
      rebalance();
    }
    ++size;
    return top;
  }

  @Override
  public void addAll(DisiWrapper[] entries, int offset, int len) {
    for (int i = 0; i < len; ++i) {
      add(entries[offset + i]);
    }
  }

  @Override
  public DisiWrapper pop() {
    assert size > 0;
    DisiWrapper result = top;
    top = other;
    other = null;
    --size;
    return result;
  }

  @Override
  public DisiWrapper updateTop() {
    rebalance();
    return top;
  }

  @Override
  DisiWrapper updateTop(DisiWrapper topReplacement) {
    top = topReplacement;
    return updateTop();
  }

  @Override
  /** Clear the heap. */
  public void clear() {
    top = other = null;
    size = 0;
  }

  @Override
  public Iterator<DisiWrapper> iterator() {
    if (size == 0) {
      return Collections.emptyIterator();
    } else if (size == 1) {
      return Collections.singleton(top).iterator();
    } else {
      return Arrays.asList(top, other).iterator();
    }
  }

}
