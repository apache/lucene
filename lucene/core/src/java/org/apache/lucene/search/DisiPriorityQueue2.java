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

/** {@link DisiPriorityQueue} of two entries or less. */
final class DisiPriorityQueue2 extends DisiPriorityQueue {

  private DisiWrapper top, top2;

  @Override
  public Iterator<DisiWrapper> iterator() {
    if (top2 != null) {
      return Arrays.asList(top, top2).iterator();
    } else if (top != null) {
      return Collections.singleton(top).iterator();
    } else {
      return Collections.emptyIterator();
    }
  }

  @Override
  public int size() {
    return top2 == null ? (top == null ? 0 : 1) : 2;
  }

  @Override
  public DisiWrapper top() {
    return top;
  }

  @Override
  public DisiWrapper top2() {
    return top2;
  }

  @Override
  public DisiWrapper topList() {
    DisiWrapper topList = null;
    if (top != null) {
      top.next = null;
      topList = top;
      if (top2 != null && top.doc == top2.doc) {
        top2.next = topList;
        topList = top2;
      }
    }
    return topList;
  }

  @Override
  public DisiWrapper add(DisiWrapper entry) {
    if (top == null) {
      return top = entry;
    } else if (top2 == null) {
      top2 = entry;
      return updateTop();
    } else {
      throw new IllegalStateException(
          "Trying to add a 3rd element to a DisiPriorityQueue configured with a max size of 2");
    }
  }

  @Override
  public DisiWrapper pop() {
    DisiWrapper ret = top;
    top = top2;
    top2 = null;
    return ret;
  }

  @Override
  public DisiWrapper updateTop() {
    if (top2 != null && top2.doc < top.doc) {
      DisiWrapper tmp = top;
      top = top2;
      top2 = tmp;
    }
    return top;
  }

  @Override
  DisiWrapper updateTop(DisiWrapper topReplacement) {
    top = topReplacement;
    return updateTop();
  }

  @Override
  public void clear() {
    top = null;
    top2 = null;
  }
}
