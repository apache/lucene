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

package org.apache.lucene.util.hnsw;

/**
 * A collection of objects that is threadsafe, providing offer(T) that tries to add an element and
 * poll() that removes and returns an element or null. The storage will never grow. There are no
 * guarantees about which object will be returned from poll(), just that it will be one that was
 * added by offer().
 */
public class Bag<T> {
  private static final int DEFAULT_CAPACITY = 64;

  private final Object[] elements;
  private int writeTo;
  private int readFrom;

  public Bag() {
    this(DEFAULT_CAPACITY);
  }

  public Bag(int capacity) {
    elements = new Object[capacity];
  }

  public synchronized boolean offer(T element) {
    if (full()) {
      return false;
    }
    elements[writeTo] = element;
    writeTo = (writeTo + 1) % elements.length;
    return true;
  }

  @SuppressWarnings("unchecked")
  public synchronized T poll() {
    if (empty()) {
      return null;
    }
    T result = (T) elements[readFrom];
    readFrom = (readFrom + 1) % elements.length;
    return result;
  }

  private boolean full() {
    int headroom = readFrom - 1 - writeTo;
    if (headroom < 0) {
      headroom += elements.length;
    }
    return headroom == 0;
  }

  private boolean empty() {
    return readFrom == writeTo;
  }
}
