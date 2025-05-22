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

/**
 * Abstract implementation of a {@link DocIdSetIterator} that tracks the current doc ID.
 * Implementing {@link DocIdSetIterator} by extending either this class or {@link
 * FilterDocIdSetIterator} is a good idea in order to reduce polymorphism of call sites to {@link
 * DocIdSetIterator#docID()}.
 */
public abstract class AbstractDocIdSetIterator extends DocIdSetIterator {

  /** The current doc ID, initialized at -1. */
  protected int doc = -1;

  /** Sole constructor, invoked by sub-classes. */
  protected AbstractDocIdSetIterator() {}

  @Override
  public final int docID() {
    return doc;
  }
}
