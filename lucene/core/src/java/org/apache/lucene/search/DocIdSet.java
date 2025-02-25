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

import org.apache.lucene.util.Accountable;

/**
 * A DocIdSet contains a set of doc ids. Implementing classes must only implement {@link #iterator}
 * to provide access to the set.
 */
public abstract class DocIdSet implements Accountable {

  /** An empty {@code DocIdSet} instance */
  public static final DocIdSet EMPTY =
      new DocIdSet() {

        @Override
        public DocIdSetIterator iterator() {
          return DocIdSetIterator.empty();
        }

        @Override
        public long ramBytesUsed() {
          return 0L;
        }
      };

  /**
   * Provides a {@link DocIdSetIterator} to access the set. This implementation can return <code>
   * null</code> if there are no docs that match.
   */
  public abstract DocIdSetIterator iterator();
}
