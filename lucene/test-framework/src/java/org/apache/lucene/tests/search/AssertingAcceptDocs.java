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
package org.apache.lucene.tests.search;

import java.io.IOException;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;

/** Wraps {@link AcceptDocs} with assertions. */
public final class AssertingAcceptDocs extends AcceptDocs {

  /** Wrap the given {@link AcceptDocs} with assertions. */
  public static AcceptDocs wrap(AcceptDocs acceptDocs) {
    if (acceptDocs instanceof AssertingAcceptDocs assertingAcceptDocs) {
      return assertingAcceptDocs;
    } else {
      return new AssertingAcceptDocs(acceptDocs);
    }
  }

  private final AcceptDocs acceptDocs;
  private final Thread creationThread = Thread.currentThread();

  private AssertingAcceptDocs(AcceptDocs acceptDocs) {
    this.acceptDocs = acceptDocs;
  }

  @Override
  public Bits bits() throws IOException {
    assert Thread.currentThread() == creationThread
        : "Usage of AcceptDocs should be confined to a single thread";
    return acceptDocs.bits();
  }

  @Override
  public DocIdSetIterator iterator() throws IOException {
    assert Thread.currentThread() == creationThread
        : "Usage of AcceptDocs should be confined to a single thread";
    DocIdSetIterator iterator = acceptDocs.iterator();
    assert iterator.docID() == -1 : "Iterator must be unpositioned";
    return iterator;
  }

  @Override
  public int cost() throws IOException {
    assert Thread.currentThread() == creationThread
        : "Usage of AcceptDocs should be confined to a single thread";
    return acceptDocs.cost();
  }
}
