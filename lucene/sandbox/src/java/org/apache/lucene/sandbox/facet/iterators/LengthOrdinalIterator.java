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
package org.apache.lucene.sandbox.facet.iterators;

import java.io.IOException;

/**
 * {@link OrdinalIterator} passes through all ordinals as is and counts them.
 *
 * @lucene.experimental
 */
public final class LengthOrdinalIterator implements OrdinalIterator {

  private final OrdinalIterator delegate;
  private boolean complete;
  private int length;

  /** Constructor. */
  public LengthOrdinalIterator(OrdinalIterator delegate) throws IOException {
    this.delegate = delegate;
  }

  /** Number of ordinals in the delegate. */
  public int length() {
    if (complete) {
      return length;
    } else {
      throw new IllegalStateException(
          "You can't request count before the ordinal iterator was consumed");
    }
  }

  @Override
  public int nextOrd() throws IOException {
    int ord = delegate.nextOrd();
    if (ord == NO_MORE_ORDS) {
      complete = true;
    } else {
      length++;
    }
    return ord;
  }
}
