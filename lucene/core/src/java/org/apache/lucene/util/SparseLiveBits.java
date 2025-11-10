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

/**
 * Immutable read-only view of SparseFixedBitSet representing live docs. The underlying bitSet
 * stores DELETED docs, so get() returns the inverse.
 */
record SparseLiveBits(SparseFixedBitSet bitSet) implements Bits {

  @Override
  public boolean get(int index) {
    // bitSet stores deleted docs (1 = deleted), so invert for live docs view
    return !bitSet.get(index);
  }

  // applyMask uses default implementation from Bits interface, which calls get()
  // Since get() returns live docs view (inverted), applyMask will work correctly

  @Override
  public int length() {
    return bitSet.length();
  }
}
