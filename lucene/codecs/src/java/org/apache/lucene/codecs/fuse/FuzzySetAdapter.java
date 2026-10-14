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
package org.apache.lucene.codecs.fuse;

import org.apache.lucene.codecs.bloom.FuzzySet;
import org.apache.lucene.util.BytesRef;

/** Adapts Lucene's FuzzySet Bloom filter to the MembershipFilter interface. */
public final class FuzzySetAdapter implements MembershipFilter {
  private final FuzzySet set;

  public FuzzySetAdapter(FuzzySet set) {
    this.set = set;
  }

  public FuzzySet getSet() {
    return set;
  }

  @Override
  public boolean contains(BytesRef term) {
    return set.contains(term) == FuzzySet.ContainsResult.MAYBE;
  }

  @Override
  public long ramBytesUsed() {
    return set.ramBytesUsed();
  }
}
