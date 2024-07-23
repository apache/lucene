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
package org.apache.lucene.sandbox.facet.ordinals;

import java.io.IOException;
import org.apache.lucene.internal.hppc.IntHashSet;

/**
 * {@link OrdinalIterator} that filters out ordinals from delegate if they are not in the candidate
 * set.
 *
 * <p>Can be handy to get results only for specific facets.
 */
public class CandidateSetOrdinalIterator implements OrdinalIterator {

  private final IntHashSet candidates;
  private final OrdinalIterator sourceOrds;

  /** Constructor. */
  public CandidateSetOrdinalIterator(OrdinalIterator sourceOrds, int[] candidates) {
    this.candidates = IntHashSet.from(candidates);
    this.sourceOrds = sourceOrds;
  }

  @Override
  public int nextOrd() throws IOException {
    for (int nextOrdinal = sourceOrds.nextOrd(); nextOrdinal != NO_MORE_ORDS; ) {
      if (candidates.contains(nextOrdinal)) {
        return nextOrdinal;
      }
      nextOrdinal = sourceOrds.nextOrd();
    }
    return NO_MORE_ORDS;
  }
}
