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
import org.apache.lucene.internal.hppc.IntArrayList;

/** Iterate over ordinals. */
public interface OrdinalIterator {

  /** This const is returned by nextOrd when there are no more ordinals. */
  int NO_MORE_ORDS = -1;

  /** Returns next ord or {@link #NO_MORE_ORDS}. * */
  int nextOrd() throws IOException;

  /**
   * Convert to int array. Note that after this method is called original OrdinalIterator is
   * exhausted.
   */
  default int[] toArray() throws IOException {
    IntArrayList resultList = new IntArrayList();
    for (int nextOrdinal = this.nextOrd();
        nextOrdinal != NO_MORE_ORDS;
        nextOrdinal = this.nextOrd()) {
      resultList.add(nextOrdinal);
    }
    return resultList.toArray();
  }
}
