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
 * Half-open doc id interval {@code [minDoc, maxDoc)} used to narrow bulk scoring when a filter
 * aligns with the primary index sort.
 *
 * @lucene.experimental
 */
public record DocIdRange(int minDoc, int maxDoc) {
  public DocIdRange {
    if (minDoc < 0 || maxDoc < minDoc) {
      throw new IllegalArgumentException("Invalid doc ID range [" + minDoc + ", " + maxDoc + ")");
    }
  }

  /** True when {@code minDoc == maxDoc} (no documents in {@code [minDoc, maxDoc)}). */
  public boolean isEmpty() {
    return minDoc == maxDoc;
  }
}
