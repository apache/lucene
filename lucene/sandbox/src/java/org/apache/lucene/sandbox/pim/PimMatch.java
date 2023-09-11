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

package org.apache.lucene.sandbox.pim;

import java.util.Objects;
import org.apache.lucene.search.DocIdSetIterator;

/** Class representing the match information returned from the PIM system to the CPU */
public class PimMatch {

  public static final PimMatch UNSET = new PimMatch(-1, 0f);
  public static final PimMatch NO_MORE_RESULTS = new PimMatch(DocIdSetIterator.NO_MORE_DOCS, 0f);

  public int docId;
  public float score;

  public PimMatch(int docId, float score) {
    this.docId = docId;
    this.score = score;
  }

  @Override
  public boolean equals(Object o) {

    if (o == this) return true;
    if (!(o instanceof PimMatch)) {
      return false;
    }
    PimMatch other = (PimMatch) o;
    return (docId == other.docId) && (score == other.score);
  }

  @Override
  public int hashCode() {
    return Objects.hash(docId, score);
  }
}
