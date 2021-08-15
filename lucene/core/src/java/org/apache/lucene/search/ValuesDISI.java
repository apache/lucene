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

import java.io.IOException;
import java.util.List;
import java.util.Objects;

class ValuesDISI extends DocIdSetIterator {
  private final DocIdSetIterator lead;
  private final List<? extends Values> values;

  ValuesDISI(DocIdSetIterator lead, List<? extends Values> values) {
    Objects.requireNonNull(lead);
    Objects.requireNonNull(values);
    this.lead = lead;
    this.values = values;
  }

  @Override
  public int docID() {
    return lead.docID();
  }

  @Override
  public int nextDoc() throws IOException {
    return doNext(lead.nextDoc());
  }

  @Override
  public int advance(int target) throws IOException {
    return doNext(lead.advance(target));
  }

  @Override
  public long cost() {
    return lead.cost();
  }

  private int doNext(int target) throws IOException {
    advanceHead:
    while (target != NO_MORE_DOCS) {
      assert target == lead.docID();

      for (Values v : values) {
        if (v.advanceExact(target) == false) {
          target = lead.nextDoc();
          continue advanceHead;
        }
      }

      return target;
    }

    return NO_MORE_DOCS;
  }
}
