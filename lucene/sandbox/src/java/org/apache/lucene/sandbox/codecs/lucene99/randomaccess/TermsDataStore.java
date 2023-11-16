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

package org.apache.lucene.sandbox.codecs.lucene99.randomaccess;

import java.util.Arrays;

/** Holds all {@link TermData} for all {@link TermType} for a field. */
class TermsDataStore {
  private final TermData[] dataPerTermType;

  private TermsDataStore(TermData[] dataPerTermType) {
    this.dataPerTermType = dataPerTermType;
  }

  static class Builder {
    private final TermData[] dataPerTermType;

    Builder() {
      dataPerTermType = new TermData[TermType.NUM_TOTAL_TYPES];
      Arrays.fill(dataPerTermType, null);
    }

    void add(TermData termData) {
      assert dataPerTermType[termData.termType().getId()] == null;

      dataPerTermType[termData.termType().getId()] = termData;
    }

    TermsDataStore build() {
      return new TermsDataStore(dataPerTermType);
    }
  }
}
