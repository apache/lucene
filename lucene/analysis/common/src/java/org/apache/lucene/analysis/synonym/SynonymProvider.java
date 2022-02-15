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

package org.apache.lucene.analysis.synonym;

import java.io.IOException;
import java.util.List;

/**
 * Generic synonym provider
 *
 * @lucene.experimental
 */
public interface SynonymProvider {

  List<WeightedSynonym> getSynonyms(String term) throws IOException;

  /**
   * Term with the associated weight
   *
   * @lucene.experimental
   */
  class WeightedSynonym {
    private final String term;
    private final float weight;

    public WeightedSynonym(String term, float weight) {
      this.term = term;
      this.weight = weight;
    }

    public String getTerm() {
      return term;
    }

    public float getWeight() {
      return weight;
    }

    @Override
    public String toString() {
      return "WeightedSynonym{" + "term='" + term + '\'' + ", weight=" + weight + '}';
    }
  }
}
