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

package org.apache.lucene.analysis.synonym.word2vec;

import java.io.IOException;
import java.util.List;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.TermAndBoost;

/**
 * Generic synonym provider
 *
 * @lucene.experimental
 */
public interface SynonymProvider {

  /**
   * SynonymProvider constructor
   *
   * @param term we want to find the synonyms
   * @param maxSynonymsPerTerm maximum number of result returned by the synonym search
   * @param minAcceptedSimilarity minimal value of cosine similarity between the searched vector and
   *     the retrieved ones
   */
  List<TermAndBoost> getSynonyms(BytesRef term, int maxSynonymsPerTerm, float minAcceptedSimilarity)
      throws IOException;
}
