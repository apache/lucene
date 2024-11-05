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
package org.apache.lucene.search.join;

import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.join.DocValuesTermsCollector.Function;
import org.apache.lucene.search.join.TermsCollectorManager.TermsCollector;
import org.apache.lucene.search.join.TermsWithScoreCollectorManager.TermsWithScoreCollector;
import org.apache.lucene.util.BytesRefHash;

interface GenericTermsResult {

  BytesRefHash getCollectedTerms();

  float[] getScoresPerTerm();

  static CollectorManager<?, GenericTermsResult> createCollectorMV(
      Function<SortedSetDocValues> mvFunction, ScoreMode mode) {

    return switch (mode) {
      case None -> new TermsCollectorManager<>(() -> new TermsCollector.MV(mvFunction));
      case Avg ->
          new TermsWithScoreCollectorManager<>(
              () -> new TermsWithScoreCollector.MV.MVAvg(mvFunction));
      case Max, Min, Total ->
          new TermsWithScoreCollectorManager<>(
              () -> new TermsWithScoreCollector.MV(mvFunction, mode));
    };
  }

  static CollectorManager<?, GenericTermsResult> createCollectorSV(
      Function<SortedDocValues> svFunction, ScoreMode mode) {

    return switch (mode) {
      case None -> new TermsCollectorManager<>(() -> new TermsCollector.SV(svFunction));
      case Avg ->
          new TermsWithScoreCollectorManager<>(
              () -> new TermsWithScoreCollector.SV.SVAvg(svFunction));
      case Max, Min, Total ->
          new TermsWithScoreCollectorManager<>(
              () -> new TermsWithScoreCollector.SV(svFunction, mode));
    };
  }
}
