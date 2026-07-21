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

/** Applies log-odds fusion on leaves where only one of the query's signals can match. */
final class SingletonLogOddsFusionScorer extends FilterScorer {
  private final LogOddsFusionScoreFunction scoreFunction;

  SingletonLogOddsFusionScorer(Scorer scorer, LogOddsFusionScoreFunction scoreFunction) {
    super(scorer);
    this.scoreFunction = scoreFunction;
  }

  @Override
  public float score() throws IOException {
    return scoreFunction.score(scoreFunction.contribution(in.score(), 0));
  }

  @Override
  public int advanceShallow(int target) throws IOException {
    return in.advanceShallow(target);
  }

  @Override
  public float getMaxScore(int upTo) throws IOException {
    return Math.nextUp(scoreFunction.score(scoreFunction.maxContribution(in.getMaxScore(upTo), 0)));
  }
}
