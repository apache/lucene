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

/**
 * The Indri parent scorer that stores the boost so that IndriScorers can use the boost outside of
 * the term.
 */
public abstract class IndriScorer extends Scorer {

  private float boost;

  protected IndriScorer(Weight weight, float boost) {
    super(weight);
    this.boost = boost;
  }

  /**
   * Returns the smoothing score of the current document matching the query. This score is used when
   * the query/term does not appear in the document, and behaves like an idf. The smoothing score is
   * particularly important when the Scorer returns a product of probabilities so that the document
   * score does not go to zero when one probability is zero. This can return 0 or a smoothing score.
   *
   * <p>Smoothing scores are described in many papers, including: Metzler, D. and Croft, W. B. ,
   * "Combining the Language Model and Inference Network Approaches to Retrieval," Information
   * Processing and Management Special Issue on Bayesian Networks and Information Retrieval, 40(5),
   * pp.735-750.
   */
  public abstract float smoothingScore(int docId) throws IOException;

  public float getBoost() {
    return this.boost;
  }
}
