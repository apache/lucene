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
package org.apache.lucene.search.similarities;

public class TestDistributionSPL extends DistributionTestCase {

  @Override
  protected Distribution getDistribution() {
    return new DistributionSPL();
  }

  /** Test that DistributionSPL never returns negative scores, even for extreme inputs. */
  public void testNoNegativeScores() {
    DistributionSPL dist = new DistributionSPL();
    BasicStats stats = new BasicStats("field", 1.0);
    stats.setNumberOfDocuments(100);
    stats.setDocFreq(50);
    stats.setTotalTermFreq(500);
    stats.setNumberOfFieldTokens(5000);
    stats.setAvgFieldLength(50);

    // Test lambda values extremely close to 1, which can cause floating-point
    // rounding artifacts that produce -0.0 without the clamping fix.
    for (double lambda :
        new double[] {
          0.99,
          0.999,
          0.9999,
          0.99999,
          0.999999,
          Math.nextDown(1.0),
          Math.nextUp(1.0),
          1.001,
          1.01,
          1.1
        }) {
      for (double tfn : new double[] {1e-10, 1e-5, 0.1, 1, 10, 1e6, 1e10, 1e15}) {
        double score = dist.score(stats, tfn, lambda);
        assertTrue(
            "score must be non-negative for lambda="
                + lambda
                + ", tfn="
                + tfn
                + ", but got "
                + score,
            score >= 0);
      }
    }
  }
}
