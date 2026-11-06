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

package org.apache.lucene.monitor;

import java.util.HashSet;
import java.util.Set;
import java.util.function.DoubleUnaryOperator;
import org.apache.lucene.index.Term;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestTermWeightor extends LuceneTestCase {

  private static final Term TERM = new Term("f", "term");

  public void testDefaultNegationReadsWeightsAsSurprisal() {
    // weights as -ln p: the rare term is in 1% of documents, the common one in 60%
    TermWeightor weightor = TermWeightor.DEFAULT;
    assertEquals(-Math.log(0.99), weightor.negate(-Math.log(0.01)), 1e-9);
    assertEquals(-Math.log(0.4), weightor.negate(-Math.log(0.6)), 1e-9);
    assertEquals(TermWeightor.MIN_NEGATED_WEIGHT, weightor.negate(1000), 0);
  }

  public void testDefaultWeightorRanksNotSlotsBelowTerms() {
    double minTermWeight = Double.MAX_VALUE;
    Set<Term> allTerms = new HashSet<>();
    for (int length = 1; length <= 40; length++) {
      Term term = new Term("f", "x".repeat(length));
      allTerms.add(term);
      minTermWeight = Math.min(minTermWeight, TermWeightor.DEFAULT.applyAsDouble(term));
      assertTrue(QueryTree.notSlot(Set.of(term), TermWeightor.DEFAULT).weight() < minTermWeight);
    }
    assertTrue(QueryTree.notSlot(allTerms, TermWeightor.DEFAULT).weight() < minTermWeight);
  }

  public void testCombineKeepsTheNegation() {
    DoubleUnaryOperator half = w -> w / 2;
    TermWeightor negating = TermWeightor.lengthWeightor(3, 0.3).withNegation(half);
    TermWeightor field = TermWeightor.fieldWeightor(0.5, "f");

    TermWeightor combined = TermWeightor.combine(field, negating);
    assertEquals(0.5 * negating.applyAsDouble(TERM), combined.applyAsDouble(TERM), 1e-9);
    assertEquals(1.5, combined.negate(3), 0);
    assertEquals(1.5, TermWeightor.combine(combined, field).negate(3), 0);
    assertEquals(1.5, TermWeightor.combine(negating, field.withNegation(half)).negate(3), 0);
    assertEquals(TermWeightor.DEFAULT.negate(3), TermWeightor.combine(field).negate(3), 0);

    TermWeightor conflicting = field.withNegation(w -> w);
    expectThrows(IllegalArgumentException.class, () -> TermWeightor.combine(negating, conflicting));
    TermWeightor chosen = TermWeightor.combineWithNegation(_ -> 4, negating, conflicting);
    assertEquals(combined.applyAsDouble(TERM), chosen.applyAsDouble(TERM), 1e-9);
    assertEquals(4, chosen.negate(3), 0);
  }
}
