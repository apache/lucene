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
import java.util.function.BiConsumer;
import java.util.function.DoubleUnaryOperator;
import org.apache.lucene.index.Term;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

public class TestQueryTree extends LuceneTestCase {

  private static final Term A = new Term("f", "a");
  private static final Term B = new Term("f", "b");
  private static final Term C = new Term("g", "c");

  private static double negativeDisjunctWeight(TermWeightor weightor, Term... terms) {
    return QueryTree.negativeDisjunct(Set.of(terms), weightor).weight();
  }

  public void testNegativeDisjunctLeaf() {
    QueryTree negativeDisjunct = QueryTree.negativeDisjunct(Set.of(C, B, A), 0.25);
    assertEquals(0.25, negativeDisjunct.weight(), 0);
    assertFalse(negativeDisjunct.advancePhase(0));
    assertEquals("NegativeDisjunct[f:a, f:b, g:c]^0.25", negativeDisjunct.toString());
    expectThrows(IllegalArgumentException.class, () -> QueryTree.negativeDisjunct(Set.of(), 1));
    expectThrows(
        IllegalArgumentException.class, () -> QueryTree.negativeDisjunct(Set.of(A), 0).weight());
  }

  public void testCollectorsWithoutNegativeDisjunctSupportGetTheAnyToken() {
    Term any = new Term(TermFilteredPresearcher.ANYTOKEN_FIELD, TermFilteredPresearcher.ANYTOKEN);
    QueryTree tree =
        QueryTree.conjunction(QueryTree.negativeDisjunct(Set.of(A), 2), QueryTree.term(B, 1));
    Set<Term> terms = new HashSet<>();
    tree.collectTerms((BiConsumer<String, BytesRef>) (f, b) -> terms.add(new Term(f, b)));
    assertEquals(Set.of(any), terms);

    terms.clear();
    tree.collectTerms((QueryTree.TermCollector) (f, b) -> terms.add(new Term(f, b)));
    assertEquals(Set.of(any), terms);
  }

  public void testNegativeDisjunctWeightIsTheMaxNegatedWeight() {
    TermWeightor weights = t -> t.equals(A) ? 0.2 : t.equals(B) ? 0.7 : 0.1;
    TermWeightor weightor = weights.withNegation(w -> w);
    assertEquals(0.2, negativeDisjunctWeight(weightor, A), 0);
    assertEquals(0.7, negativeDisjunctWeight(weightor, A, B, C), 0);
  }

  public void testDefaultNegationReadsWeightsAsSurprisal() {
    // weights as -ln p: A is in 1% of documents, B in 60%
    TermWeightor weightor = t -> t.equals(A) ? -Math.log(0.01) : -Math.log(0.6);
    assertEquals(-Math.log(0.99), negativeDisjunctWeight(weightor, A), 1e-9);
    assertEquals(-Math.log(0.4), negativeDisjunctWeight(weightor, B), 1e-9);
    assertEquals(TermWeightor.MIN_NEGATED_WEIGHT, negativeDisjunctWeight(_ -> 1000, A), 0);
  }

  public void testNegationAppliesToTheCombinedWeight() {
    // The field weightor halves the weight of A, and the negation halves it again.
    double half = 0.5;
    DoubleUnaryOperator halve = w -> w * half;
    TermWeightor length = TermWeightor.lengthWeightor(3, 0.3);
    TermWeightor halvingField = TermWeightor.fieldWeightor(half, "f");
    double halvedOnce = length.applyAsDouble(A) * half;
    double halvedTwice = halvedOnce * half;

    TermWeightor combined = TermWeightor.combine(halvingField, length).withNegation(halve);
    assertEquals(halvedOnce, combined.applyAsDouble(A), 0);
    assertEquals(halvedTwice, negativeDisjunctWeight(combined, A), 0);
    // without a negation, the default one applies to the combined weight
    assertEquals(
        TermWeightor.DEFAULT.negate(half),
        negativeDisjunctWeight(TermWeightor.combine(halvingField), A),
        0);

    expectThrows(
        IllegalArgumentException.class,
        () -> TermWeightor.combine(halvingField, length.withNegation(halve)));
  }
}
