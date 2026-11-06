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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

public class TestQueryTree extends LuceneTestCase {

  private static final Term A = new Term("f", "a");
  private static final Term B = new Term("f", "b");
  private static final Term C = new Term("g", "c");

  /** Collects terms with the old collector API */
  private static Set<Term> terms(QueryTree tree) {
    Set<Term> terms = new HashSet<>();
    tree.collectTerms((BiConsumer<String, BytesRef>) (f, b) -> terms.add(new Term(f, b)));
    return terms;
  }

  /** Collects terms and NotSlots */
  private static List<Set<Term>> notSlots(QueryTree tree, Set<Term> terms) {
    List<Set<Term>> notSlots = new ArrayList<>();
    tree.collectTerms(
        new QueryTree.TermCollector() {
          @Override
          public void accept(String field, BytesRef term) {
            terms.add(new Term(field, BytesRef.deepCopyOf(term)));
          }

          @Override
          public void notSlot(Set<Term> slotTerms) {
            notSlots.add(slotTerms);
          }
        });
    return notSlots;
  }

  public void testNotSlotsPassThroughConjunctionsAndDisjunctions() {
    QueryTree notSlot = QueryTree.notSlot(Set.of(A, B), 0.5);
    QueryTree tree =
        QueryTree.conjunction(
            QueryTree.disjunction(notSlot, QueryTree.term(C, 1)), QueryTree.term(A, 0.1));

    Set<Term> terms = new HashSet<>();
    List<Set<Term>> notSlots = notSlots(tree, terms);
    assertEquals(List.of(Set.of(A, B)), notSlots);
    assertEquals(Set.of(C), terms);
  }

  public void testCollectorWithoutNotSlotSupportGetsAnyToken() {
    Term any = new Term(TermFilteredPresearcher.ANYTOKEN_FIELD, TermFilteredPresearcher.ANYTOKEN);
    QueryTree tree = QueryTree.conjunction(QueryTree.notSlot(Set.of(A), 2), QueryTree.term(B, 1));
    assertEquals(Set.of(any), terms(tree));

    // a TermCollector that doesn't override notSlot gets the ANYTOKEN too
    Set<Term> terms = new HashSet<>();
    tree.collectTerms(
        (QueryTree.TermCollector) (f, b) -> terms.add(new Term(f, BytesRef.deepCopyOf(b))));
    assertEquals(Set.of(any), terms);
  }

  public void testNotSlotLeaf() {
    QueryTree notSlot = QueryTree.notSlot(Set.of(C, B, A), 0.25);
    assertEquals(0.25, notSlot.weight(), 0);
    assertFalse(notSlot.advancePhase(0));
    assertEquals("NotSlot[f:a, f:b, g:c]^0.25", notSlot.toString());

    expectThrows(IllegalArgumentException.class, () -> QueryTree.notSlot(Set.of(), 1));
    expectThrows(IllegalArgumentException.class, () -> QueryTree.notSlot(Set.of(A), 0).weight());
  }

  public void testNotSlotWeightIsTheMaxNegatedWeight() {
    TermWeightor weights = t -> t.equals(A) ? 0.2 : t.equals(B) ? 0.7 : 0.1;
    TermWeightor weightor = weights.withNegation(w -> w);
    assertEquals(0.2, QueryTree.notSlot(Set.of(A), weightor).weight(), 0);
    assertEquals(0.7, QueryTree.notSlot(Set.of(A, B), weightor).weight(), 0);
    assertEquals(0.7, QueryTree.notSlot(Set.of(A, B, C), weightor).weight(), 0);
  }

  public void testCustomTreesCollectTheSameTermsThroughBothApis() {
    RegexpQueryHandler handler = new RegexpQueryHandler();
    QueryTree tree =
        handler.handleQuery(new RegexpQuery(new Term("f", "hell.*o")), TermWeightor.DEFAULT);
    Set<Term> viaTermCollector = new HashSet<>();
    assertEquals(List.of(), notSlots(tree, viaTermCollector));
    assertEquals(terms(tree), viaTermCollector);
    assertEquals(2, viaTermCollector.size());
  }
}
