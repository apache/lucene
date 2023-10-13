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

package org.apache.lucene.queries.spans;

import java.io.IOException;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.tests.search.MatchesTestBase;

public class TestSpanMatches extends MatchesTestBase {

  @Override
  protected String[] getDocuments() {
    return new String[] {
      "w1 w2 w3 w4 w5",
      "w1 w3 w2 w3 zz",
      "w1 xx w2 yy w4",
      "w1 w2 w1 w4 w2 w3",
      "a phrase sentence with many phrase sentence iterations of a phrase sentence",
      "nothing matches this document"
    };
  }

  public void testSpanQuery() throws IOException {
    SpanQuery subq =
        SpanNearQuery.newOrderedNearQuery(FIELD_WITH_OFFSETS)
            .addClause(new SpanTermQuery(new Term(FIELD_WITH_OFFSETS, "with")))
            .addClause(new SpanTermQuery(new Term(FIELD_WITH_OFFSETS, "many")))
            .build();
    Query q =
        SpanNearQuery.newOrderedNearQuery(FIELD_WITH_OFFSETS)
            .addClause(new SpanTermQuery(new Term(FIELD_WITH_OFFSETS, "sentence")))
            .addClause(
                new SpanOrQuery(
                    subq, new SpanTermQuery(new Term(FIELD_WITH_OFFSETS, "iterations"))))
            .build();
    checkMatches(
        q, FIELD_WITH_OFFSETS, new int[][] {{0}, {1}, {2}, {3}, {4, 2, 4, 9, 27, 6, 7, 35, 54}});
    checkLabelCount(q, FIELD_WITH_OFFSETS, new int[] {0, 0, 0, 0, 1});
    checkTermMatches(
        q,
        FIELD_WITH_OFFSETS,
        new TermMatch[][][] {
          {},
          {},
          {},
          {},
          {
            {new TermMatch(2, 9, 17), new TermMatch(3, 18, 22), new TermMatch(4, 23, 27)},
            {new TermMatch(6, 35, 43), new TermMatch(7, 44, 54)}
          }
        });
  }
}
