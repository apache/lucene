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

package org.apache.lucene.queries.intervals;

import java.io.IOException;
import org.apache.lucene.search.Query;
import org.apache.lucene.tests.search.MatchesTestBase;
import org.apache.lucene.util.BytesRef;

public class TestComplexMatches extends MatchesTestBase {

  @Override
  protected String[] getDocuments() {
    return new String[] {"compare computer science", "a b a b a"};
  }

  public void testWildcards() throws IOException {
    Query q =
        new IntervalQuery(
            FIELD_WITH_OFFSETS,
            Intervals.ordered(
                Intervals.wildcard(new BytesRef("comp*")), Intervals.term("science")));

    checkMatches(q, FIELD_WITH_OFFSETS, new int[][] {{0, 1, 2, 8, 24}});
  }

  public void testRepeatedIterators() throws IOException {
    Query q =
        new IntervalQuery(
            FIELD_WITH_OFFSETS,
            Intervals.ordered(
                Intervals.term("a"),
                Intervals.term("b"),
                Intervals.term("a"),
                Intervals.term("b"),
                Intervals.term("a")));

    checkTermMatches(
        q,
        FIELD_WITH_OFFSETS,
        new TermMatch[][][] {
          {},
          {
            {
              new TermMatch(0, 0, 1),
              new TermMatch(1, 2, 3),
              new TermMatch(2, 4, 5),
              new TermMatch(3, 6, 7),
              new TermMatch(4, 8, 9)
            }
          }
        });
  }
}
