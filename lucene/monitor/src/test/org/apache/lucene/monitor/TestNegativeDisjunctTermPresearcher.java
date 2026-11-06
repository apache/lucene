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

import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;

public class TestNegativeDisjunctTermPresearcher extends NegativeDisjunctPresearcherTestBase {

  @Override
  protected Presearcher createPresearcher(
      int maxNegativeDisjuncts, List<CustomQueryHandler> handlers) {
    return new TermFilteredPresearcher(
        TermWeightor.DEFAULT, handlers, Set.of(), maxNegativeDisjuncts);
  }

  /** A document query builder that ignores negative disjuncts */
  private static class IgnoringNegativeDisjuncts
      implements TermFilteredPresearcher.DocumentQueryBuilder {
    @Override
    public void addTerm(String field, BytesRef term) {}

    @Override
    public Query build() {
      return new BooleanQuery.Builder().build();
    }
  }

  private static TermFilteredPresearcher withQueryBuilder(
      TermFilteredPresearcher.DocumentQueryBuilder builder) {
    return new TermFilteredPresearcher(TermWeightor.DEFAULT, List.of(), Set.of(), 1) {
      @Override
      protected DocumentQueryBuilder getQueryBuilder() {
        return builder;
      }
    };
  }

  public void testQueryBuildersMustSupportNegativeDisjuncts() throws IOException {
    expectThrows(
        IllegalStateException.class,
        () -> run(withQueryBuilder(new IgnoringNegativeDisjuncts()), "*:* -f:t", doc("f", "t")));
    // claiming support without implementing addNegatableTerm
    TermFilteredPresearcher.DocumentQueryBuilder claiming =
        new IgnoringNegativeDisjuncts() {
          @Override
          public boolean supportsNegativeDisjuncts() {
            return true;
          }
        };
    expectThrows(
        UnsupportedOperationException.class,
        () -> run(withQueryBuilder(claiming), "*:* -f:t", doc("f", "t")));
  }

  public void testNegativeMaxNegativeDisjunctsIsRejected() {
    expectThrows(
        IllegalArgumentException.class,
        () -> new TermFilteredPresearcher(TermWeightor.DEFAULT, List.of(), Set.of(), -1));
  }
}
