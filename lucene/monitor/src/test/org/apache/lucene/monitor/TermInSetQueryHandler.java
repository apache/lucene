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
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;

/**
 * Handles {@link TermInSetQuery}s with more than one term, which visit their terms as an automaton
 * and so reach the presearcher as unknown leaves. Such a query matches exactly the documents that
 * contain any of its terms, so its negation, "contains none of them", is a negative disjunct;
 * whether this handler provides it is configurable.
 */
final class TermInSetQueryHandler implements CustomQueryHandler {

  private final boolean negatable;

  TermInSetQueryHandler(boolean negatable) {
    this.negatable = negatable;
  }

  static Set<Term> terms(TermInSetQuery query) {
    Set<Term> terms = new TreeSet<>();
    try {
      BytesRefIterator it = query.getBytesRefIterator();
      for (BytesRef term = it.next(); term != null; term = it.next()) {
        terms.add(new Term(query.getField(), BytesRef.deepCopyOf(term)));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return terms;
  }

  @Override
  public QueryTree handleQuery(Query query, TermWeightor termWeightor) {
    if (query instanceof TermInSetQuery tisq) {
      List<Function<TermWeightor, QueryTree>> children = new ArrayList<>();
      for (Term term : terms(tisq)) {
        children.add(w -> QueryTree.term(term, w));
      }
      return QueryTree.disjunction(children, termWeightor);
    }
    return null;
  }

  @Override
  public QueryTree handleNegatedQuery(Query query, TermWeightor termWeightor) {
    if (negatable && query instanceof TermInSetQuery tisq) {
      return QueryTree.negativeDisjunct(terms(tisq), termWeightor);
    }
    return null;
  }
}
