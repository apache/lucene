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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.SortedSet;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.FiniteStringsIterator;
import org.apache.lucene.util.fst.Util;

/**
 * Specialization for a disjunction over many terms that, by default, behaves like a {@link
 * ConstantScoreQuery} over a {@link BooleanQuery} containing only {@link
 * org.apache.lucene.search.BooleanClause.Occur#SHOULD} clauses.
 *
 * <p>For instance in the following example, both {@code q1} and {@code q2} would yield the same
 * scores:
 *
 * <pre class="prettyprint">
 * Query q1 = new TermInSetQuery("field", new BytesRef("foo"), new BytesRef("bar"));
 *
 * BooleanQuery bq = new BooleanQuery();
 * bq.add(new TermQuery(new Term("field", "foo")), Occur.SHOULD);
 * bq.add(new TermQuery(new Term("field", "bar")), Occur.SHOULD);
 * Query q2 = new ConstantScoreQuery(bq);
 * </pre>
 *
 * <p>Unless a custom {@link MultiTermQuery.RewriteMethod} is provided, this query executes like a
 * regular disjunction where there are few terms. However, when there are many terms, instead of
 * merging iterators on the fly, it will populate a bit set with matching docs for the least-costly
 * terms and maintain a size-limited set of more costly iterators that are merged on the fly. For
 * more details, see {@link MultiTermQuery#CONSTANT_SCORE_BLENDED_REWRITE}.
 *
 * <p>Users may also provide a custom {@link MultiTermQuery.RewriteMethod} to define different
 * execution behavior, such as relying on doc values (see: {@link
 * MultiTermQuery#DOC_VALUES_REWRITE}), or if scores are required (see: {@link
 * MultiTermQuery#SCORING_BOOLEAN_REWRITE}). See {@link MultiTermQuery} documentation for more
 * rewrite options.
 *
 * <p>NOTE: This query produces scores that are equal to its boost
 */
public class TermInSetQuery extends AutomatonQuery {
  private final String field;
  private final int termCount;

  public TermInSetQuery(String field, Collection<BytesRef> terms) {
    super(new Term(field), toAutomaton(terms), true, false, true);
    this.field = field;
    this.termCount = terms.size();
  }

  public TermInSetQuery(String field, BytesRef... terms) {
    this(field, Arrays.asList(terms));
  }

  /** Creates a new {@link TermInSetQuery} from the given collection of terms. */
  public TermInSetQuery(RewriteMethod rewriteMethod, String field, Collection<BytesRef> terms) {
    super(new Term(field), toAutomaton(terms), true, false, true, rewriteMethod);
    this.field = field;
    this.termCount = terms.size();
  }

  /** Creates a new {@link TermInSetQuery} from the given array of terms. */
  public TermInSetQuery(RewriteMethod rewriteMethod, String field, BytesRef... terms) {
    this(rewriteMethod, field, Arrays.asList(terms));
  }

  private static Automaton toAutomaton(Collection<BytesRef> terms) {
    BytesRef[] sortedTerms = terms.toArray(new BytesRef[0]);
    // already sorted if we are a SortedSet with natural order
    boolean sorted =
        terms instanceof SortedSet && ((SortedSet<BytesRef>) terms).comparator() == null;
    if (sorted == false) {
      ArrayUtil.timSort(sortedTerms);
    }
    List<BytesRef> sortedAndDeduped = new ArrayList<>(terms.size());
    BytesRefBuilder previous = null;
    for (BytesRef term : sortedTerms) {
      if (previous == null) {
        previous = new BytesRefBuilder();
      } else if (previous.get().equals(term)) {
        continue; // deduplicate
      }
      sortedAndDeduped.add(term);
      previous.copyBytes(term);
    }

    return Automata.makeBinaryStringUnion(sortedAndDeduped);
  }

  @Override
  public long getTermsCount() throws IOException {
    return termCount;
  }

  /**
   * This is semi-expensive as it fully traverses the automaton to create a helpful toString
   * representation.
   */
  @Override
  public String toString(String defaultField) {
    StringBuilder builder = new StringBuilder();
    builder.append(field);
    builder.append(":(");

    boolean first = true;
    FiniteStringsIterator it = new FiniteStringsIterator(automaton);
    BytesRefBuilder scratch = new BytesRefBuilder();
    for (IntsRef term = it.next(); term != null; term = it.next()) {
      if (first == false) {
        builder.append(' ');
      }
      first = false;
      BytesRef t = Util.toBytesRef(term, scratch);
      builder.append(Term.toString(t));
    }
    builder.append(')');

    return builder.toString();
  }
}
