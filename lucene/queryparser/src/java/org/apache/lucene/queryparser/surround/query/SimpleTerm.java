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
package org.apache.lucene.queryparser.surround.query;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOFunction;

/** Base class for queries that expand to sets of simple terms. */
public abstract class SimpleTerm extends SrndQuery implements DistanceSubQuery {
  public SimpleTerm(boolean q) {
    quoted = q;
  }

  private boolean quoted;

  boolean isQuoted() {
    return quoted;
  }

  public String getQuote() {
    return "\"";
  }

  public String getFieldOperator() {
    return "/";
  }

  public abstract String toStringUnquoted();

  protected void suffixToString(StringBuilder r) {} /* override for prefix query */

  @Override
  public String toString() {
    StringBuilder r = new StringBuilder();
    if (isQuoted()) {
      r.append(getQuote());
    }
    r.append(toStringUnquoted());
    if (isQuoted()) {
      r.append(getQuote());
    }
    suffixToString(r);
    weightToString(r);
    return r.toString();
  }

  public abstract void visitMatchingTerms(
      IndexReader reader, String fieldName, MatchingTermVisitor mtv) throws IOException;

  /**
   * Callback to visit each matching term during "rewrite" in {@link #visitMatchingTerm(Term,
   * TermStates)}
   */
  public interface MatchingTermVisitor {
    void visitMatchingTerm(Term t, TermStates termStates) throws IOException;
  }

  @Override
  public String distanceSubQueryNotAllowed() {
    return null;
  }

  @Override
  public void addSpanQueries(final SpanNearClauseFactory sncf) throws IOException {
    visitMatchingTerms(
        sncf.getIndexReader(),
        sncf.getFieldName(),
        new MatchingTermVisitor() {
          @Override
          public void visitMatchingTerm(Term term, TermStates termStates) throws IOException {
            sncf.addTermWeighted(term, getWeight(), termStates);
          }
        });
  }

  @Override
  public Query makeLuceneQueryFieldNoBoost(final String fieldName, final BasicQueryFactory qf) {
    return new SimpleTermRewriteQuery(this, fieldName, qf);
  }

  protected final Map<BytesRef, TermStates> collectTerms(
      IndexReader reader, String field, IOFunction<Terms, TermsEnum> teFunc) throws IOException {
    Map<BytesRef, TermStates> ret = new HashMap<>();
    IndexReaderContext topReaderContext = reader.getContext();
    BytesRef scratch = new BytesRef();
    for (LeafReaderContext context : topReaderContext.leaves()) {
      final Terms terms = context.reader().terms(field);
      if (terms == null) {
        // field does not exist
        continue;
      }

      final TermsEnum termsEnum = teFunc.apply(terms);
      assert termsEnum != null;

      if (termsEnum == TermsEnum.EMPTY) continue;

      BytesRef bytes;
      while ((bytes = termsEnum.next()) != null) {
        scratch.bytes = bytes.bytes;
        scratch.offset = bytes.offset;
        scratch.length = bytes.length;
        TermStates ts =
            ret.computeIfAbsent(
                scratch,
                (k) -> {
                  k.bytes = ArrayUtil.copyOfSubArray(k.bytes, k.offset, k.length);
                  k.offset = 0;
                  return new TermStates(topReaderContext);
                });
        if (scratch.bytes != bytes.bytes) {
          scratch = new BytesRef();
        }
        ts.register(
            termsEnum.termState(), context.ord, termsEnum.docFreq(), termsEnum.totalTermFreq());
      }
    }
    return ret;
  }
}
