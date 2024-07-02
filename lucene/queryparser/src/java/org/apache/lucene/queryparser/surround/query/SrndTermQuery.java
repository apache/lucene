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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;

/** Simple single-term clause */
public class SrndTermQuery extends SimpleTerm {
  public SrndTermQuery(String termText, boolean quoted) {
    super(quoted);
    this.termText = termText;
  }

  private final String termText;

  public String getTermText() {
    return termText;
  }

  public Term getLuceneTerm(String fieldName) {
    return new Term(fieldName, getTermText());
  }

  @Override
  public String toStringUnquoted() {
    return getTermText();
  }

  @Override
  public void visitMatchingTerms(IndexReader reader, String fieldName, MatchingTermVisitor mtv)
      throws IOException {
    /* check term presence in index here for symmetry with other SimpleTerm's */
    BytesRef seekTerm = new BytesRef(getTermText());
    IndexReaderContext topReaderContext = reader.getContext();
    TermStates ts = new TermStates(topReaderContext);
    for (LeafReaderContext context : topReaderContext.leaves()) {
      final Terms terms = context.reader().terms(fieldName);
      if (terms == null) {
        // field does not exist
        continue;
      }

      final TermsEnum termsEnum = terms.iterator();
      assert termsEnum != null;

      if (termsEnum == TermsEnum.EMPTY) continue;

      if (termsEnum.seekCeil(seekTerm) == TermsEnum.SeekStatus.FOUND) {
        ts.register(
            termsEnum.termState(), context.ord, termsEnum.docFreq(), termsEnum.totalTermFreq());
      }
    }
    if (ts.docFreq() > 0) {
      mtv.visitMatchingTerm(getLuceneTerm(fieldName), ts);
    }
  }
}
