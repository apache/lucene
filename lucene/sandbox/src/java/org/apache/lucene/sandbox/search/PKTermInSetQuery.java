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
package org.apache.lucene.sandbox.search;

import java.io.IOException;
import java.util.Collection;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;

/**
 * {@link TermInSetQuery} optimized for a primary key-like field.
 *
 * <p>Relies on {@link TermsEnum#seekExact(BytesRef)} instead of {@link
 * TermsEnum#seekCeil(BytesRef)} to produce a terms iterator, which is compatible with {@code
 * BloomFilteringPostingsFormat}.
 */
public class PKTermInSetQuery extends TermInSetQuery {
  public PKTermInSetQuery(String field, Collection<BytesRef> terms) {
    super(field, terms);
  }

  public PKTermInSetQuery(String field, BytesRef... terms) {
    super(field, terms);
  }

  public PKTermInSetQuery(RewriteMethod rewriteMethod, String field, Collection<BytesRef> terms) {
    super(rewriteMethod, field, terms);
  }

  public PKTermInSetQuery(RewriteMethod rewriteMethod, String field, BytesRef... terms) {
    super(rewriteMethod, field, terms);
  }

  @Override
  protected TermsEnum getTermsEnum(Terms terms, AttributeSource atts) throws IOException {
    final TermsEnum tEnum = terms.iterator();
    final BytesRefIterator queryTerms = getQueryTerms();

    return new TermsEnum() {
      @Override
      public BytesRef next() throws IOException {
        BytesRef nextTerm;
        while ((nextTerm = queryTerms.next()) != null) {
          if (tEnum.seekExact(nextTerm)) {
            break;
          }
        }
        return nextTerm;
      }

      @Override
      public AttributeSource attributes() {
        return tEnum.attributes();
      }

      @Override
      public BytesRef term() throws IOException {
        return tEnum.term();
      }

      @Override
      public long ord() throws IOException {
        return tEnum.ord();
      }

      @Override
      public int docFreq() throws IOException {
        return tEnum.docFreq();
      }

      @Override
      public long totalTermFreq() throws IOException {
        return tEnum.totalTermFreq();
      }

      @Override
      public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
        return tEnum.postings(reuse, flags);
      }

      @Override
      public ImpactsEnum impacts(int flags) throws IOException {
        return tEnum.impacts(flags);
      }

      @Override
      public TermState termState() throws IOException {
        return tEnum.termState();
      }

      @Override
      public boolean seekExact(BytesRef text) throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public SeekStatus seekCeil(BytesRef text) throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public void seekExact(long ord) throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public void seekExact(BytesRef term, TermState state) throws IOException {
        throw new UnsupportedOperationException();
      }
    };
  }
}
