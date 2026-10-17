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
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.sandbox.codecs.pkhash.HashTerms;
import org.apache.lucene.sandbox.codecs.pkhash.HashedTerms;
import org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashPostingsFormat;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.MatchesUtils;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.ByteRunAutomaton;

/**
 * A constant-score query that matches the documents whose {@code field} contains any of a set of
 * terms, called keys here, and that looks them up in the hash index of {@link
 * PrimaryKeyHashPostingsFormat} when the field uses it. It is meant for keys that each occur in
 * about one document, such as primary keys or other unique ids.
 *
 * <p>It matches exactly the documents that a {@link TermInSetQuery} on the same field and keys
 * matches, with the same scores (the boost). It differs in how keys are resolved: the keys are
 * sorted, de-duplicated and hashed once, when the query is created, and every segment then looks
 * them all up in one go:
 *
 * <ul>
 *   <li>If the segment's {@link Terms} is a {@link HashTerms}, all keys are resolved with {@link
 *       HashTerms#lookupBatch}, whose staged probes overlap the memory accesses of many keys. Only
 *       keys that have more than one document in the segment are sought with a {@link TermsEnum},
 *       in the terms dictionary of the delegate format ({@link HashTerms#getDelegate}).
 *   <li>Otherwise (another postings format, or a reader wrapper that hides the format), the keys
 *       are sought in sorted order with {@link TermsEnum#seekExact(BytesRef)} on a single {@link
 *       TermsEnum}.
 * </ul>
 *
 * <p>A segment is resolved completely when its {@link ScorerSupplier} is created, so that a segment
 * without matches gets no scorer and the reported cost is the number of documents found. This makes
 * the query a good fit for sets of keys that each match about one document, and a poor one for keys
 * that match many documents each, where a {@link TermInSetQuery} is the better choice. All keys are
 * resolved even when the query is intersected with a much more selective one, except that on a
 * segment with a hash table, keys outside the range of its terms are skipped without any read. As
 * with any query, deleted documents are filtered out by the searcher, not here.
 *
 * @lucene.experimental
 */
public final class PrimaryKeyHashInSetQuery extends Query implements Accountable {

  private static final long BASE_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(PrimaryKeyHashInSetQuery.class);

  /**
   * Keys passed to each {@link HashTerms#lookupBatch} call, which bounds the scratch space per
   * segment.
   */
  static final int LOOKUP_WINDOW = 1024;

  /**
   * If the table finds at most this many documents, and no key has several documents, they are
   * sorted in place rather than collected by a {@link DocIdSetBuilder}, whose fixed cost would
   * exceed that of the lookups. For more documents, the builder's radix sort or bit set is as
   * cheap.
   */
  private static final int MAX_SORTED_HITS = 32;

  private final String field;
  private final HashedTerms keys;

  /**
   * Creates a query that matches documents whose {@code field} contains any of {@code keys}. The
   * keys are copied, and duplicates are ignored.
   */
  public PrimaryKeyHashInSetQuery(String field, Collection<BytesRef> keys) {
    this.field = Objects.requireNonNull(field, "field must not be null");
    this.keys = new HashedTerms(Objects.requireNonNull(keys, "keys must not be null"));
  }

  /**
   * Creates a query that matches documents whose {@code field} contains any of {@code keys}. The
   * keys are copied, and duplicates are ignored.
   */
  public PrimaryKeyHashInSetQuery(String field, BytesRef... keys) {
    this.field = Objects.requireNonNull(field, "field must not be null");
    this.keys = new HashedTerms(Objects.requireNonNull(keys, "keys must not be null"));
  }

  /** Returns the field. */
  public String getField() {
    return field;
  }

  /** Returns the distinct keys, in sorted order. */
  public HashedTerms getKeys() {
    return keys;
  }

  @Override
  public Query rewrite(IndexSearcher indexSearcher) throws IOException {
    if (keys.size() == 0) {
      return new MatchNoDocsQuery("empty key set");
    }
    return super.rewrite(indexSearcher);
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    return new ConstantScoreWeight(this, boost) {

      @Override
      public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        if (keys.size() == 0) {
          return null;
        }
        final Terms terms = context.reader().terms(field);
        if (terms == null) {
          return null;
        }
        final int maxDoc = context.reader().maxDoc();
        final DocIdSetIterator iterator =
            terms instanceof HashTerms hashTerms
                ? lookupHashed(hashTerms, maxDoc)
                : lookupSorted(terms, maxDoc);
        if (iterator == null) {
          return null;
        }
        final float score = score();
        return new ScorerSupplier() {
          @Override
          public Scorer get(long leadCost) {
            return new ConstantScoreScorer(score, scoreMode, iterator);
          }

          @Override
          public long cost() {
            return iterator.cost();
          }
        };
      }

      @Override
      public Matches matches(LeafReaderContext context, int doc) throws IOException {
        if (keys.size() == 0 || context.reader().terms(field) == null) {
          return null;
        }
        return MatchesUtils.forField(
            field,
            () ->
                MatchesUtils.disjunction(
                    context, doc, PrimaryKeyHashInSetQuery.this, field, keys.iterator()));
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return true;
      }
    };
  }

  /**
   * Resolves the keys within the range of the segment's terms with {@link HashTerms#lookupBatch},
   * and keys with several documents with a {@link TermsEnum} of the delegate format's terms, which
   * the table would only send there after reading it again. Returns the matching documents, or null
   * if there are none.
   */
  private DocIdSetIterator lookupHashed(HashTerms terms, int maxDoc) throws IOException {
    // Keys are sorted, and only those within the range of the segment's terms can be in it. When
    // keys are assigned in increasing order, most segments hold a narrow range of them.
    final int start = firstKeyAfter(terms.getMin(), 0, keys.size(), true);
    final int end = firstKeyAfter(terms.getMax(), start, keys.size(), false);
    if (start == end) {
      return null;
    }
    final int[] docs = new int[Math.min(end - start, LOOKUP_WINDOW)];
    // Created when needed: for the postings of keys with several documents, or for many hits.
    DocIdSetBuilder builder = null;
    TermsEnum termsEnum = null;
    PostingsEnum postings = null;
    int numHits = 0;
    for (int from = start, n; from < end; from += n) {
      n = Math.min(LOOKUP_WINDOW, end - from);
      terms.lookupBatch(keys, from, from + n, docs);
      // Compacts the hits of the window in place, without a branch on whether each key is a hit,
      // which hits and misses mixed would mispredict: ABSENT and MULTI are negative and are
      // dropped. Keys of several documents are rare; they are sought in key order, so that the
      // enum moves forward.
      numHits = 0;
      for (int i = 0; i < n; i++) {
        final int doc = docs[i];
        assert doc >= 0 || doc == HashTerms.ABSENT || doc == HashTerms.MULTI : doc;
        if (doc == HashTerms.MULTI) {
          if (termsEnum == null) {
            termsEnum = terms.getDelegate().iterator();
          }
          if (termsEnum.seekExact(keys.get(from + i))) {
            if (builder == null) {
              builder = new DocIdSetBuilder(maxDoc, terms);
            }
            postings = termsEnum.postings(postings, PostingsEnum.NONE);
            builder.add(postings);
          }
        }
        docs[numHits] = doc;
        numHits += ~doc >>> 31;
      }
      // The hits of the last window may be all there are: they are kept for the end.
      if (numHits > 0 && from + n < end) {
        if (builder == null) {
          builder = new DocIdSetBuilder(maxDoc, terms);
        }
        builder.grow(numHits).add(new IntsRef(docs, 0, numHits));
        numHits = 0;
      }
    }
    if (builder == null) {
      if (numHits == 0) {
        return null;
      } else if (numHits <= MAX_SORTED_HITS) {
        // the common case of looking up a few ids
        return sortedDocs(docs, numHits);
      }
      builder = new DocIdSetBuilder(maxDoc, terms);
    }
    if (numHits > 0) {
      builder.grow(numHits).add(new IntsRef(docs, 0, numHits));
    }
    return builder.build().iterator();
  }

  /**
   * The first index in {@code [from, to)} of a key that sorts after {@code term}, or equal to it if
   * {@code orEqual}, or {@code to} if there is none.
   */
  private int firstKeyAfter(BytesRef term, int from, int to, boolean orEqual) {
    while (from < to) {
      int mid = (from + to) >>> 1;
      int cmp = keys.get(mid).compareTo(term);
      if (cmp > 0 || (orEqual && cmp == 0)) {
        to = mid;
      } else {
        from = mid + 1;
      }
    }
    return from;
  }

  /**
   * Sorts and de-duplicates (a document may hold several of the keys) the first {@code length}
   * documents of {@code docs}, few of them, and iterates over them.
   */
  private static DocIdSetIterator sortedDocs(int[] docs, int length) {
    assert length > 0;
    Arrays.sort(docs, 0, length);
    int upto = 1;
    for (int i = 1; i < length; i++) {
      if (docs[i] != docs[upto - 1]) {
        docs[upto++] = docs[i];
      }
    }
    return new SortedDocsIterator(docs, upto);
  }

  /** Iterates over the first {@code length} documents of {@code docs}, which are sorted. */
  private static final class SortedDocsIterator extends DocIdSetIterator {
    private final int[] docs;
    private final int length;
    private int index = -1;
    private int doc = -1;

    SortedDocsIterator(int[] docs, int length) {
      this.docs = docs;
      this.length = length;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() {
      return doc = ++index < length ? docs[index] : NO_MORE_DOCS;
    }

    @Override
    public int advance(int target) {
      // a scan rather than a binary search: there are few documents, and a full iteration with
      // advance() still visits each one once
      while (++index < length) {
        if (docs[index] >= target) {
          return doc = docs[index];
        }
      }
      return doc = NO_MORE_DOCS;
    }

    @Override
    public long cost() {
      return length;
    }
  }

  /**
   * Resolves the keys in sorted order with {@link TermsEnum#seekExact(BytesRef)} on one {@link
   * TermsEnum}. Returns the matching documents, or null if there are none.
   */
  private DocIdSetIterator lookupSorted(Terms terms, int maxDoc) throws IOException {
    final TermsEnum termsEnum = terms.iterator();
    final BytesRefIterator iterator = keys.iterator();
    PostingsEnum postings = null;
    DocIdSetBuilder builder = null;
    for (BytesRef key = iterator.next(); key != null; key = iterator.next()) {
      if (termsEnum.seekExact(key)) {
        if (builder == null) {
          builder = new DocIdSetBuilder(maxDoc, terms);
        }
        postings = termsEnum.postings(postings, PostingsEnum.NONE);
        builder.add(postings);
      }
    }
    return builder == null ? null : builder.build().iterator();
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(field) == false) {
      return;
    }
    if (keys.size() == 1) {
      visitor.consumeTerms(this, new Term(field, BytesRef.deepCopyOf(keys.get(0))));
    } else if (keys.size() > 1) {
      visitor.consumeTermsMatching(this, field, this::asByteRunAutomaton);
    }
  }

  private ByteRunAutomaton asByteRunAutomaton() {
    try {
      return new ByteRunAutomaton(Automata.makeBinaryStringUnion(keys.iterator()), true);
    } catch (IOException e) {
      // cannot happen: the iterator reads from memory
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) && equalsTo(getClass().cast(other));
  }

  private boolean equalsTo(PrimaryKeyHashInSetQuery other) {
    // the hash code of the keys is cached, and comparing it first is cheap
    return field.equals(other.field)
        && keys.hashCode() == other.keys.hashCode()
        && keys.equals(other.keys);
  }

  @Override
  public int hashCode() {
    return 31 * (31 * classHash() + field.hashCode()) + keys.hashCode();
  }

  @Override
  public String toString(String defaultField) {
    StringBuilder builder = new StringBuilder();
    builder.append(field);
    builder.append(":(");
    for (int i = 0; i < keys.size(); i++) {
      if (i > 0) {
        builder.append(' ');
      }
      builder.append(Term.toString(keys.get(i)));
    }
    builder.append(')');
    return builder.toString();
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOfObject(field) + keys.ramBytesUsed();
  }
}
