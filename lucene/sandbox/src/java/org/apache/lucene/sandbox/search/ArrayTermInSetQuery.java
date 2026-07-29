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
import java.util.Collections;
import java.util.SortedSet;
import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.BytesRefComparator;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.StringSorter;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.ByteRunAutomaton;

/**
 * Array-backed alternative to {@link TermInSetQuery} that stores its terms as a sorted {@code
 * BytesRef[]} (zero-copy views into a single packed {@code byte[]}) instead of {@link
 * org.apache.lucene.index.PrefixCodedTerms}. Trades a bit of RAM for cheaper per-segment iteration
 * and a vectorized {@link #equals}/{@link #hashCode} fast path on the packed bytes.
 *
 * <p>Constructed and used identically to {@link TermInSetQuery} — accepts an arbitrary {@link
 * Collection} of terms and sorts/deduplicates internally. If the input is a {@link SortedSet} with
 * natural-order comparator, the sort is skipped (same fast path as {@link TermInSetQuery}).
 *
 * <p>Useful in CPU-bound workloads with large term sets (tens of thousands of terms) where many
 * segments are too small to benefit from the {@link org.apache.lucene.search.LRUQueryCache}, so the
 * per-segment unpacking cost of {@link org.apache.lucene.index.PrefixCodedTerms} dominates.
 */
public class ArrayTermInSetQuery extends MultiTermQuery implements Accountable {

  private static final long BASE_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(ArrayTermInSetQuery.class);

  private static final long BYTES_REF_SHALLOW_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(BytesRef.class);

  private final byte[] packedTerms;
  private final BytesRef[] terms;
  private final int cachedHashCode;
  private final long ramBytesUsed;

  /**
   * Constructs a query matching documents containing any of the given terms in the given field.
   *
   * <p>Terms are MSB radix-sorted and adjacent duplicates removed, then packed into a single
   * contiguous {@code byte[]} with VInt-length prefixes encoding term boundaries. The
   * length-prefixed packing makes the representation canonically unique so {@link #equals} and
   * {@link #hashCode} can operate on the raw {@code byte[]} via a single vectorized memcmp.
   */
  public ArrayTermInSetQuery(String field, Collection<BytesRef> terms) {
    super(field, CONSTANT_SCORE_BLENDED_REWRITE);

    BytesRef[] sortedTerms = terms.toArray(new BytesRef[0]);
    // Already sorted if we are a SortedSet with natural-order comparator. Same O(1)
    // fast path TermInSetQuery uses.
    boolean sorted =
        terms instanceof SortedSet && ((SortedSet<BytesRef>) terms).comparator() == null;
    if (sorted == false) {
      new StringSorter(BytesRefComparator.NATURAL) {
        @Override
        protected void get(BytesRefBuilder builder, BytesRef result, int i) {
          BytesRef t = sortedTerms[i];
          result.bytes = t.bytes;
          result.offset = t.offset;
          result.length = t.length;
        }

        @Override
        protected void swap(int i, int j) {
          BytesRef tmp = sortedTerms[i];
          sortedTerms[i] = sortedTerms[j];
          sortedTerms[j] = tmp;
        }
      }.sort(0, sortedTerms.length);
    }

    // Walk once to count uniques and total packed length, then a second time to
    // write packed bytes + views. Two passes give us exact-sized allocations
    // without an intermediate growable buffer or an explicit dedup'd array.
    int uniqueCount = 0;
    int totalLen = 0;
    BytesRef previous = null;
    for (BytesRef t : sortedTerms) {
      if (previous != null && previous.equals(t)) {
        continue;
      }
      uniqueCount++;
      totalLen += vIntSize(t.length) + t.length;
      previous = t;
    }

    byte[] packed = new byte[totalLen];
    BytesRef[] views = new BytesRef[uniqueCount];
    int pos = 0;
    int idx = 0;
    previous = null;
    for (BytesRef t : sortedTerms) {
      if (previous != null && previous.equals(t)) {
        continue;
      }
      pos = writeVInt(packed, pos, t.length);
      System.arraycopy(t.bytes, t.offset, packed, pos, t.length);
      views[idx++] = new BytesRef(packed, pos, t.length);
      pos += t.length;
      previous = t;
    }

    this.packedTerms = packed;
    this.terms = views;
    this.cachedHashCode = Arrays.hashCode(packed);
    this.ramBytesUsed =
        BASE_RAM_BYTES_USED
            + RamUsageEstimator.sizeOf(packed)
            + RamUsageEstimator.shallowSizeOf(views)
            + (long) views.length * BYTES_REF_SHALLOW_SIZE;
  }

  private static int vIntSize(int value) {
    int size = 1;
    while ((value & ~0x7F) != 0) {
      size++;
      value >>>= 7;
    }
    return size;
  }

  private static int writeVInt(byte[] dest, int pos, int value) {
    while ((value & ~0x7F) != 0) {
      dest[pos++] = (byte) ((value & 0x7F) | 0x80);
      value >>>= 7;
    }
    dest[pos++] = (byte) value;
    return pos;
  }

  @Override
  protected TermsEnum getTermsEnum(Terms terms, AttributeSource atts) throws IOException {
    return new ArraySetEnum(terms.iterator(), this.terms);
  }

  @Override
  public long getTermsCount() {
    return terms.length;
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(getField()) == false) {
      return;
    }
    if (terms.length == 1) {
      visitor.consumeTerms(this, new Term(getField(), terms[0]));
    }
    if (terms.length > 1) {
      visitor.consumeTermsMatching(this, getField(), this::asByteRunAutomaton);
    }
  }

  private ByteRunAutomaton asByteRunAutomaton() {
    try {
      Automaton a = Automata.makeBinaryStringUnion(new ArrayBytesRefIterator(terms));
      return new ByteRunAutomaton(a, true);
    } catch (IOException e) {
      // Shouldn't happen: ArrayBytesRefIterator's next() never throws.
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) && equalsTo(getClass().cast(other));
  }

  private boolean equalsTo(ArrayTermInSetQuery other) {
    return cachedHashCode == other.cachedHashCode
        && getField().equals(other.getField())
        && Arrays.equals(packedTerms, other.packedTerms);
  }

  @Override
  public int hashCode() {
    return 31 * classHash() + cachedHashCode;
  }

  @Override
  public String toString(String defaultField) {
    StringBuilder sb = new StringBuilder();
    sb.append(getField()).append(":(");
    for (int i = 0; i < terms.length; i++) {
      if (i > 0) {
        sb.append(' ');
      }
      sb.append(Term.toString(terms[i]));
    }
    sb.append(')');
    return sb.toString();
  }

  @Override
  public long ramBytesUsed() {
    return ramBytesUsed;
  }

  @Override
  public Collection<Accountable> getChildResources() {
    return Collections.emptyList();
  }

  /**
   * Same ping-pong logic as {@code TermInSetQuery.SetEnum} but backed by a pre-decoded {@link
   * BytesRef}{@code []} instead of a streaming {@code PrefixCodedTerms.TermIterator}.
   */
  private static class ArraySetEnum extends FilteredTermsEnum {
    private final BytesRef[] terms;
    private int idx = 0;

    ArraySetEnum(TermsEnum termsEnum, BytesRef[] terms) {
      super(termsEnum);
      this.terms = terms;
    }

    private BytesRef currentTerm() {
      return idx < terms.length ? terms[idx] : null;
    }

    @Override
    protected AcceptStatus accept(BytesRef term) throws IOException {
      int cmp = 0;
      while (idx < terms.length && (cmp = terms[idx].compareTo(term)) < 0) {
        idx++;
      }
      if (idx >= terms.length) {
        return AcceptStatus.END;
      } else if (cmp == 0) {
        return AcceptStatus.YES_AND_SEEK;
      } else {
        return AcceptStatus.NO_AND_SEEK;
      }
    }

    @Override
    protected BytesRef nextSeekTerm(BytesRef currentTerm) throws IOException {
      if (currentTerm == null) {
        return currentTerm();
      }
      while (idx < terms.length && terms[idx].compareTo(currentTerm) <= 0) {
        idx++;
      }
      return currentTerm();
    }
  }

  private static class ArrayBytesRefIterator implements BytesRefIterator {
    private final BytesRef[] terms;
    private int idx = 0;

    ArrayBytesRefIterator(BytesRef[] terms) {
      this.terms = terms;
    }

    @Override
    public BytesRef next() {
      return idx < terms.length ? terms[idx++] : null;
    }
  }
}
