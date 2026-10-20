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
package org.apache.lucene.sandbox.codecs.pkhash;

import java.io.IOException;
import org.apache.lucene.index.BaseTermsEnum;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SlowImpactsEnum;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IOBooleanSupplier;

/**
 * A {@link TermsEnum} whose exact seeks go through the hash table and everything else through the
 * delegate's enum, which is only pulled when needed.
 *
 * <p>Exact seeks do not read the field's filter, if it has one: a single lookup has no other reads
 * to overlap the read of the filter with. They check the range of the field's terms, and the key's
 * length if the field's keys are inline, then read the control bytes, and then, for each slot with
 * the key's control byte, its slot word and, if the tag matches, its record, or on an inline field
 * its record.
 *
 * <p>The enum is in one of three modes. After a successful exact seek of a key with a single
 * document it is in {@code HASH} mode: it knows the term and its document, and has not touched the
 * delegate. Keys with more than one document are sought in the delegate, as are all ordered
 * operations, which put the enum in {@code DELEGATE} mode. Moving on from a term found in {@code
 * HASH} mode ({@link #next}, {@link #ord}) first positions the delegate on that term.
 */
final class HashTermsEnum extends BaseTermsEnum {

  private enum Mode {
    /** Not positioned on a term: fresh, after a failed seek, or exhausted. */
    UNPOSITIONED,
    /** On a term found in the hash table: {@link #term} and {@link #doc} are valid. */
    HASH,
    /** On the delegate's current term. */
    DELEGATE
  }

  private final HashTerms terms;

  private final BytesRefBuilder term = new BytesRefBuilder();
  private RandomAccessInput in;
  private TermsEnum delegate;
  private Mode mode = Mode.UNPOSITIONED;
  private int doc;

  HashTermsEnum(HashTerms terms) {
    this.terms = terms;
  }

  private TermsEnum delegate() throws IOException {
    if (delegate == null) {
      // pulled lazily, an exact-seek-only consumer never needs it
      delegate = terms.getDelegate().iterator();
    }
    return delegate;
  }

  private RandomAccessInput in() throws IOException {
    if (in == null) {
      in = terms.newSlice();
    }
    return in;
  }

  @Override
  public boolean preferSeekExact() {
    return true;
  }

  @Override
  public boolean seekExact(BytesRef text) throws IOException {
    if (terms.outOfRange(text.bytes, text.offset, text.length)
        || terms.excludesLength(text.length)) {
      mode = Mode.UNPOSITIONED;
      return false;
    }
    final long hash = TermHash.hash(text.bytes, text.offset, text.length);
    return position(text, terms.lookup(in(), text, hash));
  }

  /** Positions the enum on {@code text}, given what its lookup in the hash table returned. */
  private boolean position(BytesRef text, int result) throws IOException {
    if (result >= 0) {
      term.copyBytes(text);
      doc = result;
      mode = Mode.HASH;
      return true;
    } else if (result == HashTerms.MULTI) {
      boolean found = delegate().seekExact(text);
      mode = found ? Mode.DELEGATE : Mode.UNPOSITIONED;
      return found;
    } else {
      assert result == HashTerms.ABSENT;
      mode = Mode.UNPOSITIONED;
      return false;
    }
  }

  /**
   * Hashes the key and prefetches the control bytes from its home slot. If the prefetch reports
   * that they have to be read, the lookup is deferred to the returned supplier, so that the reads
   * of many segments (see {@code TermStates}) can be in flight together. If they are already in
   * memory, the key is looked up right away, and this returns null if it is absent. Returns null,
   * with no read, for a key outside the range of the field's terms, or whose length differs from
   * that of the field's inline keys. Like {@link #seekExact(BytesRef)}, this does not read the
   * field's filter.
   */
  @Override
  public IOBooleanSupplier prepareSeekExact(BytesRef text) throws IOException {
    if (terms.outOfRange(text.bytes, text.offset, text.length)
        || terms.excludesLength(text.length)) {
      mode = Mode.UNPOSITIONED;
      return null;
    }
    final long hash = TermHash.hash(text.bytes, text.offset, text.length);
    final RandomAccessInput input = in();
    final int p = terms.partition(hash);
    final int home = terms.homeSlot(hash, p);
    if (input.prefetch(terms.controlOffset(p) + home, Long.BYTES)) {
      return new IOBooleanSupplier() {
        @Override
        public boolean get() throws IOException {
          return position(text, terms.lookup(input, text, hash, p, home));
        }

        @Override
        public boolean doDefer() {
          return true;
        }
      };
    }
    final int result = terms.lookup(input, text, hash, p, home);
    if (result == HashTerms.ABSENT) {
      mode = Mode.UNPOSITIONED;
      return null;
    }
    return () -> position(text, result);
  }

  @Override
  public void seekExact(BytesRef target, TermState state) throws IOException {
    if (state instanceof HashTermState hashState) {
      term.copyBytes(target);
      doc = hashState.docID;
      mode = Mode.HASH;
    } else {
      // never pass our own state to the delegate
      delegate().seekExact(target, state);
      mode = Mode.DELEGATE;
    }
  }

  @Override
  public TermState termState() throws IOException {
    return switch (mode) {
      case HASH -> new HashTermState(doc);
      case DELEGATE -> delegate.termState();
      case UNPOSITIONED -> throw unpositioned();
    };
  }

  /** Positions the delegate on the term found in the hash table, before it moves on from there. */
  private TermsEnum syncDelegate() throws IOException {
    if (mode == Mode.HASH) {
      if (delegate().seekExact(term.get()) == false) {
        throw new CorruptIndexException(
            "term found in the hash table is missing from the terms dictionary: " + term.get(),
            toString());
      }
      mode = Mode.DELEGATE;
    }
    return delegate();
  }

  @Override
  public BytesRef next() throws IOException {
    BytesRef next = syncDelegate().next();
    mode = next == null ? Mode.UNPOSITIONED : Mode.DELEGATE;
    return next;
  }

  @Override
  public SeekStatus seekCeil(BytesRef text) throws IOException {
    SeekStatus status = delegate().seekCeil(text);
    mode = status == SeekStatus.END ? Mode.UNPOSITIONED : Mode.DELEGATE;
    return status;
  }

  @Override
  public void seekExact(long ord) throws IOException {
    delegate().seekExact(ord);
    mode = Mode.DELEGATE;
  }

  @Override
  public long ord() throws IOException {
    return syncDelegate().ord();
  }

  @Override
  public BytesRef term() throws IOException {
    return switch (mode) {
      case HASH -> term.get();
      case DELEGATE -> delegate.term();
      case UNPOSITIONED -> throw unpositioned();
    };
  }

  @Override
  public int docFreq() throws IOException {
    return switch (mode) {
      case HASH -> 1;
      case DELEGATE -> delegate.docFreq();
      case UNPOSITIONED -> throw unpositioned();
    };
  }

  @Override
  public long totalTermFreq() throws IOException {
    return switch (mode) {
      case HASH -> 1; // DOCS only: one occurrence per document
      case DELEGATE -> delegate.totalTermFreq();
      case UNPOSITIONED -> throw unpositioned();
    };
  }

  /**
   * Returns a {@link SingleDocPostingsEnum} in {@code HASH} mode and the delegate's postings
   * otherwise. On keys that alternate between modes, reusing one kind of enum would lose the other,
   * so a {@link SingleDocPostingsEnum} keeps the delegate's enum that it replaced, and gives it
   * back to the delegate when it is passed for reuse in {@code DELEGATE} mode.
   */
  @Override
  public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
    return switch (mode) {
      case HASH -> {
        final SingleDocPostingsEnum single;
        if (reuse instanceof SingleDocPostingsEnum s) {
          single = s;
        } else {
          single = new SingleDocPostingsEnum();
          single.delegateReuse = reuse;
        }
        yield single.reset(doc);
      }
      case DELEGATE -> {
        PostingsEnum delegateReuse = reuse;
        if (reuse instanceof SingleDocPostingsEnum single) {
          delegateReuse = single.delegateReuse;
          single.delegateReuse = null;
        }
        yield delegate.postings(delegateReuse, flags);
      }
      case UNPOSITIONED -> throw unpositioned();
    };
  }

  @Override
  public ImpactsEnum impacts(int flags) throws IOException {
    return switch (mode) {
      case HASH -> new SlowImpactsEnum(new SingleDocPostingsEnum().reset(doc));
      case DELEGATE -> delegate.impacts(flags);
      case UNPOSITIONED -> throw unpositioned();
    };
  }

  private IllegalStateException unpositioned() {
    return new IllegalStateException("the enum is not positioned on a term");
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(mode=" + mode + ", terms=" + terms + ")";
  }
}
