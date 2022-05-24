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
package org.apache.lucene.index;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import org.apache.lucene.index.TermsEnum.SeekStatus;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;

class TermsEnumIndex {

  static final TermsEnumIndex[] EMPTY_ARRAY = new TermsEnumIndex[0];

  /**
   * Copy the first 8 bytes of the given term as a comparable unsigned long. In case the term has
   * less than 8 bytes, missing bytes will be replaced with zeroes. Note that two terms that produce
   * the same long could still be different due to the fact that missing bytes are replaced with
   * zeroes, e.g. {@code [1, 0]} and {@code [1]} get mapped to the same long.
   */
  static long prefix8ToComparableUnsignedLong(BytesRef term, int prefixLength) {
    // Use Big Endian so that longs are comparable
    final int offset = term.offset + prefixLength;
    final int length = term.length - prefixLength;
    if (length >= Long.BYTES) {
      return (long) BitUtil.VH_BE_LONG.get(term.bytes, offset);
    } else {
      long l;
      int o;
      if (Integer.BYTES <= length) {
        l = (int) BitUtil.VH_BE_INT.get(term.bytes, offset);
        o = Integer.BYTES;
      } else {
        l = 0;
        o = 0;
      }
      if (o + Short.BYTES <= length) {
        l =
            (l << Short.SIZE)
                | Short.toUnsignedLong((short) BitUtil.VH_BE_SHORT.get(term.bytes, offset + o));
        o += Short.BYTES;
      }
      if (o < length) {
        l = (l << Byte.SIZE) | Byte.toUnsignedLong(term.bytes[offset + o]);
      }
      l <<= (Long.BYTES - length) << 3;
      return l;
    }
  }

  final int subIndex;
  private TermsEnum termsEnum;
  private BytesRef currentTerm;
  private int prefixLength;
  private long currentTermPrefix8;

  TermsEnumIndex(TermsEnum termsEnum, int subIndex) {
    this.termsEnum = termsEnum;
    this.subIndex = subIndex;
  }

  int docFreq() throws IOException {
    return termsEnum.docFreq();
  }

  long totalTermFreq() throws IOException {
    return termsEnum.totalTermFreq();
  }

  PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
    return termsEnum.postings(reuse, flags);
  }

  BytesRef term() {
    return currentTerm;
  }

  // TODO: remove
  @Deprecated
  TermsEnum termsEnum() {
    return termsEnum;
  }

  @Deprecated
  void reset(TermsEnum te, BytesRef term) {
    termsEnum = te;
    currentTerm = term;
  }

  private void setTerm(BytesRef term) {
    currentTerm = term;
    if (currentTerm == null) {
      currentTermPrefix8 = 0;
    } else {
      currentTermPrefix8 = prefix8ToComparableUnsignedLong(currentTerm, prefixLength);
    }
  }

  BytesRef next() throws IOException {
    BytesRef term = termsEnum.next();
    setTerm(term);
    return term;
  }

  SeekStatus seekCeil(BytesRef term) throws IOException {
    SeekStatus status = termsEnum.seekCeil(term);
    if (status == SeekStatus.END) {
      setTerm(null);
    } else {
      setTerm(termsEnum.term());
    }
    return status;
  }

  boolean seekExact(BytesRef term) throws IOException {
    boolean found = termsEnum.seekExact(term);
    if (found) {
      setTerm(termsEnum.term());
    } else {
      setTerm(null);
    }
    return found;
  }

  void reset(TermsEnumIndex tei) throws IOException {
    termsEnum = tei.termsEnum;
    setTerm(tei.currentTerm);
  }

  void setPrefixLength(int prefixLength) {
    this.prefixLength = prefixLength;
    currentTermPrefix8 = prefix8ToComparableUnsignedLong(currentTerm, prefixLength);
  }

  int compareTermTo(TermsEnumIndex that) {
    assert currentTermPrefix8 == prefix8ToComparableUnsignedLong(currentTerm, prefixLength);
    assert that.currentTermPrefix8 == prefix8ToComparableUnsignedLong(that.currentTerm, prefixLength);

    assert Arrays.equals(
        currentTerm.bytes,
        currentTerm.offset,
        currentTerm.offset + prefixLength,
        that.currentTerm.bytes,
        that.currentTerm.offset,
        that.currentTerm.offset + prefixLength);

    if (currentTermPrefix8 != that.currentTermPrefix8) {
      int cmp = Long.compareUnsigned(currentTermPrefix8, that.currentTermPrefix8);
      assert Integer.signum(cmp)
          == Integer.signum(
              Arrays.compareUnsigned(
                  currentTerm.bytes,
                  currentTerm.offset + prefixLength,
                  currentTerm.offset + currentTerm.length,
                  that.currentTerm.bytes,
                  that.currentTerm.offset + prefixLength,
                  that.currentTerm.offset + that.currentTerm.length));
      return cmp;
    }

    return Arrays.compareUnsigned(
        currentTerm.bytes,
        currentTerm.offset + prefixLength,
        currentTerm.offset + currentTerm.length,
        that.currentTerm.bytes,
        that.currentTerm.offset + prefixLength,
        that.currentTerm.offset + that.currentTerm.length);
  }

  @Override
  public String toString() {
    return Objects.toString(termsEnum);
  }
}
