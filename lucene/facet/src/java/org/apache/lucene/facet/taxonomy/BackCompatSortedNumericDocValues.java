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
package org.apache.lucene.facet.taxonomy;

import java.io.IOException;
import java.util.function.BiConsumer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;

/**
 * Wraps a {@link BinaryDocValues} instance, providing a {@link SortedNumericDocValues} interface
 * for the purpose of being backwards-compatible. (see: LUCENE-10062)
 *
 * @deprecated Only here for back-compat support. Should be removed with Lucene 10.
 */
@Deprecated
public class BackCompatSortedNumericDocValues extends SortedNumericDocValues {
  private final BinaryDocValues binaryDocValues;
  private final BiConsumer<BytesRef, IntsRef> binaryValueDecoder;
  private final IntsRef scratch = new IntsRef();
  private int curr;

  /**
   * Wrap the provided binary encoded doc values. Decodes the binary values with the provided {@code
   * binaryValueDecoder}, allowing the default decoding behavior to be overridden. If a null doc
   * values instance is provided, the returned instance will also be null. If a null value decoder
   * is specified, the default encoding will be assumed.
   */
  public static SortedNumericDocValues wrap(
      BinaryDocValues binaryDocValues, BiConsumer<BytesRef, IntsRef> binaryValueDecoder) {
    if (binaryDocValues == null) {
      return null;
    }

    return new BackCompatSortedNumericDocValues(binaryDocValues, binaryValueDecoder);
  }

  /** see the static {@code wrap} methods */
  private BackCompatSortedNumericDocValues(
      BinaryDocValues binaryDocValues, BiConsumer<BytesRef, IntsRef> binaryValueDecoder) {
    assert binaryDocValues != null;
    this.binaryDocValues = binaryDocValues;

    if (binaryValueDecoder != null) {
      this.binaryValueDecoder = binaryValueDecoder;
    } else {
      this.binaryValueDecoder = BackCompatSortedNumericDocValues::loadValues;
    }
  }

  @Override
  public boolean advanceExact(int target) throws IOException {
    boolean result = binaryDocValues.advanceExact(target);
    if (result) {
      reloadValues();
    }
    return result;
  }

  @Override
  public long nextValue() throws IOException {
    curr++;
    assert curr < scratch.length;
    return scratch.ints[scratch.offset + curr];
  }

  @Override
  public int docValueCount() {
    return scratch.length;
  }

  @Override
  public int docID() {
    return binaryDocValues.docID();
  }

  @Override
  public int nextDoc() throws IOException {
    return advance(binaryDocValues.docID() + 1);
  }

  @Override
  public int advance(int target) throws IOException {
    int doc = binaryDocValues.advance(target);
    if (doc != NO_MORE_DOCS) {
      reloadValues();
    }
    return doc;
  }

  @Override
  public long cost() {
    return binaryDocValues.cost();
  }

  private void reloadValues() throws IOException {
    curr = -1;
    binaryValueDecoder.accept(binaryDocValues.binaryValue(), scratch);
  }

  /** Load ordinals for the currently-positioned doc, assuming the default binary encoding. */
  static void loadValues(BytesRef buf, IntsRef ordinals) {
    // grow the buffer up front, even if by a large number of values (buf.length)
    // that saves the need to check inside the loop for every decoded value if
    // the buffer needs to grow.
    if (ordinals.ints.length < buf.length) {
      ordinals.ints = ArrayUtil.grow(ordinals.ints, buf.length);
    }

    ordinals.offset = 0;
    ordinals.length = 0;

    // it is better if the decoding is inlined like so, and not e.g.
    // in a utility method
    int upto = buf.offset + buf.length;
    int value = 0;
    int offset = buf.offset;
    int prev = 0;
    while (offset < upto) {
      byte b = buf.bytes[offset++];
      if (b >= 0) {
        ordinals.ints[ordinals.length] = ((value << 7) | b) + prev;
        value = 0;
        prev = ordinals.ints[ordinals.length];
        ordinals.length++;
      } else {
        value = (value << 7) | (b & 0x7F);
      }
    }
  }
}
