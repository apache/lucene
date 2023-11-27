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
package org.apache.lucene.util.fst;

import java.io.IOException;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

/**
 * Enumerates all input (BytesRef) + output pairs in a {@link PrimitiveLongFST}.
 *
 * @lucene.experimental
 */
public final class BytesRefPrimitiveLongFSTEnum extends PrimitiveLongFSTEnum {
  private final BytesRef current = new BytesRef(10);
  private final InputOutput result = new InputOutput();
  private BytesRef target;

  /** Holds a single input (BytesRef) + output pair. */
  public static class InputOutput {
    public BytesRef input;
    public long output;
  }

  /**
   * doFloor controls the behavior of advance: if it's true doFloor is true, advance positions to
   * the biggest term before target.
   */
  public BytesRefPrimitiveLongFSTEnum(PrimitiveLongFST fst) {
    super(fst);
    result.input = current;
    current.offset = 1;
  }

  public InputOutput current() {
    return result;
  }

  public InputOutput next() throws IOException {
    // System.out.println("  enum.next");
    doNext();
    return setResult();
  }

  /** Seeks to smallest term that's &gt;= target. */
  public InputOutput seekCeil(BytesRef target) throws IOException {
    this.target = target;
    targetLength = target.length;
    super.doSeekCeil();
    return setResult();
  }

  /** Seeks to biggest term that's &lt;= target. */
  public InputOutput seekFloor(BytesRef target) throws IOException {
    this.target = target;
    targetLength = target.length;
    super.doSeekFloor();
    return setResult();
  }

  /**
   * Seeks to exactly this term, returning null if the term doesn't exist. This is faster than using
   * {@link #seekFloor} or {@link #seekCeil} because it short-circuits as soon the match is not
   * found.
   */
  public InputOutput seekExact(BytesRef target) throws IOException {
    this.target = target;
    targetLength = target.length;
    if (doSeekExact()) {
      assert upto == 1 + target.length;
      return setResult();
    } else {
      return null;
    }
  }

  @Override
  protected int getTargetLabel() {
    if (upto - 1 == target.length) {
      return FST.END_LABEL;
    } else {
      return target.bytes[target.offset + upto - 1] & 0xFF;
    }
  }

  @Override
  protected int getCurrentLabel() {
    // current.offset fixed at 1
    return current.bytes[upto] & 0xFF;
  }

  @Override
  protected void setCurrentLabel(int label) {
    current.bytes[upto] = (byte) label;
  }

  @Override
  protected void grow() {
    current.bytes = ArrayUtil.grow(current.bytes, upto + 1);
  }

  private InputOutput setResult() {
    if (upto == 0) {
      result.output = -1;
    } else {
      current.length = upto - 1;
      result.output = output[upto];
    }
    return result;
  }
}
