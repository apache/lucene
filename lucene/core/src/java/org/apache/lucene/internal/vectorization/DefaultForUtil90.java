// This file has been automatically generated, DO NOT EDIT

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
package org.apache.lucene.internal.vectorization;

import java.io.IOException;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;

final class DefaultForUtil90 implements ForUtil90 {
  private final int[] tmp = new int[BLOCK_SIZE];

  private final int[] decoded = new int[BLOCK_SIZE];

  private static final int totalBits = 32;

  /** Encode 128 integers from {@code input} into {@code out}. */
  @Override
  public void encode(int[] input, int bitsPerValue, DataOutput out) throws IOException {
    int MASK = (1 << bitsPerValue) - 1;
    int cur = 0;
    int bitsRemaining = totalBits;
    tmp[0] = 0;
    tmp[1] = 0;
    tmp[2] = 0;
    tmp[3] = 0;
    for (int i = 0; i < input.length; i += 4) {
      tmp[cur] |= (input[i] & MASK) << (totalBits - bitsRemaining);
      tmp[cur + 1] |= (input[i + 1] & MASK) << (totalBits - bitsRemaining);
      tmp[cur + 2] |= (input[i + 2] & MASK) << (totalBits - bitsRemaining);
      tmp[cur + 3] |= (input[i + 3] & MASK) << (totalBits - bitsRemaining);

      bitsRemaining -= bitsPerValue;

      if (bitsRemaining <= 0) {
        cur += 4;
        tmp[cur] = ((input[i] & MASK) >>> (bitsPerValue + bitsRemaining));
        tmp[cur + 1] = ((input[i + 1] & MASK) >>> (bitsPerValue + bitsRemaining));
        tmp[cur + 2] = ((input[i + 2] & MASK) >>> (bitsPerValue + bitsRemaining));
        tmp[cur + 3] = ((input[i + 3] & MASK) >>> (bitsPerValue + bitsRemaining));
        bitsRemaining += totalBits;
      }
    }

    for (int i = 0; i < 4 * bitsPerValue; ++i) {
      out.writeInt(tmp[i]);
    }
  }

  /** Decode 128 integers into {@code output}. */
  @Override
  public void decode(int bitsPerValue, DataInput in, int[] output) throws IOException {
    in.readInts(tmp, 0, 4 * bitsPerValue);
    int MASK = (1 << bitsPerValue) - 1;
    int cur = 0;
    int bitsRemaining = totalBits;

    for (int i = 0; i < 128; i += 4) {
      output[i] = tmp[cur] & MASK;
      output[i + 1] = tmp[cur + 1] & MASK;
      output[i + 2] = tmp[cur + 2] & MASK;
      output[i + 3] = tmp[cur + 3] & MASK;

      tmp[cur] >>>= bitsPerValue;
      tmp[cur + 1] >>>= bitsPerValue;
      tmp[cur + 2] >>>= bitsPerValue;
      tmp[cur + 3] >>>= bitsPerValue;
      bitsRemaining -= bitsPerValue;

      if (bitsRemaining <= 0) {
        cur += 4;
        int PART_MASK = (1 << (-bitsRemaining)) - 1;
        output[i] |= ((tmp[cur] & PART_MASK) << (bitsRemaining + bitsPerValue));
        output[i + 1] |= ((tmp[cur + 1] & PART_MASK) << (bitsRemaining + bitsPerValue));
        output[i + 2] |= ((tmp[cur + 2] & PART_MASK) << (bitsRemaining + bitsPerValue));
        output[i + 3] |= ((tmp[cur + 3] & PART_MASK) << (bitsRemaining + bitsPerValue));

        tmp[cur] >>>= (-bitsRemaining);
        tmp[cur + 1] >>>= (-bitsRemaining);
        tmp[cur + 2] >>>= (-bitsRemaining);
        tmp[cur + 3] >>>= (-bitsRemaining);
        bitsRemaining += totalBits;
      }
    }
    assert cur == 4 * bitsPerValue;
  }

  /**
   * Decodes 128 integers into 64 {@code longs} such that each long contains two values, each
   * represented with 32 bits. Values [0..63] are encoded in the high-order bits of {@code longs}
   * [0..63], and values [64..127] are encoded in the low-order bits of {@code longs} [0..63]. This
   * representation may allow subsequent operations to be performed on two values at a time.
   */
  @Override
  public void decodeTo32(int bitsPerValue, DataInput in, long[] longs) throws IOException {
    decode(bitsPerValue, in, decoded);
    for (int i = 0; i < 64; ++i) {
      longs[i] = (long) decoded[i] << 32 | decoded[i + 64];
    }
  }
}
