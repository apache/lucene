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

import static com.carrotsearch.randomizedtesting.generators.RandomNumbers.randomIntBetween;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.packed.PackedInts;

public class TestForUtil90Vectorization extends BaseVectorizationTestCase {

  int[] encodedInts1 = new int[ForUtil90.BLOCK_SIZE];
  int[] encodedInts2 = new int[ForUtil90.BLOCK_SIZE];
  int[] decodedInts1 = new int[ForUtil90.BLOCK_SIZE];
  int[] decodedInts2 = new int[ForUtil90.BLOCK_SIZE];
  byte[] encoded1 = new byte[ForUtil90.BLOCK_SIZE * 4];
  byte[] encoded2 = new byte[ForUtil90.BLOCK_SIZE * 4];
  ByteArrayDataOutput out1 = new ByteArrayDataOutput(encoded1);
  ByteArrayDataOutput out2 = new ByteArrayDataOutput(encoded2);
  ByteArrayDataInput in1 = new ByteArrayDataInput(encoded1);
  ByteArrayDataInput in2 = new ByteArrayDataInput(encoded2);

  public void testEncodeDecode() throws Exception {
    ForUtil90 defaultForUtil = LUCENE_PROVIDER.newForUtil90();
    ForUtil90 panamaForUtil = PANAMA_PROVIDER.newForUtil90();

    final int iterations = randomIntBetween(random(), 50, 1000);
    final int[] input = new int[ForUtil90.BLOCK_SIZE];

    for (int i = 0; i < iterations; ++i) {
      final int bpv = TestUtil.nextInt(random(), 1, 31);
      for (int j = 0; j < ForUtil90.BLOCK_SIZE; ++j) {
        input[j] = randomIntBetween(random(), 0, (int) PackedInts.maxValue(bpv));
      }

      // encode
      defaultForUtil.encode(input, bpv, out1);
      panamaForUtil.encode(input, bpv, out2);
      in1.readInts(encodedInts1, 0, ForUtil90.BLOCK_SIZE);
      in2.readInts(encodedInts2, 0, ForUtil90.BLOCK_SIZE);
      assertArrayEquals(encodedInts1, encodedInts2);
      out1.reset(encoded1);
      out2.reset(encoded2);
      in1.reset(encoded1);
      in2.reset(encoded2);

      // decode
      defaultForUtil.decode(bpv, in1, decodedInts1);
      panamaForUtil.decode(bpv, in2, decodedInts2);
      assertArrayEquals(input, decodedInts1);
      assertArrayEquals(decodedInts1, decodedInts2);
      in1.reset(encoded1);
      in2.reset(encoded2);
    }
  }
}
