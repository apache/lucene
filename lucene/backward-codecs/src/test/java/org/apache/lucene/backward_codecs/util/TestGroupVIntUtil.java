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
package org.apache.lucene.backward_codecs.util;

import java.io.IOException;
import org.apache.lucene.backward_codecs.store.DataOutputUtil;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestGroupVIntUtil extends LuceneTestCase {

  public void testLongArrayRoundTrip() throws IOException {
    long[] original = {1L, 127L, 128L, 16383L, 16384L, 2097151L, 2097152L, 268435455L};
    
    // Write using the backward-codecs utility
    ByteArrayDataOutput out = new ByteArrayDataOutput();
    DataOutputUtil.writeGroupVInts(out, original, original.length);
    
    // Read back using the backward-codecs utility
    ByteArrayDataInput in = new ByteArrayDataInput(out.toArrayCopy());
    long[] result = new long[original.length];
    GroupVIntUtil.readGroupVInts(in, result, original.length);
    
    assertArrayEquals(original, result);
  }

  public void testSingleGroupVInt() throws IOException {
    long[] original = {1L, 2L, 3L, 4L};
    
    ByteArrayDataOutput out = new ByteArrayDataOutput();
    byte[] scratch = new byte[GroupVIntUtil.MAX_LENGTH_PER_GROUP];
    GroupVIntUtil.writeGroupVInts(out, scratch, original, original.length);
    
    ByteArrayDataInput in = new ByteArrayDataInput(out.toArrayCopy());
    long[] result = new long[original.length];
    GroupVIntUtil.readGroupVInt(in, result, 0);
    
    assertArrayEquals(original, result);
  }
}