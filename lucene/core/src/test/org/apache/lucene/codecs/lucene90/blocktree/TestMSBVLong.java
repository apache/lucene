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
package org.apache.lucene.codecs.lucene90.blocktree;

import java.io.IOException;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.ArrayUtil;

public class TestMSBVLong extends LuceneTestCase {

  public void testMSBVLong() throws IOException {
    assertMSBVLong(Long.MAX_VALUE);
    int iter = atLeast(10000);
    for (long i = 0; i < iter; i++) {
      assertMSBVLong(i);
    }
  }

  private static void assertMSBVLong(long l) throws IOException {
    byte[] bytes = new byte[10];
    ByteArrayDataOutput output = new ByteArrayDataOutput(bytes);
    Lucene90BlockTreeTermsWriter.writeMSBVLong(l, output);
    ByteArrayDataInput in =
        new ByteArrayDataInput(ArrayUtil.copyOfSubArray(bytes, 0, output.getPosition()));
    long recovered = FieldReader.readMSBVLong(in);
    assertEquals(l + " != " + recovered, l, recovered);
  }
}
