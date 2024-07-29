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
package org.apache.lucene.codecs.lucene912;

import java.io.IOException;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.tests.index.BasePostingsFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;

public class TestLucene912PostingsFormat extends BasePostingsFormatTestCase {

  @Override
  protected Codec getCodec() {
    return TestUtil.alwaysPostingsFormat(new Lucene912PostingsFormat());
  }

  public void testVInt15() throws IOException {
    byte[] bytes = new byte[5];
    ByteArrayDataOutput out = new ByteArrayDataOutput(bytes);
    ByteArrayDataInput in = new ByteArrayDataInput();
    for (int i : new int[] {0, 1, 127, 128, 32767, 32768, Integer.MAX_VALUE}) {
      out.reset(bytes);
      Lucene912PostingsWriter.writeVInt15(out, i);
      in.reset(bytes, 0, out.getPosition());
      assertEquals(i, Lucene912PostingsReader.readVInt15(in));
      assertEquals(out.getPosition(), in.getPosition());
    }
  }

  public void testVLong15() throws IOException {
    byte[] bytes = new byte[9];
    ByteArrayDataOutput out = new ByteArrayDataOutput(bytes);
    ByteArrayDataInput in = new ByteArrayDataInput();
    for (long i : new long[] {0, 1, 127, 128, 32767, 32768, Integer.MAX_VALUE, Long.MAX_VALUE}) {
      out.reset(bytes);
      Lucene912PostingsWriter.writeVLong15(out, i);
      in.reset(bytes, 0, out.getPosition());
      assertEquals(i, Lucene912PostingsReader.readVLong15(in));
      assertEquals(out.getPosition(), in.getPosition());
    }
  }
}
