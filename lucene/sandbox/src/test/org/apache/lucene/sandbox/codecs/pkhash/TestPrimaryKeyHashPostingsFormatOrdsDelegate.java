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

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.tests.codecs.mockrandom.MockRandomPostingsFormat;
import org.apache.lucene.tests.index.BasePostingsFormatTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Runs the generic postings format tests with {@link MockRandomPostingsFormat} as the delegate,
 * whose terms dictionary, chosen per segment, may support ords: the tests then check {@code ord()}
 * and {@code seekExact(long)} after seeks that go through the hash table.
 */
public class TestPrimaryKeyHashPostingsFormatOrdsDelegate extends BasePostingsFormatTestCase {

  private static Codec codec;

  @BeforeClass
  public static void createCodec() {
    codec =
        TestPrimaryKeyHashPostingsFormat.randomCodec(
            random(), new MockRandomPostingsFormat(random()));
  }

  @AfterClass
  public static void releaseCodec() {
    codec = null;
  }

  @Override
  protected Codec getCodec() {
    return codec;
  }

  /** A merge may choose another terms dictionary of {@link MockRandomPostingsFormat}. */
  @Override
  protected boolean mergeIsStable() {
    return false;
  }
}
