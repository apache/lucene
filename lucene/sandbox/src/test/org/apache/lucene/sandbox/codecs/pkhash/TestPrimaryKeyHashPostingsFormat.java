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

import java.util.List;
import java.util.Random;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashPostingsFormat.Inlining;
import org.apache.lucene.tests.index.BasePostingsFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Runs the generic postings format tests. {@code DOCS} fields get hash tables, with random options
 * and often tiny partitions so that partitioning and spilling are exercised; other fields go to the
 * delegate.
 */
public class TestPrimaryKeyHashPostingsFormat extends BasePostingsFormatTestCase {

  private static Codec codec;

  @BeforeClass
  public static void createCodec() {
    codec = randomCodec(random(), TestUtil.getDefaultPostingsFormat());
  }

  /** A codec that indexes every field with a random hash format around the given delegate. */
  static Codec randomCodec(Random random, PostingsFormat delegate) {
    return TestUtil.alwaysPostingsFormat(
        PrimaryKeyHashTestUtil.randomFormat(
            random,
            delegate,
            PrimaryKeyHashTestUtil.randomFilterBitsPerKey(random),
            PrimaryKeyHashTestUtil.randomInlining(random)));
  }

  @AfterClass
  public static void releaseCodec() {
    codec = null;
  }

  @Override
  protected Codec getCodec() {
    return codec;
  }

  public void testInvalidOptions() {
    final PostingsFormat delegate = TestUtil.getDefaultPostingsFormat();
    expectThrows(NullPointerException.class, () -> new PrimaryKeyHashPostingsFormat(null));
    final int probeLimit = PrimaryKeyHashPostingsFormat.DEFAULT_MAX_PROBE_LIMIT;
    final long recordsLength = PrimaryKeyHashPostingsFormat.MAX_RECORDS_LENGTH;
    assertInvalid("loadFactor", 0.4f, 16, probeLimit, recordsLength);
    assertInvalid("loadFactor", 0.95f, 16, probeLimit, recordsLength);
    assertInvalid("loadFactor", Float.NaN, 16, probeLimit, recordsLength);
    assertInvalid("maxKeysPerPartition", 0.75f, 15, probeLimit, recordsLength);
    assertInvalid("maxKeysPerPartition", 0.75f, (1 << 30) + 1, probeLimit, recordsLength);
    assertInvalid("maxProbeLimit", 0.75f, 16, -1, recordsLength);
    assertInvalid("maxRecordsLength", 0.75f, 16, probeLimit, -1);
    assertInvalid("maxRecordsLength", 0.75f, 16, probeLimit, recordsLength + 1);
    for (int filterBitsPerKey :
        new int[] {
          -1,
          PrimaryKeyHashPostingsFormat.MIN_FILTER_BITS_PER_KEY - 1,
          PrimaryKeyHashPostingsFormat.MAX_FILTER_BITS_PER_KEY + 1
        }) {
      IllegalArgumentException e =
          expectThrows(
              IllegalArgumentException.class,
              () -> new PrimaryKeyHashPostingsFormat(delegate, filterBitsPerKey));
      assertTrue(e.getMessage(), e.getMessage().contains("filterBitsPerKey"));
    }
    for (int filterBitsPerKey :
        new int[] {
          0,
          PrimaryKeyHashPostingsFormat.MIN_FILTER_BITS_PER_KEY,
          PrimaryKeyHashPostingsFormat.MAX_FILTER_BITS_PER_KEY
        }) {
      assertTrue(
          new PrimaryKeyHashPostingsFormat(delegate, filterBitsPerKey)
              .toString()
              .contains("filterBitsPerKey=" + filterBitsPerKey));
    }
    // the constructors without a number of bits per key give a filter of the default one
    for (PrimaryKeyHashPostingsFormat format :
        List.of(new PrimaryKeyHashPostingsFormat(), new PrimaryKeyHashPostingsFormat(delegate))) {
      assertTrue(
          format.toString(),
          format
              .toString()
              .contains(
                  "filterBitsPerKey=" + PrimaryKeyHashPostingsFormat.DEFAULT_FILTER_BITS_PER_KEY));
    }
  }

  private static void assertInvalid(
      String option,
      float loadFactor,
      int maxKeysPerPartition,
      int maxProbeLimit,
      long maxRecordsLength) {
    IllegalArgumentException e =
        expectThrows(
            IllegalArgumentException.class,
            () ->
                new PrimaryKeyHashPostingsFormat(
                    TestUtil.getDefaultPostingsFormat(),
                    loadFactor,
                    maxKeysPerPartition,
                    maxProbeLimit,
                    maxRecordsLength,
                    PrimaryKeyHashPostingsFormat.DEFAULT_FILTER_BITS_PER_KEY,
                    Inlining.IF_NOT_LARGER));
    assertTrue(e.getMessage(), e.getMessage().contains(option));
  }
}
