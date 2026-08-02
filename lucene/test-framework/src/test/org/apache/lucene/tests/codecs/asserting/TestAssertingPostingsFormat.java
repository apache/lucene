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
package org.apache.lucene.tests.codecs.asserting;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.Terms;
import org.apache.lucene.tests.codecs.asserting.AssertingPostingsFormat.AssertingFieldsProducer;
import org.apache.lucene.tests.index.BasePostingsFormatTestCase;

/** Test AssertingPostingsFormat directly */
public class TestAssertingPostingsFormat extends BasePostingsFormatTestCase {
  private final Codec codec = new AssertingCodec();

  @Override
  protected Codec getCodec() {
    return codec;
  }

  @Override
  protected boolean isPostingsEnumReuseImplemented() {
    return false;
  }

  /** A producer that lists exactly the given field names, in the given order. */
  private static FieldsProducer producing(List<String> fields) {
    return new FieldsProducer() {
      @Override
      public Iterator<String> iterator() {
        // a fresh iterator per call, as every real FieldsProducer does
        return List.copyOf(fields).iterator();
      }

      @Override
      public Terms terms(String field) {
        return null;
      }

      @Override
      public int size() {
        return fields.size();
      }

      @Override
      public void checkIntegrity(MergePolicy.OneMerge merge) {}

      @Override
      public void close() {}
    };
  }

  private static void drain(Fields fields) {
    Iterator<String> it = fields.iterator();
    while (it.hasNext()) {
      it.next();
    }
  }

  /**
   * {@link Fields#iterator()} requires ascending order, but nothing in production code checks it:
   * {@link org.apache.lucene.util.MergedIterator}, which is what relies on the order, is documented
   * as undefined for unsorted input, so a violating {@link FieldsProducer} silently yields wrong
   * results. The asserting codec is where the whole test suite gets to catch that.
   */
  public void testRejectsUnsortedFields() throws IOException {
    try (FieldsProducer producer =
        new AssertingFieldsProducer(producing(List.of("zebra", "mid", "alpha")))) {
      AssertionError e = expectThrows(AssertionError.class, () -> drain(producer));
      assertTrue(e.getMessage(), e.getMessage().contains("ascending order"));
      assertTrue(e.getMessage(), e.getMessage().contains("\"zebra\" followed by \"mid\""));
    }
  }

  public void testRejectsDuplicateFields() throws IOException {
    try (FieldsProducer producer =
        new AssertingFieldsProducer(producing(List.of("alpha", "alpha")))) {
      AssertionError e = expectThrows(AssertionError.class, () -> drain(producer));
      assertTrue(e.getMessage(), e.getMessage().contains("no duplicates"));
    }
  }

  public void testAcceptsSortedFields() throws IOException {
    try (FieldsProducer producer =
        new AssertingFieldsProducer(producing(List.of("Z", "alpha", "mid", "zebra")))) {
      drain(producer);
    }
  }

  /** Each call to {@link Fields#iterator()} must track order independently. */
  public void testIteratorsDoNotShareState() throws IOException {
    try (FieldsProducer producer =
        new AssertingFieldsProducer(producing(List.of("alpha", "mid")))) {
      drain(producer);
      // a second pass starts from scratch rather than comparing "alpha" against the previous "mid"
      drain(producer);
    }
  }
}
