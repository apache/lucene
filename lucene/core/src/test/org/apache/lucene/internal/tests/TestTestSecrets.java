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
package org.apache.lucene.internal.tests;

import org.apache.lucene.tests.util.LuceneTestCase;

public class TestTestSecrets extends LuceneTestCase {

  public void testCallerOfGetter() {
    final UnsupportedOperationException expected =
        expectThrows(UnsupportedOperationException.class, TestTestSecrets::illegalCaller);
    assertEquals(
        "Lucene TestSecrets can only be used by the test-framework.", expected.getMessage());
  }

  private static void illegalCaller() {
    TestSecrets.getIndexWriterAccess();
  }

  public void testCannotSet() {
    expectThrows(AssertionError.class, () -> TestSecrets.setIndexWriterAccess(null));
    expectThrows(AssertionError.class, () -> TestSecrets.setConcurrentMergeSchedulerAccess(null));
    expectThrows(AssertionError.class, () -> TestSecrets.setIndexPackageAccess(null));
    expectThrows(AssertionError.class, () -> TestSecrets.setSegmentReaderAccess(null));
  }
}
