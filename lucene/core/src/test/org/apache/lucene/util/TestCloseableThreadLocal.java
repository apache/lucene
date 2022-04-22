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
package org.apache.lucene.util;

import org.apache.lucene.tests.util.LuceneTestCase;

import java.util.concurrent.atomic.AtomicBoolean;

public class TestCloseableThreadLocal extends LuceneTestCase {
  public static final String TEST_VALUE = "initvaluetest";

  public void testInitValue() {
    InitValueThreadLocal tl = new InitValueThreadLocal();
    String str = (String) tl.get();
    assertEquals(TEST_VALUE, str);
  }

  public void testNullValue() throws Exception {
    // Tests that null can be set as a valid value (LUCENE-1805). This
    // previously failed in get().
    CloseableThreadLocal<Object> ctl = new CloseableThreadLocal<>();
    ctl.set(null);
    assertNull(ctl.get());
  }

  public void testDefaultValueWithoutSetting() throws Exception {
    // LUCENE-1805: make sure default get returns null,
    // twice in a row
    CloseableThreadLocal<Object> ctl = new CloseableThreadLocal<>();
    assertNull(ctl.get());
  }

  public static class InitValueThreadLocal extends CloseableThreadLocal<Object> {
    @Override
    protected Object initialValue() {
      return TEST_VALUE;
    }
  }


  public void testSetGetValueWithMultiThreads() {
    final int CONCURRENT_THREADS = 5;
    final int LOOPS = 10000;
    final CloseableThreadLocal<String> ctl1 = new CloseableThreadLocal();
    final CloseableThreadLocal<Integer> ctl2 = new CloseableThreadLocal();

    final Thread[] threads = new Thread[CONCURRENT_THREADS];
    final AtomicBoolean[] results = new AtomicBoolean[CONCURRENT_THREADS];
    for (int i = 0; i < CONCURRENT_THREADS; i++) {
      final String TNAME = "T-" + i;
      final int BASE = (i + 1) * 100000;
      AtomicBoolean result = new AtomicBoolean(true);
      results[i] = result;

      threads[i] = new Thread(() -> {
        String lastValue1 = null;
        int lastValue2 = -1;
        for (int j = 0; j < LOOPS; j++) {
          if (j > 0) {
            if (!lastValue1.equals(ctl1.get()) || ctl2.get() != lastValue2) {
              result.set(false);
              break;
            }
          }

          String value1 = TNAME + "-" + j;
          int value2 = BASE + j;
          ctl1.set(value1);
          ctl2.set(value2);

          lastValue1 = value1;
          lastValue2 = value2;
        }

      });
      threads[i].setName(TNAME);
      threads[i].start();
    }
    for (Thread t : threads) {
      try {
        t.join();
      } catch (InterruptedException e) {
      }
    }
    for (AtomicBoolean r : results) {
      assertTrue(r.get());
    }

    // Set Value from current thread.
    ctl1.set("Value-FromCurrentThread");
    ctl2.set(123456789);

    // Check values after force purge. Expecting all the dead threads values get cleared except current thread.
    assertEquals(1, ctl1.getValuesAfterPurge().size());
    assertEquals(1, ctl2.getValuesAfterPurge().size());

    ctl1.close();
    ctl2.close();
  }
}
