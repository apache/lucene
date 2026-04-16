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
package org.apache.lucene.search.join;

import java.util.Objects;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.LuceneTestCaseJupiter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 3, unit = TimeUnit.SECONDS)
public class TestJupiter {
  @Nested
  class T1 extends LuceneTestCaseJupiter {
    @Test
    public void t1() throws Exception {
      Random r1 = LuceneTestCase.random();
      Random r2 = LuceneTestCase.random();
      Assertions.assertSame(r1, r2);
      System.out.println("R: " + random().nextLong());
    }

    @Test
    public void t2() throws Exception {
      {
        Random r1 = LuceneTestCase.random();
        System.out.println("R: " + Objects.hashCode(r1) + " " + r1.nextLong());
      }
      var t =
          new Thread(
              () -> {
                Random r1 = LuceneTestCase.random();
                Random r2 = LuceneTestCase.random();
                Assertions.assertSame(r1, r2);
                System.out.println("R: " + Objects.hashCode(r1) + " " + r1.nextLong());
              });
      t.start();
      t.join();
    }
  }

  @Nested
  public class T2 extends LuceneTestCaseJupiter {
    @Test
    public void testNoParent(Random random) throws Exception {}
  }
}
