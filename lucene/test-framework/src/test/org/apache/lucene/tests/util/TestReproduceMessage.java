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
package org.apache.lucene.tests.util;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.is;

import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.JUnitCore;
import org.junit.runners.model.Statement;

/** Test reproduce message is right. */
public class TestReproduceMessage extends WithNestedTests {
  @SuppressWarnings("NonFinalStaticField")
  public static SorePoint where;

  @SuppressWarnings("NonFinalStaticField")
  public static SoreType type;

  public static class Nested extends AbstractNestedTest {
    @BeforeClass
    public static void beforeClass() {
      if (isRunningNested()) {
        triggerOn(SorePoint.BEFORE_CLASS);
      }
    }

    @Rule
    public TestRule rule =
        (base, _) ->
            new Statement() {
              @Override
              public void evaluate() throws Throwable {
                triggerOn(SorePoint.RULE);
                base.evaluate();
              }
            };

    /** Class initializer block/ default constructor. */
    public Nested() {
      triggerOn(SorePoint.INITIALIZER);
    }

    @Before
    public void before() {
      triggerOn(SorePoint.BEFORE);
    }

    @Test
    public void test() {
      triggerOn(SorePoint.TEST);
    }

    @After
    public void after() {
      triggerOn(SorePoint.AFTER);
    }

    @AfterClass
    public static void afterClass() {
      if (isRunningNested()) {
        triggerOn(SorePoint.AFTER_CLASS);
      }
    }

    /** */
    private static void triggerOn(SorePoint pt) {
      if (pt == where) {
        switch (type) {
          case ASSUMPTION:
            LuceneTestCase.assumeTrue(pt.toString(), false);
            throw new RuntimeException("unreachable");
          case ERROR:
            throw new RuntimeException(pt.toString());
          case FAILURE:
            fail(pt.toString());
            throw new RuntimeException("unreachable");
        }
      }
    }
  }

  /*
   * ASSUMPTIONS.
   */

  public TestReproduceMessage() {
    super(true);
  }

  @Test
  public void testAssumeBeforeClass() throws Exception {
    type = SoreType.ASSUMPTION;
    where = SorePoint.BEFORE_CLASS;
    MatcherAssert.assertThat(runAndReturnSyserr(), is(emptyString()));
  }

  @Test
  public void testAssumeInitializer() throws Exception {
    type = SoreType.ASSUMPTION;
    where = SorePoint.INITIALIZER;
    MatcherAssert.assertThat(runAndReturnSyserr(), is(emptyString()));
  }

  @Test
  public void testAssumeRule() throws Exception {
    type = SoreType.ASSUMPTION;
    where = SorePoint.RULE;
    MatcherAssert.assertThat(runAndReturnSyserr(), is(emptyString()));
  }

  @Test
  public void testAssumeBefore() throws Exception {
    type = SoreType.ASSUMPTION;
    where = SorePoint.BEFORE;
    MatcherAssert.assertThat(runAndReturnSyserr(), is(emptyString()));
  }

  @Test
  public void testAssumeTest() throws Exception {
    type = SoreType.ASSUMPTION;
    where = SorePoint.TEST;
    MatcherAssert.assertThat(runAndReturnSyserr(), is(emptyString()));
  }

  @Test
  public void testAssumeAfter() throws Exception {
    type = SoreType.ASSUMPTION;
    where = SorePoint.AFTER;
    MatcherAssert.assertThat(runAndReturnSyserr(), is(emptyString()));
  }

  @Test
  public void testAssumeAfterClass() throws Exception {
    type = SoreType.ASSUMPTION;
    where = SorePoint.AFTER_CLASS;
    MatcherAssert.assertThat(runAndReturnSyserr(), is(emptyString()));
  }

  /*
   * FAILURES
   */

  @Test
  public void testFailureBeforeClass() throws Exception {
    type = SoreType.FAILURE;
    where = SorePoint.BEFORE_CLASS;
    MatcherAssert.assertThat(runAndReturnSyserr(), containsString("NOTE: reproduce with:"));
  }

  @Test
  public void testFailureInitializer() throws Exception {
    type = SoreType.FAILURE;
    where = SorePoint.INITIALIZER;
    MatcherAssert.assertThat(runAndReturnSyserr(), containsString("NOTE: reproduce with:"));
  }

  static void checkTestName(String syserr, String expectedName) {
    MatcherAssert.assertThat(syserr, containsString("NOTE: reproduce with:"));
    MatcherAssert.assertThat(syserr, containsString(" --tests " + expectedName));
  }

  @Test
  public void testFailureRule() throws Exception {
    type = SoreType.FAILURE;
    where = SorePoint.RULE;
    checkTestName(runAndReturnSyserr(), Nested.class.getSimpleName() + ".test");
  }

  @Test
  public void testFailureBefore() throws Exception {
    type = SoreType.FAILURE;
    where = SorePoint.BEFORE;
    checkTestName(runAndReturnSyserr(), Nested.class.getSimpleName() + ".test");
  }

  @Test
  public void testFailureTest() throws Exception {
    type = SoreType.FAILURE;
    where = SorePoint.TEST;
    checkTestName(runAndReturnSyserr(), Nested.class.getSimpleName() + ".test");
  }

  @Test
  public void testFailureAfter() throws Exception {
    type = SoreType.FAILURE;
    where = SorePoint.AFTER;
    checkTestName(runAndReturnSyserr(), Nested.class.getSimpleName() + ".test");
  }

  @Test
  public void testFailureAfterClass() throws Exception {
    type = SoreType.FAILURE;
    where = SorePoint.AFTER_CLASS;
    MatcherAssert.assertThat(runAndReturnSyserr(), containsString("NOTE: reproduce with:"));
  }

  /*
   * ERRORS
   */

  @Test
  public void testErrorBeforeClass() throws Exception {
    type = SoreType.ERROR;
    where = SorePoint.BEFORE_CLASS;
    MatcherAssert.assertThat(runAndReturnSyserr(), containsString("NOTE: reproduce with:"));
  }

  @Test
  public void testErrorInitializer() throws Exception {
    type = SoreType.ERROR;
    where = SorePoint.INITIALIZER;
    MatcherAssert.assertThat(runAndReturnSyserr(), containsString("NOTE: reproduce with:"));
  }

  @Test
  public void testErrorRule() throws Exception {
    type = SoreType.ERROR;
    where = SorePoint.RULE;
    checkTestName(runAndReturnSyserr(), Nested.class.getSimpleName() + ".test");
  }

  @Test
  public void testErrorBefore() throws Exception {
    type = SoreType.ERROR;
    where = SorePoint.BEFORE;
    checkTestName(runAndReturnSyserr(), Nested.class.getSimpleName() + ".test");
  }

  @Test
  public void testErrorTest() throws Exception {
    type = SoreType.ERROR;
    where = SorePoint.TEST;
    checkTestName(runAndReturnSyserr(), Nested.class.getSimpleName() + ".test");
  }

  @Test
  public void testErrorAfter() throws Exception {
    type = SoreType.ERROR;
    where = SorePoint.AFTER;
    checkTestName(runAndReturnSyserr(), Nested.class.getSimpleName() + ".test");
  }

  @Test
  public void testErrorAfterClass() throws Exception {
    type = SoreType.ERROR;
    where = SorePoint.AFTER_CLASS;
    MatcherAssert.assertThat(runAndReturnSyserr(), containsString("NOTE: reproduce with:"));
  }

  private String runAndReturnSyserr() {
    JUnitCore.runClasses(Nested.class);
    return getSysErr();
  }
}
