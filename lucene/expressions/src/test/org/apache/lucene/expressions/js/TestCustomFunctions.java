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
package org.apache.lucene.expressions.js;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;
import java.text.ParseException;
import java.util.Collections;
import java.util.Map;
import org.apache.lucene.expressions.Expression;
import org.apache.lucene.tests.util.LuceneTestCase;

/** Tests customing the function map */
public class TestCustomFunctions extends CompilerTestCase {
  private static final double DELTA = 0.0000001;
  private static final Lookup LOOKUP = MethodHandles.lookup();

  /** empty list of methods */
  public void testEmpty() throws Exception {
    Map<String, MethodHandle> functions = Map.of();
    ParseException expected =
        expectThrows(
            ParseException.class,
            () -> {
              compile("sqrt(20)", functions);
            });
    assertEquals(
        "Invalid expression 'sqrt(20)': Unrecognized function call (sqrt).", expected.getMessage());
    assertEquals(expected.getErrorOffset(), 0);
  }

  /** using the default map explicitly */
  public void testDefaultList() throws Exception {
    Map<String, MethodHandle> functions = JavascriptCompiler.DEFAULT_FUNCTIONS;
    Expression expr = compile("sqrt(20)", functions);
    assertEquals(Math.sqrt(20), expr.evaluate(null), DELTA);
  }

  public static double zeroArgMethod() {
    return 5;
  }

  private static MethodHandle localMethod(String name, MethodType type)
      throws NoSuchMethodException, IllegalAccessException {
    return LOOKUP.findStatic(LOOKUP.lookupClass(), name, type);
  }

  private static MethodHandle localMethod(String name, int arity)
      throws NoSuchMethodException, IllegalAccessException {
    return localMethod(
        name, MethodType.methodType(double.class, Collections.nCopies(arity, double.class)));
  }

  /** tests a method with no arguments */
  public void testNoArgMethod() throws Exception {
    Map<String, MethodHandle> functions = Map.of("foo", localMethod("zeroArgMethod", 0));
    Expression expr = compile("foo()", functions);
    assertEquals(5, expr.evaluate(null), DELTA);
  }

  public static double oneArgMethod(double arg1) {
    return 3 + arg1;
  }

  /** tests a method with one arguments */
  public void testOneArgMethod() throws Exception {
    Map<String, MethodHandle> functions = Map.of("foo", localMethod("oneArgMethod", 1));
    Expression expr = compile("foo(3)", functions);
    assertEquals(6, expr.evaluate(null), DELTA);
  }

  public static double threeArgMethod(double arg1, double arg2, double arg3) {
    return arg1 + arg2 + arg3;
  }

  /** tests a method with three arguments */
  public void testThreeArgMethod() throws Exception {
    Map<String, MethodHandle> functions = Map.of("foo", localMethod("threeArgMethod", 3));
    Expression expr = compile("foo(3, 4, 5)", functions);
    assertEquals(12, expr.evaluate(null), DELTA);
  }

  /** tests a map with 2 functions */
  public void testTwoMethods() throws Exception {
    Map<String, MethodHandle> functions =
        Map.of("foo", localMethod("zeroArgMethod", 0), "bar", localMethod("oneArgMethod", 1));
    Expression expr = compile("foo() + bar(3)", functions);
    assertEquals(11, expr.evaluate(null), DELTA);
  }

  /** tests invalid methods that are not allowed to become variables to be mapped */
  public void testInvalidVariableMethods() {
    ParseException expected =
        expectThrows(
            ParseException.class,
            () -> {
              compile("method()");
            });
    assertEquals(
        "Invalid expression 'method()': Unrecognized function call (method).",
        expected.getMessage());
    assertEquals(0, expected.getErrorOffset());

    expected =
        expectThrows(
            ParseException.class,
            () -> {
              compile("method.method(1)");
            });
    assertEquals(
        "Invalid expression 'method.method(1)': Unrecognized function call (method.method).",
        expected.getMessage());
    assertEquals(0, expected.getErrorOffset());

    expected =
        expectThrows(
            ParseException.class,
            () -> {
              compile("1 + method()");
            });
    assertEquals(
        "Invalid expression '1 + method()': Unrecognized function call (method).",
        expected.getMessage());
    assertEquals(4, expected.getErrorOffset());
  }

  public static String bogusReturnType() {
    return "bogus!";
  }

  /** wrong return type: must be double */
  public void testWrongReturnType() throws Exception {
    Map<String, MethodHandle> functions =
        Map.of("foo", localMethod("bogusReturnType", MethodType.methodType(String.class)));
    IllegalArgumentException expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              compile("foo()", functions);
            });
    assertTrue(expected.getMessage().contains("does not return a double"));
  }

  public static double bogusParameterType(String s) {
    return 0;
  }

  /** wrong param type: must be doubles */
  public void testWrongParameterType() throws Exception {
    Map<String, MethodHandle> functions =
        Map.of(
            "foo",
            localMethod("bogusParameterType", MethodType.methodType(double.class, String.class)));
    IllegalArgumentException expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              compile("foo(2)", functions);
            });
    assertTrue(expected.getMessage().contains("must take only double parameters"));
  }

  public double nonStaticMethod() {
    return 0;
  }

  /** wrong modifiers: must be static */
  public void testWrongNotStatic() throws Exception {
    Map<String, MethodHandle> functions =
        Map.of(
            "foo",
            LOOKUP.findVirtual(
                LOOKUP.lookupClass(), "nonStaticMethod", MethodType.methodType(double.class)));
    IllegalArgumentException expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              compile("foo()", functions);
            });
    assertTrue(expected.getMessage().contains("is not static"));
  }

  static double nonPublicMethod() {
    return 7;
  }

  /** non public methods work as the lookup allows access */
  public void testNotPublic() throws Exception {
    Map<String, MethodHandle> functions = Map.of("foo", localMethod("nonPublicMethod", 0));
    Expression expr = compile("foo()", functions);
    assertEquals(7, expr.evaluate(null), DELTA);
  }

  static class NestedNotPublic {
    public static double method() {
      return 41;
    }
  }

  /** class containing method is not public */
  public void testNestedNotPublic() throws Exception {
    Map<String, MethodHandle> functions =
        Map.of(
            "foo",
            LOOKUP.findStatic(
                NestedNotPublic.class, "method", MethodType.methodType(double.class)));
    Expression expr = compile("foo()", functions);
    assertEquals(41, expr.evaluate(null), DELTA);
  }

  /** class containing method is not public */
  public void testNonDirectMethodHandle() throws Exception {
    Map<String, MethodHandle> functions =
        Map.of(
            "foo",
            MethodHandles.constant(double.class, 9),
            "bar",
            MethodHandles.identity(double.class));
    Expression expr = compile("foo() + bar(7)", functions);
    assertEquals(16, expr.evaluate(null), DELTA);
  }

  private static final String MESSAGE = "This should not happen but it happens";

  public static double staticThrowingException() {
    throw new ArithmeticException(MESSAGE);
  }

  /**
   * the method throws an exception. We should check the stack trace that it contains the source
   * code of the expression as file name.
   */
  public void testThrowingException() throws Exception {
    Map<String, MethodHandle> functions = Map.of("foo", localMethod("staticThrowingException", 0));
    String source = "3 * foo() / 5";
    Expression expr = compile(source, functions);
    ArithmeticException expected =
        expectThrows(
            ArithmeticException.class,
            () -> {
              expr.evaluate(null);
            });
    assertEquals(MESSAGE, expected.getMessage());
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    expected.printStackTrace(pw);
    pw.flush();
    String exceptionText = sw.toString();
    if (LuceneTestCase.VERBOSE) {
      System.err.println("Exception thrown with full stacktrace:");
      System.err.println(exceptionText);
    }
    assertTrue(exceptionText.contains(expr.getClass().getName() + ".evaluate(" + source + ")"));
  }

  /** test that namespaces work with custom expressions as direct method handle. */
  public void testNamespacesWithDirectMH() throws Exception {
    Map<String, MethodHandle> functions = Map.of("foo.bar", localMethod("zeroArgMethod", 0));
    String source = "foo.bar()";
    Expression expr = compile(source, functions);
    assertEquals(5, expr.evaluate(null), DELTA);
  }

  /**
   * test that namespaces work with general method handles (ensure field name of handle is correct).
   */
  public void testNamespacesWithoutDirectMH() throws Exception {
    Map<String, MethodHandle> functions =
        Map.of(
            "foo.bar",
            MethodHandles.constant(double.class, 9),
            "bar.foo",
            MethodHandles.identity(double.class));
    Expression expr = compile("foo.bar() + bar.foo(7)", functions);
    assertEquals(16, expr.evaluate(null), DELTA);
  }

  public void testLegacyFunctions() throws Exception {
    var functions =
        Map.of("foo", TestCustomFunctions.class.getMethod("oneArgMethod", double.class));
    var newFunctions = JavascriptCompiler.convertLegacyFunctions(functions);
    newFunctions.putAll(JavascriptCompiler.DEFAULT_FUNCTIONS);
    Expression expr = compile("foo(3) + abs(-7)", newFunctions);
    assertEquals(13, expr.evaluate(null), DELTA);
  }

  public void testInvalidLegacyFunctions() throws Exception {
    var functions = Map.of("foo", TestCustomFunctions.class.getMethod("nonStaticMethod"));
    var newFunctions = JavascriptCompiler.convertLegacyFunctions(functions);
    newFunctions.putAll(JavascriptCompiler.DEFAULT_FUNCTIONS);
    expectThrows(IllegalArgumentException.class, () -> compile("foo(3) + abs(-7)", newFunctions));
  }
}
