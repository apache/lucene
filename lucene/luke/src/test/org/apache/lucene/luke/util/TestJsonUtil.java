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

package org.apache.lucene.luke.util;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.text.ParseException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.lucene.tests.util.LuceneTestCase;

/** Test for {@link JsonUtil} */
public class TestJsonUtil extends LuceneTestCase {

  public void testKeyNaming() {
    /*
     Local class for test
    */
    @SuppressWarnings("unused")
    final class FooBar {
      public final int v1;

      public final int camelCase;

      public final int v3;

      private FooBar(int v1, int camelCase, int v3) {
        this.v1 = v1;
        this.camelCase = camelCase;
        this.v3 = v3;
      }
    }

    FooBar fooBar = new FooBar(1, 2, 3);
    assertEquals("{\n  \"v1\":1,\n  \"camel_case\":2,\n  \"v3\":3\n}", JsonUtil.asJson(fooBar, 0));
  }

  public void testPrimitiveMemberAsElement() {
    /*
     * Local class for test
     */
    @SuppressWarnings("unused")
    final class Foo {
      public final String v1;

      public final int v2;

      public final int v3;

      private Foo(String v1, int v2, int v3) {
        this.v1 = v1;
        this.v2 = v2;
        this.v3 = v3;
      }
    }

    Foo foo = new Foo("test", 2, 3);
    assertEquals("{\n  \"v1\":\"test\",\n  \"v2\":2,\n  \"v3\":3\n}", JsonUtil.asJson(foo, 0));
  }

  public void testIgnoreTransientFields() {
    /*
     * Local class for test
     */
    @SuppressWarnings("unused")
    final class FooBar {
      public final int v1;

      public final transient int v2;

      public final transient int v3;

      private FooBar(int v1, int v2, int v3) {
        this.v1 = v1;
        this.v2 = v2;
        this.v3 = v3;
      }
    }

    FooBar fooBar = new FooBar(1, 2, 3);
    assertEquals("{\n  \"v1\":1\n}", JsonUtil.asJson(fooBar, 0));
  }

  public void testSerialize() {
    /* Local class for test */
    @SuppressWarnings("unused")
    final class Target {
      /** A letter */
      enum Letter {
        A,
        B,
        CEE
      }

      public String aString;
      public Integer anInteger;
      public Long aLong;
      public boolean aBoolean;
      public boolean boolean2;
      public Boolean boxedBoolean;
      public Nested nest;
      public Letter aBigLetter;
      public List<String> name;
      public List<Nested> nested;
      public String classy;
      public List<String> breaks;
      public String[] tokens;

      public Map<String, String> stringMap;

      /** A nested class */
      static class Nested {
        public String egg;
        public String bird;
        public String value;
      }
    }
    Target t = new Target();
    t.aString = "\"<&a>\"";
    t.aLong = -12L;
    t.aBoolean = true;
    t.boxedBoolean = true;
    t.aBigLetter = Target.Letter.CEE;
    t.nest = null;
    t.name = Arrays.asList("ted", "jane");
    Target.Nested n1 = new Target.Nested();
    n1.egg = "scrambled";
    n1.bird = "duck";
    n1.value = "100";
    t.nested = Arrays.asList(n1, n1);
    t.classy = "sophisticated";
    t.breaks = Arrays.asList("10am", "2<pm>");
    t.stringMap = new TreeMap<>(Map.of("a", "av", "b", "bv"));
    t.tokens = new String[] {"the", "thee", "them"};
    assertEquals(
        "{"
            + "\"a_string\":\"\\\"<&a>\\\"\","
            + "\"an_integer\":null,"
            + "\"a_long\":-12,"
            + "\"a_boolean\":true,"
            + "\"boolean2\":false,"
            + "\"boxed_boolean\":true,"
            + "\"nest\":null,"
            + "\"a_big_letter\":\"CEE\","
            + "\"name\":[\"ted\",\"jane\"],"
            + "\"nested\":[{\"egg\":\"scrambled\",\"bird\":\"duck\",\"value\":\"100\"}"
            + ",{\"egg\":\"scrambled\",\"bird\":\"duck\",\"value\":\"100\"}],"
            + "\"classy\":\"sophisticated\","
            + "\"breaks\":[\"10am\",\"2<pm>\"],"
            + "\"tokens\":[\"the\",\"thee\",\"them\"],"
            + "\"string_map\":{\"a\":\"av\",\"b\":\"bv\"}"
            + "}",
        JsonUtil.asJson(t));
    assertEquals(
        "{\n"
            + "  \"a_string\":\"\\\"<&a>\\\"\",\n"
            + "  \"an_integer\":null,\n"
            + "  \"a_long\":-12,\n"
            + "  \"a_boolean\":true,\n"
            + "  \"boolean2\":false,\n"
            + "  \"boxed_boolean\":true,\n"
            + "  \"nest\":null,\n"
            + "  \"a_big_letter\":\"CEE\",\n"
            + "  \"name\":[\n"
            + "    \"ted\",\n"
            + "    \"jane\"\n"
            + "  ],\n"
            + "  \"nested\":[\n"
            + "    {\n"
            + "      \"egg\":\"scrambled\",\n"
            + "      \"bird\":\"duck\",\n"
            + "      \"value\":\"100\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"egg\":\"scrambled\",\n"
            + "      \"bird\":\"duck\",\n"
            + "      \"value\":\"100\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"classy\":\"sophisticated\",\n"
            + "  \"breaks\":[\n"
            + "    \"10am\",\n"
            + "    \"2<pm>\"\n"
            + "  ],\n"
            + "  \"tokens\":[\n"
            + "    \"the\",\n"
            + "    \"thee\",\n"
            + "    \"them\"\n"
            + "  ],\n"
            + "  \"string_map\":{\n"
            + "    \"a\":\"av\",\n"
            + "    \"b\":\"bv\"\n"
            + "  }\n"
            + "}",
        JsonUtil.asJson(t, 0));
  }

  public void testSerializeStringArray() {
    /*
     * Local class for test
     */
    @SuppressWarnings("unused")
    final class Foo {
      public String[] arr0, arr1, arr2;
    }

    Foo foo = new Foo();
    foo.arr0 = new String[0];
    foo.arr1 = new String[] {"foo"};
    foo.arr2 = new String[] {"foo", "bar"};

    assertEquals(
        "{\"arr0\":[],\"arr1\":[\"foo\"],\"arr2\":[\"foo\",\"bar\"]}", JsonUtil.asJson(foo, null));
    assertEquals(
        "{\n  \"arr0\":[],\n  \"arr1\":[\n    \"foo\"\n  ],\n"
            + "  \"arr2\":[\n    \"foo\",\n    \"bar\"\n  ]\n}",
        JsonUtil.asJson(foo, 0));
  }

  public void testSerializeInheritance() throws Exception {
    assertEquals("{\"b\":\"B.b\",\"bb\":\"B.bb\"}", JsonUtil.asJson(new B(), null));
    assertEquals(
        "{\"a\":\"AB.a\",\"b\":\"AB.b\",\"bb\":\"B.bb\"}", JsonUtil.asJson(new AB(), null));
  }

  public void testJsonEscape() {
    assertEquals("a < b >= \\\"c' & \\\\", JsonUtil.jsonEscape("a < b >= \"c' & \\"));
    assertEquals("ok\\u0000\ufffe\uffff", JsonUtil.jsonEscape("ok\u0000\ufffe\uffff"));
    assertEquals(
        "ok\\u0001 ok\\u0009ok\\u000a\\u001f", JsonUtil.jsonEscape("ok\u0001 ok\tok\n\u001f"));
  }

  /** test class */
  @SuppressWarnings("unused")
  private class B {
    protected String b;
    protected String bb;

    B() {
      b = "B.b";
      bb = "B.bb";
    }
  }

  /** test class */
  @SuppressWarnings("unused")
  private class AB extends B {
    protected String a;
    public String b;

    AB() {
      a = "AB.a";
      b = "AB.b";
    }
  }

  public void testParseEmpty() {
    expectThrows(ParseException.class, () -> JsonUtil.parse(""));
    expectThrows(ParseException.class, () -> JsonUtil.parse(" "));
  }

  public void testParseToken() throws Exception {
    assertEquals(Boolean.FALSE, JsonUtil.parse("false"));
    assertEquals(Boolean.TRUE, JsonUtil.parse("true"));
    assertNull(JsonUtil.parse("null"));
    expectThrows(ParseException.class, () -> JsonUtil.parse("fals"));
  }

  public void testExtraWhitespace() throws Exception {
    assertEquals(Boolean.TRUE, JsonUtil.parse(" true"));
    assertEquals(Boolean.TRUE, JsonUtil.parse(" true "));
    assertEquals(Boolean.TRUE, JsonUtil.parse(" \t\n\rtrue "));
  }

  public void testParseString() throws Exception {
    assertEquals("", JsonUtil.parse("\"\""));
    assertEquals("cat", JsonUtil.parse("\"cat\""));
    assertEquals("\"cat\"", JsonUtil.parse("\"\\\"cat\\\"\""));
    assertEquals("\\cat\n", JsonUtil.parse("\"\\\\cat\\n\""));
    assertEquals("\b\f\r\n\t", JsonUtil.parse("\"\\b\\f\\r\\n\\t\""));
    // standard JSON defines unicode escapes, but we don't support them
    expectThrows(ParseException.class, () -> JsonUtil.parse("\"\\u0097\""));
    expectThrows(ParseException.class, () -> JsonUtil.parse("\"cat"));
    expectThrows(ParseException.class, () -> JsonUtil.parse("\"\\"));
  }

  public void testParseBigString() throws Exception {
    // exceeds the initial 1024-byte internal buffer
    byte[] buf = new byte[1028];
    Arrays.fill(buf, (byte) 'x');
    buf[0] = '"';
    buf[1027] = '"';
    assertEquals(new String(buf, 1, 1026, UTF_8), JsonUtil.parse(new String(buf, UTF_8)));
  }

  public void testParseNumber() throws Exception {
    assertEquals(0d, JsonUtil.parse("0"));
    assertEquals(9d, JsonUtil.parse("9 "));
    assertEquals(-1d, JsonUtil.parse("-1"));
    assertEquals(1d, JsonUtil.parse("+1"));
    assertEquals(0.9d, JsonUtil.parse(".9"));
    assertEquals(0.9d, JsonUtil.parse("0.9"));
    assertEquals(999.123e-10d, JsonUtil.parse("999.123e-10"));
    assertEquals(999.123e20d, JsonUtil.parse("999.123e20"));
    assertEquals(999.123e+2d, JsonUtil.parse("999.123e+2"));
    // we read the first valid Json value and in theory would get a parse error on the *next* read:
    assertEquals(9d, JsonUtil.parse("9x"));
    // but these fail because we gobble up the possible-number-characters. Hmm seems a little wonky.
    expectThrows(ParseException.class, () -> JsonUtil.parse("9-2"));
    expectThrows(ParseException.class, () -> JsonUtil.parse("9.2.3"));
  }

  public void testParseArray() throws Exception {
    assertEquals(List.of(), JsonUtil.parse("[]"));
    assertEquals(List.of(1d), JsonUtil.parse("[1]"));
    assertEquals(List.of(1d, 2d), JsonUtil.parse("[1,2]"));
    assertEquals(List.of(1d, 2d), JsonUtil.parse("[1,2,]"));
    assertEquals(List.of("1", 2d), JsonUtil.parse("[\"1\",2]"));
    assertEquals(List.of(List.of(1d, List.of(2d)), 3d), JsonUtil.parse("[[1,[2]],3]"));
    assertEquals(List.of(List.of(1d, List.of(2d)), 3d), JsonUtil.parse("[ [ 1, [ 2 ] ] , 3 ] "));
    expectThrows(ParseException.class, () -> JsonUtil.parse("[ 100,,]"));
    expectThrows(ParseException.class, () -> JsonUtil.parse("[ 100 100 ]"));
    expectThrows(ParseException.class, () -> JsonUtil.parse("[ 100"));
    expectThrows(ParseException.class, () -> JsonUtil.parse("]"));
  }

  public void testParseMap() throws Exception {
    assertEquals(Map.of(), JsonUtil.parse("{}"));
    assertEquals(Map.of("a", 1d), JsonUtil.parse("{\"a\":1}"));
    assertEquals(Map.of("a", 1d, "b", 2d), JsonUtil.parse("{\"a\":1,\"b\":2}"));
    assertEquals(Map.of("a", "1", "b", 2d), JsonUtil.parse("{\"a\":\"1\",\"b\":2}"));
    assertEquals(
        Map.of("a", Map.of("1", List.of(2d)), "b", 3d),
        JsonUtil.parse("{\"a\":{\"1\":[2]},\"b\":3}"));
    assertEquals(
        Map.of("a", Map.of("1", List.of(2d)), "b", 3d),
        JsonUtil.parse("{ \"a\" : {  \"1\" : [2] }, \"b\": 3} "));
    expectThrows(ParseException.class, () -> JsonUtil.parse("{ 100: 100}"));
    assertEquals(Map.of("a", 100d), JsonUtil.parse("{ \"a\": 100,}"));
    expectThrows(ParseException.class, () -> JsonUtil.parse("{ \"a\": 100,,}"));
    expectThrows(ParseException.class, () -> JsonUtil.parse("{ \"a\": 100 \"b\"}"));
    expectThrows(ParseException.class, () -> JsonUtil.parse("{ \"a\": }"));
    expectThrows(ParseException.class, () -> JsonUtil.parse("{ \"a\"}"));
    expectThrows(ParseException.class, () -> JsonUtil.parse("{ \"a\" : 1"));
    expectThrows(ParseException.class, () -> JsonUtil.parse("}"));
  }
}
