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

import static org.apache.lucene.tests.util.RamUsageTester.ramUsed;
import static org.apache.lucene.util.RamUsageEstimator.COMPRESSED_REFS_ENABLED;
import static org.apache.lucene.util.RamUsageEstimator.JVM_IS_HOTSPOT_64BIT;
import static org.apache.lucene.util.RamUsageEstimator.LONG_SIZE;
import static org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
import static org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_OBJECT_ALIGNMENT;
import static org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_OBJECT_HEADER;
import static org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_OBJECT_REF;
import static org.apache.lucene.util.RamUsageEstimator.shallowSizeOf;
import static org.apache.lucene.util.RamUsageEstimator.shallowSizeOfInstance;
import static org.apache.lucene.util.RamUsageEstimator.sizeOf;

import java.lang.management.CompilationMXBean;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestRamUsageEstimator extends LuceneTestCase {

  static final String[] strings = new String[] {"test string", "hollow", "catchmaster"};

  public void testSanity() {
    assertTrue(ramUsed("test string") > shallowSizeOfInstance(String.class));

    Holder holder = new Holder();
    holder.holder = new Holder("string2", 5000L);
    assertTrue(ramUsed(holder) > shallowSizeOfInstance(Holder.class));
    assertTrue(ramUsed(holder) > ramUsed(holder.holder));

    assertTrue(shallowSizeOfInstance(HolderSubclass.class) >= shallowSizeOfInstance(Holder.class));
    assertTrue(shallowSizeOfInstance(Holder.class) == shallowSizeOfInstance(HolderSubclass2.class));

    assertTrue(ramUsed(strings) > shallowSizeOf(strings));
  }

  public void testStaticOverloads() {
    Random rnd = random();
    {
      byte[] array = new byte[rnd.nextInt(1024)];
      assertEquals(sizeOf(array), ramUsed(array));
      assertEquals(shallowSizeOf(array), ramUsed(array));
    }

    {
      boolean[] array = new boolean[rnd.nextInt(1024)];
      assertEquals(ramUsed(array), sizeOf(array));
      assertEquals(ramUsed(array), shallowSizeOf(array));
    }

    {
      char[] array = new char[rnd.nextInt(1024)];
      assertEquals(ramUsed(array), sizeOf(array));
      assertEquals(ramUsed(array), shallowSizeOf(array));
    }

    {
      short[] array = new short[rnd.nextInt(1024)];
      assertEquals(ramUsed(array), sizeOf(array));
      assertEquals(ramUsed(array), shallowSizeOf(array));
    }

    {
      int[] array = new int[rnd.nextInt(1024)];
      assertEquals(ramUsed(array), sizeOf(array));
      assertEquals(ramUsed(array), shallowSizeOf(array));
    }

    {
      float[] array = new float[rnd.nextInt(1024)];
      assertEquals(ramUsed(array), sizeOf(array));
      assertEquals(ramUsed(array), shallowSizeOf(array));
    }

    {
      long[] array = new long[rnd.nextInt(1024)];
      assertEquals(ramUsed(array), sizeOf(array));
      assertEquals(ramUsed(array), shallowSizeOf(array));
    }

    {
      double[] array = new double[rnd.nextInt(1024)];
      assertEquals(ramUsed(array), sizeOf(array));
      assertEquals(ramUsed(array), shallowSizeOf(array));
    }
  }

  public void testStrings() {
    long actual = ramUsed(strings);
    long estimated = sizeOf(strings);
    assertEquals(actual, estimated);
  }

  public void testBytesRefHash() {
    BytesRefHash bytes = new BytesRefHash();
    for (int i = 0; i < 100; i++) {
      bytes.add(new BytesRef("foo bar " + i));
      bytes.add(new BytesRef("baz bam " + i));
    }
    long actual = ramUsed(bytes);
    long estimated = sizeOf(bytes);
    assertEquals((double) actual, (double) estimated, (double) actual * 0.1);
  }

  public void testMap() {
    Map<String, Object> map = new HashMap<>();
    map.put("primitive", 1234L);
    map.put("string", "string");
    for (int i = 0; i < 100; i++) {
      map.put("complex " + i, new Term("foo " + i, "bar " + i));
    }
    double errorFactor = COMPRESSED_REFS_ENABLED ? 0.2 : 0.3;
    long actual = ramUsed(map);
    long estimated = RamUsageEstimator.sizeOfObject(map);
    assertEquals((double) actual, (double) estimated, (double) actual * errorFactor);

    // test recursion
    map.put("self", map);
    actual = ramUsed(map);
    estimated = RamUsageEstimator.sizeOfObject(map);
    assertEquals((double) actual, (double) estimated, (double) actual * errorFactor);
  }

  public void testCollection() {
    List<Object> list = new ArrayList<>();
    list.add(1234L);
    list.add("string");
    for (int i = 0; i < 100; i++) {
      list.add(new Term("foo " + i, "term " + i));
    }
    long actual = ramUsed(list);
    long estimated = RamUsageEstimator.sizeOfObject(list);
    assertEquals((double) actual, (double) estimated, (double) actual * 0.1);

    // test recursion
    list.add(list);
    actual = ramUsed(list);
    estimated = RamUsageEstimator.sizeOfObject(list);
    assertEquals((double) actual, (double) estimated, (double) actual * 0.1);
  }

  public void testQuery() {
    DisjunctionMaxQuery dismax =
        new DisjunctionMaxQuery(
            Arrays.asList(
                new TermQuery(new Term("foo1", "bar1")), new TermQuery(new Term("baz1", "bam1"))),
            1.0f);
    BooleanQuery bq =
        new BooleanQuery.Builder()
            .add(new TermQuery(new Term("foo2", "bar2")), BooleanClause.Occur.SHOULD)
            .add(
                new PhraseQuery.Builder().add(new Term("foo3", "baz3")).build(),
                BooleanClause.Occur.MUST_NOT)
            .add(dismax, BooleanClause.Occur.MUST)
            .build();
    long actual = ramUsed(bq);
    long estimated = RamUsageEstimator.sizeOfObject(bq);
    // sizeOfObject uses much lower default size estimate than we normally use
    // but the query-specific default is so large that the comparison becomes meaningless.
    assertEquals((double) actual, (double) estimated, (double) actual * 0.5);
  }

  public void testReferenceSize() {
    assertTrue(NUM_BYTES_OBJECT_REF == 4 || NUM_BYTES_OBJECT_REF == 8);
    if (Constants.JRE_IS_64BIT) {
      assertEquals(
          "For 64 bit JVMs, reference size must be 8, unless compressed references are enabled",
          COMPRESSED_REFS_ENABLED ? 4 : 8,
          NUM_BYTES_OBJECT_REF);
    } else {
      assertEquals("For 32bit JVMs, reference size must always be 4", 4, NUM_BYTES_OBJECT_REF);
      assertFalse(
          "For 32bit JVMs, compressed references can never be enabled", COMPRESSED_REFS_ENABLED);
    }
  }

  public void testHotspotBean() {
    assumeTrue("testHotspotBean only works on 64bit JVMs.", Constants.JRE_IS_64BIT);
    assumeTrue(
        "testHotspotBean only works on Hotspot (OpenJDK, Oracle) virtual machines.",
        Optional.ofNullable(ManagementFactory.getCompilationMXBean())
            .map(CompilationMXBean::getName)
            .orElse("Unknown")
            .startsWith("HotSpot"));
    assertTrue(
        "We should have been able to detect Hotspot's internal settings from the management bean.",
        JVM_IS_HOTSPOT_64BIT);
  }

  /** Helper to print out current settings for debugging {@code -Dtests.verbose=true} */
  public void testPrintValues() {
    assumeTrue("Specify -Dtests.verbose=true to print constants of RamUsageEstimator.", VERBOSE);
    System.out.println("JVM_IS_HOTSPOT_64BIT = " + JVM_IS_HOTSPOT_64BIT);
    System.out.println("COMPRESSED_REFS_ENABLED = " + COMPRESSED_REFS_ENABLED);
    System.out.println("NUM_BYTES_OBJECT_ALIGNMENT = " + NUM_BYTES_OBJECT_ALIGNMENT);
    System.out.println("NUM_BYTES_OBJECT_REF = " + NUM_BYTES_OBJECT_REF);
    System.out.println("NUM_BYTES_OBJECT_HEADER = " + NUM_BYTES_OBJECT_HEADER);
    System.out.println("NUM_BYTES_ARRAY_HEADER = " + NUM_BYTES_ARRAY_HEADER);
    System.out.println("LONG_SIZE = " + LONG_SIZE);
  }

  @SuppressWarnings("unused")
  private static class Holder {
    long field1 = 5000L;
    String name = "name";
    Holder holder;
    long field2, field3, field4;

    Holder() {}

    Holder(String name, long field1) {
      this.name = name;
      this.field1 = field1;
    }
  }

  @SuppressWarnings("unused")
  private static class HolderSubclass extends Holder {
    byte foo;
    int bar;
  }

  private static class HolderSubclass2 extends Holder {
    // empty, only inherits all fields -> size should be identical to superclass
  }
}
