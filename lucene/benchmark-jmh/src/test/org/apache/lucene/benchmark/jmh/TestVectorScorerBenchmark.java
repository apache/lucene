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
package org.apache.lucene.benchmark.jmh;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.VectorUtil;
import org.openjdk.jmh.annotations.Param;

public class TestVectorScorerBenchmark extends LuceneTestCase {

  static final float DELTA = 1e-3f;

  final int size;

  public TestVectorScorerBenchmark(int size) {
    this.size = size;
  }

  public void testDotProduct() throws IOException {
    var bench = new VectorScorerBenchmark();
    bench.size = size;
    bench.init();

    try {
      float expected = VectorUtil.dotProductScore(bench.vec1, bench.vec2);
      assertEquals(expected, bench.binaryDotProductDefault(), DELTA);
      assertEquals(expected, bench.binaryDotProductMemSeg(), DELTA);
    } finally {
      bench.teardown();
    }
  }

  @ParametersFactory
  public static Iterable<Object[]> parametersFactory() {
    try {
      var cls = VectorScorerBenchmark.class;
      String[] params = cls.getField("size").getAnnotationsByType(Param.class)[0].value();
      return () ->
          Arrays.stream(params).map(Integer::parseInt).map(i -> new Object[] {i}).iterator();
    } catch (NoSuchFieldException e) {
      throw new AssertionError(e);
    }
  }
}
