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

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.text.ParseException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.expressions.Expression;
import org.apache.lucene.expressions.js.JavascriptCompiler;
import org.apache.lucene.search.DoubleValues;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 5)
@Measurement(iterations = 12, time = 8)
@Fork(value = 1)
public class ExpressionsBenchmark {

  /**
   * Some extra functions to bench "identity" in various variants, another one is named
   * "native_identity" (see below).
   */
  private static final Map<String, MethodHandle> FUNCTIONS = getFunctions();

  private static final String NATIVE_IDENTITY_NAME = "native_identity";

  private static Map<String, MethodHandle> getFunctions() {
    try {
      var lookup = MethodHandles.lookup();
      Map<String, MethodHandle> m = new HashMap<>(JavascriptCompiler.DEFAULT_FUNCTIONS);
      m.put(
          "func_identity",
          lookup.findStatic(
              lookup.lookupClass(), "ident", MethodType.methodType(double.class, double.class)));
      m.put("mh_identity", MethodHandles.identity(double.class));
      return Collections.unmodifiableMap(m);
    } catch (ReflectiveOperationException e) {
      throw new LinkageError("Couldn't find function", e);
    }
  }

  @SuppressWarnings("unused")
  private static double ident(double v) {
    return v;
  }

  /** A native implementation of an expression to compare performance */
  private static final Expression NATIVE_IDENTITY_EXPRESSION =
      new Expression(NATIVE_IDENTITY_NAME, new String[] {"x"}) {
        @Override
        public double evaluate(DoubleValues[] functionValues) throws IOException {
          return functionValues[0].doubleValue();
        }
      };

  private double[] randomData;
  private Expression expression;

  @Param({"x", "func_identity(x)", "mh_identity", "native_identity", "cos(x)", "cos(x) + sin(x)"})
  String js;

  @Setup(Level.Iteration)
  public void init() throws ParseException {
    ThreadLocalRandom random = ThreadLocalRandom.current();
    randomData = random.doubles().limit(1024).toArray();
    expression =
        Objects.equals(js, NATIVE_IDENTITY_NAME)
            ? NATIVE_IDENTITY_EXPRESSION
            : JavascriptCompiler.compile(js, FUNCTIONS);
  }

  @Benchmark
  public double expression() throws IOException {
    var it = new ValuesIterator(randomData);
    var values = it.getDoubleValues();
    double result = 0d;
    while (it.next()) {
      result += expression.evaluate(values);
    }
    return result;
  }

  static final class ValuesIterator {
    final double[] data;
    final DoubleValues[] dv;
    int pos = -1;

    ValuesIterator(double[] data) {
      this.data = data;
      var dv =
          new DoubleValues() {
            @Override
            public double doubleValue() throws IOException {
              return data[pos];
            }

            @Override
            public boolean advanceExact(int doc) throws IOException {
              throw new UnsupportedOperationException();
            }
          };
      this.dv = new DoubleValues[] {dv};
    }

    boolean next() {
      pos++;
      return (pos < data.length);
    }

    DoubleValues[] getDoubleValues() {
      return dv;
    }
  }
}
