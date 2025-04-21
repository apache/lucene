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

import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.*;

@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1, warmups = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class SloppyMathBenchmark {
    private static double sloppySin(double a) {
        // Common sloppySin implementation
        return Math.abs(a) <= Math.PI/4
                ? a * (0.9992947 + a * a * (-0.16161097 + a * a * 0.0066208798))
                : Math.sin(a);
    }

    @Benchmark
    public double standardSin(ExecutionPlan plan) {
        return Math.sin(plan.value);
    }

    @Benchmark
    public double approximateSin(ExecutionPlan plan) {
        return sloppySin(plan.value);
    }

    @State(Scope.Benchmark)
    public static class ExecutionPlan {

        // Test with different input ranges
        @Param({"0.1", "0.5", "1.0", "2.0", "3.14"})
        public double value;
    }

}