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

import com.carrotsearch.randomizedtesting.Xoroshiro128PlusRandom;
import com.carrotsearch.randomizedtesting.jupiter.Hashing;
import com.carrotsearch.randomizedtesting.jupiter.Seed;
import com.carrotsearch.randomizedtesting.jupiter.SeedChain;
import com.carrotsearch.randomizedtesting.jupiter.SysProps;
import java.util.Collections;
import java.util.Optional;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.MethodOrdererContext;
import org.junit.jupiter.api.parallel.ExecutionMode;

/** A {@link MethodOrderer} that provides random-seed dependent ordering. */
public final class CustomMethodOrderer implements MethodOrderer {
  @Override
  public void orderMethods(MethodOrdererContext context) {
    var seed =
        SeedChain.parse(
                context
                    .getConfigurationParameter(SysProps.TESTS_SEED.propertyKey)
                    .orElse(new Seed(Hashing.hash(context.getTestClass().getName())).toString()))
            .seeds()
            .getFirst()
            .value();

    Collections.shuffle(context.getMethodDescriptors(), new Xoroshiro128PlusRandom(seed));
  }

  @Override
  public Optional<ExecutionMode> getDefaultExecutionMode() {
    return Optional.of(ExecutionMode.SAME_THREAD);
  }
}
