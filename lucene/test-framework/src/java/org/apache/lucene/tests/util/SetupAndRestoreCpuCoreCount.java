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

import org.apache.lucene.index.ConcurrentMergeScheduler;

/**
 * Randomizes the CPU core count seen by {@link ConcurrentMergeScheduler} so that it varies its
 * dynamic defaults. This also "fixes" the core count from the master seed so it will always be the
 * same on reproduce.
 */
final class SetupAndRestoreCpuCoreCount implements BeforeAfterCallback {
  @Override
  public void before() {
    int numCores = TestUtil.nextInt(LuceneTestCaseParent.random(), 1, 4);
    System.setProperty(
        ConcurrentMergeScheduler.DEFAULT_CPU_CORE_COUNT_PROPERTY, Integer.toString(numCores));
  }

  @Override
  public void after() {
    System.clearProperty(ConcurrentMergeScheduler.DEFAULT_CPU_CORE_COUNT_PROPERTY);
  }
}
