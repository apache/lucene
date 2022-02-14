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
package org.apache.lucene.core.tests;

import org.apache.lucene.core.tests.main.EmptyReference;
import org.apache.lucene.index.IndexWriter;
import org.junit.Test;

/** Intentionally not a subclass of {@code LuceneTestCase}. */
public class TestRuntimeDependenciesSane {
  @Test
  public void testExternalDependenciesAreModules() {
    isModule(org.junit.Test.class);
  }

  @Test
  public void testInterProjectDependenciesAreModules() {
    // The core Lucene should be a modular dependency.
    isModule(IndexWriter.class);
  }

  @Test
  public void testTestSourceSetIsAModule() {
    // The test source set itself should be loaded as a module.
    isModule(getClass());
  }

  @Test
  public void testMainSourceSetIsAModule() {
    // The test source set itself should be loaded as a module.
    isModule(EmptyReference.class);
  }

  private static void isModule(Class<?> clazz) {
    var module = clazz.getModule();
    if (!module.isNamed()) {
      throw new AssertionError(
          "Class should be loaded from a named module: "
              + clazz.getName()
              + " but is instead part of: "
              + module);
    }
  }
}
