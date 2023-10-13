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

import java.io.IOException;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.ModuleResourceLoader;
import org.apache.lucene.util.ResourceLoader;
import org.junit.BeforeClass;

public class TestModuleResourceLoader extends LuceneTestCase {
  private static final Module MODULE = TestModuleResourceLoader.class.getModule();

  private final ResourceLoader loader = new ModuleResourceLoader(MODULE);

  @BeforeClass
  public static void beforeClass() {
    assertTrue("Test class must be in a named module", MODULE.isNamed());
  }

  public void testModuleResources() throws Exception {
    try (var stream = loader.openResource("org/apache/lucene/core/testresources/accessible.txt")) {
      stream.available();
    }

    assertNotNull(
        "resource should exist when loaded by test from classloader",
        getClass().getResource("/org/apache/lucene/core/testresources/accessible.txt"));

    try (var stream = loader.openResource("org/apache/lucene/core/tests/nonaccessible.txt")) {
      stream.available();
      fail("Should throw exception");
    } catch (IOException e) {
      assertTrue(e.getMessage().startsWith("Resource not found:"));
    }
  }

  public void testModuleClassloading() {
    assertSame(
        TestModuleResourceLoader.class,
        loader.findClass(TestModuleResourceLoader.class.getName(), Object.class));

    var cname = "org.foobar.Something";
    try {
      loader.findClass(cname, Object.class);
      fail("Should throw exception");
    } catch (RuntimeException e) {
      assertEquals("Cannot load class: " + cname, e.getMessage());
    }
  }
}
