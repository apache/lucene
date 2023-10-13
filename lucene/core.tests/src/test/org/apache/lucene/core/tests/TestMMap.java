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

import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.Assert;

public class TestMMap extends LuceneTestCase {
  public void testUnmapSupported() {
    final Module module = MMapDirectory.class.getModule();
    Assert.assertTrue("Lucene Core is not loaded as module", module.isNamed());
    Assert.assertTrue(
        "Lucene Core can't read 'jdk.unsupported' module",
        module.getLayer().findModule("jdk.unsupported").map(module::canRead).orElse(false));

    // check that MMapDirectory can unmap by running the autodetection logic:
    Assert.assertTrue(MMapDirectory.UNMAP_NOT_SUPPORTED_REASON, MMapDirectory.UNMAP_SUPPORTED);
  }
}
