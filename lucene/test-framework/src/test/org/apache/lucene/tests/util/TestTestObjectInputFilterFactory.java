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

import static java.io.ObjectInputFilter.Config.createFilter;
import static java.io.ObjectInputFilter.Config.getSerialFilterFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InvalidClassException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import org.apache.lucene.util.SuppressForbidden;
import org.junit.BeforeClass;

@SuppressForbidden(reason = "tests the forbidden code")
public final class TestTestObjectInputFilterFactory extends LuceneTestCase {

  @BeforeClass
  public static void beforeClass() {
    assumeTrue(
        "Only works when TestObjectInputFilterFactory is installed in JVM",
        getSerialFilterFactory() instanceof TestObjectInputFilterFactory);
  }

  private InputStream openStream() throws IOException {
    var out = new ByteArrayOutputStream();
    try (var oout = new ObjectOutputStream(out)) {
      oout.writeObject(new HashMap<>());
    }
    return new ByteArrayInputStream(out.toByteArray());
  }

  public void testDeserializationWithoutFilter() throws Exception {
    try (var oin = new ObjectInputStream(openStream())) {
      // no filter set!
      var ex = assertThrows(InvalidClassException.class, () -> oin.readObject());
      assertTrue(ex.getMessage().contains("REJECTED"));
    }
  }

  public void testDeserializationWithAllowFilter() throws Exception {
    try (var oin = new ObjectInputStream(openStream())) {
      oin.setObjectInputFilter(createFilter("java.util.HashMap;!*"));
      oin.readObject();
    }
  }

  public void testDeserializationWithDenyFilter() throws Exception {
    try (var oin = new ObjectInputStream(openStream())) {
      oin.setObjectInputFilter(createFilter("!*"));
      var ex = assertThrows(InvalidClassException.class, () -> oin.readObject());
      assertTrue(ex.getMessage().contains("REJECTED"));
    }
  }

  public void testBadCodeJustSettingNullFilter() throws Exception {
    try (var oin = new ObjectInputStream(openStream())) {
      assertThrows(IllegalStateException.class, () -> oin.setObjectInputFilter(null));
    }
  }
}
