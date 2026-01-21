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

import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.TestMinimalCodec;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

// TODO: maybe we should test this with mocks, but it's easy
// enough to test the basics via Codec
public class TestNamedSPILoader extends LuceneTestCase {

  public void testLookup() {
    String currentName = TestUtil.getDefaultCodec().getName();
    Codec codec = Codec.forName(currentName);
    assertEquals(currentName, codec.getName());
  }

  // we want an exception if it's not found.
  public void testBogusLookup() {
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          Codec.forName("dskfdskfsdfksdfdsf");
        });
  }

  public void testAvailableServices() {
    Set<String> codecs = Codec.availableCodecs();
    assertTrue(codecs.contains(TestUtil.getDefaultCodec().getName()));
  }

  public void testReloadNoChange() {
    var loader = new NamedSPILoader<Codec>(Codec.class);
    var initialProviders = stream(loader.iterator()).toList();
    var initialNames = loader.availableServices().stream().toList();

    loader.reload(Codec.class.getClassLoader());
    var reloadedProviders = stream(loader.iterator()).toList();
    var reloadedNames = loader.availableServices().stream().toList();

    // nothing has changed so the services should be identical
    assertEquals(initialProviders, reloadedProviders);
    assertEquals(initialNames, reloadedNames);
  }

  // Checks that the test-specific ThrowingCodec is not installed - replaced during iteration
  public void testReplaceCodec() {
    var loader = new NamedSPILoader<Codec>(Codec.class);
    for (int i = 0; i < 5; i++) {
      var names = loader.availableServices().stream().toList();
      var providers = stream(loader.iterator()).map(c -> c.getClass().getName()).toList();
      assertFalse(names.contains("ThrowingCodec"));
      assertFalse(providers.contains(TestMinimalCodec.ThrowingCodec.class.getName()));
      loader.reload(Codec.class.getClassLoader());
    }
  }

  public void testReplaceable() {
    var loader = new NamedSPILoader<ReplaceableService>(ReplaceableService.class);
    for (int i = 0; i < 5; i++) {
      var names = loader.availableServices().stream().toList();
      var providers = stream(loader.iterator()).map(c -> c.getClass().getName()).toList();
      assertTrue(names.contains("ReplaceableService"));
      assertTrue(providers.contains(ReplaceableServiceImpl2.class.getName()));
      loader.reload(ReplaceableService.class.getClassLoader());
    }
  }

  public void testNonReplaceable() {
    var loader = new NamedSPILoader<NonReplaceableService>(NonReplaceableService.class);
    for (int i = 0; i < 5; i++) {
      var names = loader.availableServices().stream().toList();
      var providers = stream(loader.iterator()).map(c -> c.getClass().getName()).toList();
      assertTrue(names.contains("NonReplaceableService"));
      assertTrue(providers.contains(NonReplaceableServiceImpl1.class.getName()));
      loader.reload(NonReplaceableService.class.getClassLoader());
    }
  }

  public abstract static class ReplaceableService implements NamedSPILoader.NamedSPI {
    @Override
    public String getName() {
      return "ReplaceableService";
    }
  }

  public static class ReplaceableServiceImpl1 extends ReplaceableService {
    @Override
    public boolean replace(NamedSPILoader.NamedSPI previous) {
      assert false : "should never reach here";
      return true;
    }
  }

  public static class ReplaceableServiceImpl2 extends ReplaceableService {
    @Override
    public boolean replace(NamedSPILoader.NamedSPI prev) {
      assertEquals("ReplaceableService", prev.getName());
      var expected = "org.apache.lucene.util.TestNamedSPILoader$ReplaceableServiceImpl1";
      assertEquals(expected, prev.getClass().getName());
      return true;
    }
  }

  public abstract static class NonReplaceableService implements NamedSPILoader.NamedSPI {
    @Override
    public String getName() {
      return "NonReplaceableService";
    }
  }

  public static class NonReplaceableServiceImpl1 extends NonReplaceableService {
    @Override
    public boolean replace(NamedSPILoader.NamedSPI previous) {
      assert false : "should never reach here";
      return true;
    }
  }

  public static class NonReplaceableServiceImpl2 extends NonReplaceableService {
    // default is replace false
  }

  static <T> Stream<T> stream(Iterator<T> iterator) {
    return StreamSupport.stream(((Iterable<T>) () -> iterator).spliterator(), false);
  }
}
