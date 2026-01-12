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
package org.apache.lucene.index;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Random;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.junit.Test;

public class TestFilterIndexInput extends TestIndexInput {

  @Override
  public IndexInput getIndexInput(long len) {
    return new FilterIndexInput("wrapped foo", new InterceptingIndexInput("foo", len));
  }

  public void testRawFilterIndexInputRead() throws IOException {
    for (int i = 0; i < 10; i++) {
      Random random = random();
      final Directory dir = newDirectory();
      IndexOutput os = dir.createOutput("foo", newIOContext(random));
      os.writeBytes(READ_TEST_BYTES, READ_TEST_BYTES.length);
      os.close();
      IndexInput is =
          new FilterIndexInput("wrapped foo", dir.openInput("foo", newIOContext(random)));
      checkReads(is, IOException.class);
      checkSeeksAndSkips(is, random);
      is.close();

      os = dir.createOutput("bar", newIOContext(random));
      os.writeBytes(RANDOM_TEST_BYTES, RANDOM_TEST_BYTES.length);
      os.close();
      is = new FilterIndexInput("wrapped bar", dir.openInput("bar", newIOContext(random)));
      checkRandomReads(is);
      checkSeeksAndSkips(is, random);
      is.close();
      dir.close();
    }
  }

  @Test
  public void testOverrides() {
    for (Method m : FilterIndexInput.class.getMethods()) {
      if (m.getName().contains("clone")) {
        // special case
        continue;
      }
      if (m.getDeclaringClass() == FilterIndexInput.class) {
        // verify that only abstract methods are overridden
        Method indexInputMethod;
        try {
          indexInputMethod = IndexInput.class.getMethod(m.getName(), m.getParameterTypes());
          assertTrue(
              "Non-abstract method " + m.getName() + " is overridden",
              Modifier.isAbstract(indexInputMethod.getModifiers()));
        } catch (Exception e) {
          assertTrue(e instanceof NoSuchMethodException);
        }
      }
    }
  }

  public void testUnwrap() throws IOException {
    Directory dir = FSDirectory.open(createTempDir());
    IndexOutput ignored = dir.createOutput("test", IOContext.DEFAULT);
    IndexInput indexInput = dir.openInput("test", IOContext.DEFAULT);
    FilterIndexInput filterIndexInput = new FilterIndexInput("wrapper of test", indexInput);
    assertEquals(indexInput, filterIndexInput.getDelegate());
    assertEquals(indexInput, FilterIndexInput.unwrap(filterIndexInput));
    ignored.close();
    filterIndexInput.close();
    dir.close();
  }
}
