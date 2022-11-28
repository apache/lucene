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
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import org.apache.lucene.store.DataInput;
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
  public void testOverrides() throws Exception {
    // verify that all abstract methods of IndexInput/DataInput are overridden by FilterDirectory,
    // except those under the 'exclude' list
    Set<Method> exclude = new HashSet<>();

    exclude.add(IndexInput.class.getMethod("toString"));
    exclude.add(IndexInput.class.getMethod("skipBytes", long.class));
    exclude.add(IndexInput.class.getDeclaredMethod("getFullSliceDescription", String.class));
    exclude.add(IndexInput.class.getMethod("randomAccessSlice", long.class, long.class));

    exclude.add(
        DataInput.class.getMethod("readBytes", byte[].class, int.class, int.class, boolean.class));
    exclude.add(DataInput.class.getMethod("readShort"));
    exclude.add(DataInput.class.getMethod("readInt"));
    exclude.add(DataInput.class.getMethod("readVInt"));
    exclude.add(DataInput.class.getMethod("readZInt"));
    exclude.add(DataInput.class.getMethod("readLong"));
    exclude.add(DataInput.class.getMethod("readLongs", long[].class, int.class, int.class));
    exclude.add(DataInput.class.getMethod("readInts", int[].class, int.class, int.class));
    exclude.add(DataInput.class.getMethod("readFloats", float[].class, int.class, int.class));
    exclude.add(DataInput.class.getMethod("readVLong"));
    exclude.add(DataInput.class.getMethod("readZLong"));
    exclude.add(DataInput.class.getMethod("readString"));
    exclude.add(DataInput.class.getMethod("readMapOfStrings"));
    exclude.add(DataInput.class.getMethod("readSetOfStrings"));

    for (Method m : FilterIndexInput.class.getMethods()) {
      if (m.getName().contains("clone")) {
        // special case
        continue;
      }
      if (m.getDeclaringClass() == IndexInput.class || m.getDeclaringClass() == DataInput.class) {
        String className = IndexInput.class.getSimpleName();
        if (m.getDeclaringClass() == DataInput.class) {
          className = DataInput.class.getSimpleName();
        }
        assertTrue(
            "method " + m.getName() + " not overridden from " + className + "!",
            exclude.contains(m));
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
