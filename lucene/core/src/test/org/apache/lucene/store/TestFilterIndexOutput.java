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
package org.apache.lucene.store;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;

public class TestFilterIndexOutput extends BaseDataOutputTestCase<FilterIndexOutput> {

  @Override
  protected FilterIndexOutput newInstance() {
    return new FilterIndexOutput(
        "test",
        "test",
        new ByteBuffersIndexOutput(ByteBuffersDataOutput.newResettableInstance(), "test", "test"));
  }

  @Override
  protected byte[] toBytes(FilterIndexOutput instance) {
    return ((ByteBuffersIndexOutput) instance.out).toArrayCopy();
  }

  @Test
  public void testOverrides() throws Exception {
    // verify that all methods of IndexOutput/DataOutput are overridden by FilterDirectory,
    // except those under the 'exclude' list
    Set<Method> exclude = new HashSet<>();

    exclude.add(DataOutput.class.getMethod("copyBytes", DataInput.class, long.class));

    exclude.add(IndexOutput.class.getMethod("toString"));
    exclude.add(IndexOutput.class.getMethod("getName"));

    // final methods
    exclude.add(IndexOutput.class.getMethod("alignOffset", long.class, int.class));
    exclude.add(IndexOutput.class.getMethod("alignFilePointer", int.class));
    exclude.add(DataOutput.class.getMethod("writeZLong", long.class));
    exclude.add(DataOutput.class.getMethod("writeVLong", long.class));
    exclude.add(DataOutput.class.getMethod("writeZInt", int.class));
    exclude.add(DataOutput.class.getMethod("writeVInt", int.class));

    for (Method m : FilterIndexOutput.class.getMethods()) {
      if (m.getDeclaringClass() == IndexOutput.class || m.getDeclaringClass() == DataOutput.class) {
        String className = IndexOutput.class.getSimpleName();
        if (m.getDeclaringClass() == DataOutput.class) {
          className = DataOutput.class.getSimpleName();
        }
        assertTrue(
            "method " + m.getName() + " not overridden from " + className + "!",
            exclude.contains(m));
      }
    }
  }

  public void testUnwrap() throws IOException {
    Directory dir = FSDirectory.open(createTempDir());
    IndexOutput output = dir.createOutput("test", IOContext.DEFAULT);
    FilterIndexOutput filterIndexOutput =
        new FilterIndexOutput("wrapper of test", "FilterDirectory{test}", output);
    assertEquals(output, filterIndexOutput.getDelegate());
    assertEquals(output, FilterIndexOutput.unwrap(filterIndexOutput));
    filterIndexOutput.close();
    dir.close();
  }
}
