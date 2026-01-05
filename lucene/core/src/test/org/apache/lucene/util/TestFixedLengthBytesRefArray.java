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

import java.util.Arrays;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

public class TestFixedLengthBytesRefArray extends LuceneTestCase {

  public void testBasic() throws Exception {
    FixedLengthBytesRefArray a = new FixedLengthBytesRefArray(Integer.BYTES);
    int numValues = 100;
    for (int i = 0; i < numValues; i++) {
      byte[] bytes = {0, 0, 0, (byte) (10 - i)};
      a.append(new BytesRef(bytes));
    }

    BytesRefIterator iterator = a.iterator(BytesRef::compareTo);
    BytesRef last = null;

    int count = 0;
    while (true) {
      BytesRef bytes = iterator.next();
      if (bytes == null) {
        break;
      }
      if (last != null) {
        assertTrue(
            "count=" + count + " last=" + last + " bytes=" + bytes, last.compareTo(bytes) < 0);
      }
      last = BytesRef.deepCopyOf(bytes);
      count++;
    }

    assertEquals(numValues, count);
  }

  public void testRandom() throws Exception {
    int length = TestUtil.nextInt(random(), 4, 10);
    int count = atLeast(10000);
    BytesRef[] values = new BytesRef[count];
    BytesRef scratch = new BytesRef();
    scratch.length = length;

    FixedLengthBytesRefArray a = new FixedLengthBytesRefArray(length);
    for (int i = 0; i < count; i++) {
      BytesRef value = new BytesRef(new byte[length]);
      random().nextBytes(value.bytes);
      values[i] = value;
      a.append(value);
    }

    for (int i = 0; i < count; i++) {
      assertEquals(values[i], a.get(scratch, i));
    }

    Arrays.sort(values);
    BytesRefIterator iterator = a.iterator(BytesRef::compareTo);
    for (int i = 0; i < count; i++) {
      BytesRef next = iterator.next();
      assertNotNull(next);
      assertEquals(values[i], next);
    }
  }
}
