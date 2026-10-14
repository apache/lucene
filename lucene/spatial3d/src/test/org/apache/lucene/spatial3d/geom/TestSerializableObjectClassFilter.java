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

package org.apache.lucene.spatial3d.geom;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestSerializableObjectClassFilter extends LuceneTestCase {

  static volatile boolean nonSerializableConstructed = false,
      nonSerializableClassInitialized = false;

  /** A SerializableObject that is not registered in {@link StandardObjects}. */
  public static class CustomShape implements SerializableObject {
    final int value;

    public CustomShape(int value) {
      this.value = value;
    }

    public CustomShape(InputStream inputStream) throws IOException {
      this.value = SerializableObject.readInt(inputStream);
    }

    @Override
    public void write(OutputStream outputStream) throws IOException {
      SerializableObject.writeInt(outputStream, value);
    }
  }

  /** Not a SerializableObject, but instantiable from an InputStream. */
  public static class NotASerializableObject {
    static {
      nonSerializableClassInitialized = true;
    }

    public NotASerializableObject(InputStream inputStream) {
      nonSerializableConstructed = true;
    }
  }

  public void testCustomClassRoundTrips() throws IOException {
    CustomShape shape = new CustomShape(42);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    SerializableObject.writeObject(out, shape);
    ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
    SerializableObject copy = SerializableObject.readObject(in);
    assertTrue(copy instanceof CustomShape);
    assertEquals(42, ((CustomShape) copy).value);
  }

  public void testRejectsNonSerializableObjectClass() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    SerializableObject.writeBoolean(out, false);
    SerializableObject.writeString(out, NotASerializableObject.class.getName());
    ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
    var e = expectThrows(IOException.class, () -> SerializableObject.readObject(in));
    assertTrue(e.getCause() instanceof ClassCastException);
    assertTrue(e.getMessage().contains(NotASerializableObject.class.getName()));
    assertFalse(nonSerializableClassInitialized);
    assertFalse(nonSerializableConstructed);
  }
}
