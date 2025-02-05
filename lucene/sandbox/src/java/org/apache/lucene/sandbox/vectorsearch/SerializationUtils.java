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
package org.apache.lucene.sandbox.vectorsearch;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.Objects;

/*package-private*/ class SerializationUtils {

  static byte[] serialize(final Serializable obj) {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream(64 * 1024);
    serialize(obj, baos);
    return baos.toByteArray();
  }

  static void serialize(final Serializable obj, final OutputStream outputStream) {
    Objects.requireNonNull(outputStream);
    try (ObjectOutputStream out = new ObjectOutputStream(outputStream)) {
      out.writeObject(obj);
    } catch (final IOException ex) {
      throw new UncheckedIOException(ex);
    }
  }

  static <T> T deserialize(final byte[] objectData) {
    Objects.requireNonNull(objectData);
    return deserialize(new ByteArrayInputStream(objectData));
  }

  static <T> T deserialize(final InputStream inputStream) {
    Objects.requireNonNull(inputStream);
    try (ObjectInputStream in = new ObjectInputStream(inputStream)) {
      @SuppressWarnings("unchecked")
      final T obj = (T) in.readObject();
      return obj;
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    } catch (ClassNotFoundException ex) {
      throw new AssertionError(ex);
    }
  }
}
