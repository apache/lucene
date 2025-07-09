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
package org.apache.lucene.sandbox.codecs.faiss;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

/**
 * Provides a Faiss-based vector format, see corresponding classes in {@code java21/} for docs!
 *
 * @lucene.experimental
 */
public class FaissKnnVectorsFormatProvider extends KnnVectorsFormat {
  private final KnnVectorsFormat delegate;

  public FaissKnnVectorsFormatProvider() {
    this(lookup());
  }

  public FaissKnnVectorsFormatProvider(String description, String indexParams) {
    this(lookup(description, indexParams));
  }

  private FaissKnnVectorsFormatProvider(KnnVectorsFormat delegate) {
    super(delegate.getName());
    this.delegate = delegate;
  }

  private static KnnVectorsFormat lookup(Object... args) {
    try {
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      Class<?> cls =
          lookup.findClass("org.apache.lucene.sandbox.codecs.faiss.FaissKnnVectorsFormat");

      MethodType type =
          MethodType.methodType(
              void.class,
              Arrays.stream(args).map(Object::getClass).collect(Collectors.toUnmodifiableList()));
      MethodHandle constr = lookup.findConstructor(cls, type);

      return (KnnVectorsFormat) constr.invokeWithArguments(args);
    } catch (ClassNotFoundException e) {
      throw new LinkageError("FaissKnnVectorsFormat is missing from JAR file", e);
    } catch (IllegalAccessException | NoSuchMethodException e) {
      throw new LinkageError("FaissKnnVectorsFormat is missing correctly typed constructor", e);
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  @Override
  public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
    return delegate.fieldsWriter(state);
  }

  @Override
  public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
    return delegate.fieldsReader(state);
  }

  @Override
  public int getMaxDimensions(String fieldName) {
    return delegate.getMaxDimensions(fieldName);
  }
}
