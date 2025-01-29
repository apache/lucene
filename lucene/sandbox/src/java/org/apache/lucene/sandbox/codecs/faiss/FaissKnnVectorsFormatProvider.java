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
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Arrays;
import java.util.logging.Logger;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

/**
 * Wraps <a href="https://github.com/facebookresearch/faiss">Faiss</a> to create and search vector
 * indexes. This class is mainly for backwards compatibility with older versions of Java (<22), use
 * underlying format directly after upgrade.
 *
 * @lucene.experimental
 */
public class FaissKnnVectorsFormatProvider extends KnnVectorsFormat {
  private final KnnVectorsFormat delegate;

  public FaissKnnVectorsFormatProvider() {
    this(new Object[0]);
  }

  public FaissKnnVectorsFormatProvider(Object... args) {
    super(FaissKnnVectorsFormatProvider.class.getSimpleName());

    KnnVectorsFormat delegate;
    try {
      Class<?> cls =
          MethodHandles.lookup()
              .findClass("org.apache.lucene.sandbox.codecs.faiss.FaissKnnVectorsFormat");

      MethodType methodType =
          MethodType.methodType(
              void.class, Arrays.stream(args).map(Object::getClass).toArray(Class<?>[]::new));

      delegate =
          (KnnVectorsFormat)
              MethodHandles.lookup().findConstructor(cls, methodType).invokeWithArguments(args);

    } catch (
        @SuppressWarnings("unused")
        ClassNotFoundException e) {

      delegate = new Lucene99HnswVectorsFormat();
      Logger.getLogger(getClass().getName())
          .warning("FaissKnnVectorsFormat class missing, falling back to " + delegate);

    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new LinkageError("FaissKnnVectorsFormat is missing correctly typed constructor", e);
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
    this.delegate = delegate;
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
