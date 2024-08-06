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
package org.apache.lucene.codecs.lucene912;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Optional;
import java.util.function.Function;
import org.apache.lucene.internal.vectorization.VectorizationProvider;
import org.apache.lucene.store.IndexInput;

/** Utility class to decode postings. */
public abstract class PostingDecodingUtil {

  private static Function<IndexInput, PostingDecodingUtil> lookup() {
    final var vectorMod =
        Optional.ofNullable(VectorizationProvider.class.getModule().getLayer())
            .orElse(ModuleLayer.boot())
            .findModule("jdk.incubator.vector");
    vectorMod.ifPresent(VectorizationProvider.class.getModule()::addReads);
    try {
      final var lookup = MethodHandles.lookup();
      final var cls =
          lookup.findClass(
              "org.apache.lucene.internal.vectorization.MemorySegmentPostingDecodingUtilProvider");
      final var ctor = lookup.findConstructor(cls, MethodType.methodType(void.class));
      return (Function<IndexInput, PostingDecodingUtil>) ctor.invoke();
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Throwable th) {
      throw new AssertionError(th);
    }
  }

  private static final class Holder {
    private Holder() {}

    static final Function<IndexInput, PostingDecodingUtil> LOOKUP = lookup();
  }

  public static PostingDecodingUtil wrap(IndexInput in) throws IOException {
    return Holder.LOOKUP.apply(in);
  }

  /**
   * Read {@code count} longs. This number must not exceed 64. Apply shift {@code bShift} and mask
   * {@code bMask} and store the result in {@code b} starting at offset 0. Apply mask {@code cMask}
   * and store the result in {@code c} starting at offset 0. As a side-effect, this method may
   * override entries in {@code b} and {@code c} at indexes between {@code count+1} included and
   * {@code 64} excluded.
   */
  public abstract void splitLongs(int count, long[] b, int bShift, long bMask, long[] c, long cMask)
      throws IOException;
}
