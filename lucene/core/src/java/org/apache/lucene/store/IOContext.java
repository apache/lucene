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

import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

/**
 * IOContext holds additional details on the merge/search context. An IOContext object can never be
 * passed as a {@code null} parameter to either {@link
 * org.apache.lucene.store.Directory#openInput(String, IOContext)} or {@link
 * org.apache.lucene.store.Directory#createOutput(String, IOContext)}
 *
 * <p>Implementations of IOContext are immutable and thread-safe.
 */
public interface IOContext {

  /** Context is an enumerator which specifies the context in which the Directory is being used. */
  enum Context {
    /** Context for reads and writes that are associated with a merge. */
    MERGE,
    /** Context for writes that are associated with a segment flush. */
    FLUSH,
    /** Default context, can be used for reading or writing. */
    DEFAULT
  };

  /** Implemented by classes that can specify hints on how the file will be used */
  interface FileOpenHint {}

  /**
   * A default context for normal reads/writes. Use {@link #withHints} to specify additional
   * information on how the file is to be used.
   *
   * <p>It will use {@link ReadAdvice#RANDOM} by default, unless set by system property {@code
   * org.apache.lucene.store.defaultReadAdvice}.
   */
  @SuppressWarnings("ClassInitializationDeadlock") // FIXME: may cause hangs!
  IOContext DEFAULT = new DefaultIOContext();

  /**
   * A default context for reads with {@link ReadAdvice#SEQUENTIAL}.
   *
   * <p>This context should only be used when the read operations will be performed in the same
   * thread as the thread that opens the underlying storage.
   */
  @SuppressWarnings("ClassInitializationDeadlock") // FIXME: may cause hangs!
  IOContext READONCE = new DefaultIOContext(DataAccessHint.SEQUENTIAL, ReadOnceHint.INSTANCE);

  /** Returns an {@link IOContext} for merging with the specified {@link MergeInfo} */
  static IOContext merge(MergeInfo mergeInfo) {
    Objects.requireNonNull(mergeInfo);
    return new IOContext() {
      @Override
      public Context context() {
        return Context.MERGE;
      }

      @Override
      public MergeInfo mergeInfo() {
        return mergeInfo;
      }

      @Override
      public FlushInfo flushInfo() {
        return null;
      }

      @Override
      public Set<FileOpenHint> hints() {
        return Set.of();
      }

      @Override
      public IOContext withHints(FileOpenHint... hints) {
        return this;
      }
    };
  }

  /** Returns an {@link IOContext} for flushing with the specified {@link FlushInfo} */
  static IOContext flush(FlushInfo flushInfo) {
    Objects.requireNonNull(flushInfo);
    return new IOContext() {
      @Override
      public Context context() {
        return Context.FLUSH;
      }

      @Override
      public MergeInfo mergeInfo() {
        return null;
      }

      @Override
      public FlushInfo flushInfo() {
        return flushInfo;
      }

      @Override
      public Set<FileOpenHint> hints() {
        return Set.of();
      }

      @Override
      public IOContext withHints(FileOpenHint... hints) {
        return this;
      }
    };
  }

  /** The {@link Context} this context is for */
  Context context();

  /** Merge info, if {@link #context()} is {@link Context#MERGE} */
  MergeInfo mergeInfo();

  /** Flush info, if {@link #context()} is {@link Context#FLUSH} */
  FlushInfo flushInfo();

  /** Any hints on how the file will be opened */
  Set<FileOpenHint> hints();

  /** Finds all hints of type {@code cls} */
  default <T extends FileOpenHint> Stream<T> hints(Class<T> cls) {
    return hints().stream().filter(cls::isInstance).map(cls::cast);
  }

  /**
   * Returns an IOContext with the given hints, if it makes sense to do so for this specific
   * context. Otherwise, returns this context.
   *
   * <p>The returned context has the same {@link #context()}, {@link #mergeInfo()}, and {@link
   * #flushInfo()} as this context.
   *
   * <p>This instance is immutable and unaffected by this method call.
   */
  IOContext withHints(FileOpenHint... hints);
}
