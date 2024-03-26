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

/**
 * IOContext holds additional details on the merge/search context. A IOContext object can never be
 * initialized as null as passed as a parameter to either {@link
 * org.apache.lucene.store.Directory#openInput(String, IOContext)} or {@link
 * org.apache.lucene.store.Directory#createOutput(String, IOContext)}
 *
 * @param context An object of a enumerator Context type
 * @param mergeInfo must be given when {@code context == MERGE}
 * @param flushInfo must be given when {@code context == FLUSH}
 * @param readOnce This flag indicates that the file will be opened, then fully read sequentially
 *     then closed.
 * @param load This flag is used for files that are a small fraction of the total index size and are
 *     expected to be heavily accessed in random-access fashion. Some {@link Directory}
 *     implementations may choose to load such files into physical memory (e.g. Java heap) as a way
 *     to provide stronger guarantees on query latency.
 * @param randomAccess This flag indicates that the file will be accessed randomly. If this flag is
 *     set, then readOnce will be false.
 */
public record IOContext(
    Context context,
    MergeInfo mergeInfo,
    FlushInfo flushInfo,
    boolean readOnce,
    boolean load,
    boolean randomAccess) {

  /**
   * Context is a enumerator which specifies the context in which the Directory is being used for.
   */
  public enum Context {
    MERGE,
    READ,
    FLUSH,
    DEFAULT
  };

  public static final IOContext DEFAULT = new IOContext(Context.DEFAULT);

  public static final IOContext READONCE = new IOContext(true, false, false);

  public static final IOContext READ = new IOContext(false, false, false);

  public static final IOContext LOAD = new IOContext(false, true, true);

  public static final IOContext RANDOM = new IOContext(false, false, true);

  @SuppressWarnings("incomplete-switch")
  public IOContext {
    switch (context) {
      case MERGE -> Objects.requireNonNull(
          mergeInfo, "mergeInfo must not be null if context is MERGE");
      case FLUSH -> Objects.requireNonNull(
          flushInfo, "flushInfo must not be null if context is FLUSH");
    }
    if (load && readOnce) {
      throw new IllegalArgumentException("load and readOnce are mutually exclusive");
    }
    if (readOnce && randomAccess) {
      throw new IllegalArgumentException("readOnce and randomAccess are mutually exclusive");
    }
    if (load && randomAccess == false) {
      throw new IllegalArgumentException("cannot be load but not randomAccess");
    }
  }

  private IOContext(boolean readOnce, boolean load, boolean randomAccess) {
    this(Context.READ, null, null, readOnce, load, randomAccess);
  }

  private IOContext(Context context) {
    this(context, null, null, false, false, false);
  }

  /** Creates an IOContext for flushing. */
  public IOContext(FlushInfo flushInfo) {
    this(Context.FLUSH, null, flushInfo, false, false, false);
  }

  /** Creates an IOContext for merging. */
  public IOContext(MergeInfo mergeInfo) {
    this(Context.MERGE, mergeInfo, null, false, false, false);
  }

  /**
   * Return a copy of this IOContext with {@link #readOnce} set to {@code true}. The {@link #load}
   * flag is set to {@code false}.
   */
  public IOContext toReadOnce() {
    return new IOContext(context, mergeInfo, flushInfo, true, false, randomAccess);
  }
}
