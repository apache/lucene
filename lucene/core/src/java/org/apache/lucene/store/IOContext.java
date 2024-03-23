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
 */
public record IOContext(
    Context context, MergeInfo mergeInfo, FlushInfo flushInfo, boolean readOnce, boolean load) {

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

  public static final IOContext READONCE = new IOContext(true, false);

  public static final IOContext READ = new IOContext(false, false);

  public static final IOContext LOAD = new IOContext(false, true);

  @SuppressWarnings("incomplete-switch")
  public IOContext {
    switch (context) {
      case MERGE -> Objects.requireNonNull(
          mergeInfo, "mergeInfo must not be null if context is MERGE");
      case FLUSH -> Objects.requireNonNull(
          flushInfo, "flushInfo must not be null if context is FLUSH");
    }
  }

  public IOContext() {
    this(false, false);
  }

  public IOContext(FlushInfo flushInfo) {
    this(Context.FLUSH, null, flushInfo, false, false);
  }

  public IOContext(Context context) {
    this(context, null, null, false, false);
  }

  private IOContext(boolean readOnce, boolean load) {
    this(Context.READ, null, null, readOnce, load);
  }

  public IOContext(MergeInfo mergeInfo) {
    this(Context.MERGE, mergeInfo, null, false, false);
  }

  /**
   * This constructor is used to initialize a {@link IOContext} instance with a new value for the
   * readOnce variable.
   *
   * @param ctxt {@link IOContext} object whose information is used to create the new instance
   *     except the readOnce variable.
   * @param readOnce The new {@link IOContext} object will use this value for readOnce.
   */
  public IOContext(IOContext ctxt, boolean readOnce) {
    this(ctxt.context, ctxt.mergeInfo, ctxt.flushInfo, readOnce, ctxt.load);
  }
}
