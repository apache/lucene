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
 * @param readAdvice Advice regarding the read access pattern
 */
public record IOContext(
    Context context, MergeInfo mergeInfo, FlushInfo flushInfo, ReadAdvice readAdvice) {

  /**
   * Context is a enumerator which specifies the context in which the Directory is being used for.
   */
  public enum Context {
    MERGE,
    READ,
    FLUSH,
    DEFAULT
  };

  public static final IOContext DEFAULT =
      new IOContext(Context.DEFAULT, null, null, ReadAdvice.NORMAL);

  public static final IOContext READONCE = new IOContext(ReadAdvice.SEQUENTIAL);

  public static final IOContext READ = new IOContext(ReadAdvice.NORMAL);

  @SuppressWarnings("incomplete-switch")
  public IOContext {
    Objects.requireNonNull(context, "context must not be null");
    Objects.requireNonNull(readAdvice, "readAdvice must not be null");
    switch (context) {
      case MERGE -> Objects.requireNonNull(
          mergeInfo, "mergeInfo must not be null if context is MERGE");
      case FLUSH -> Objects.requireNonNull(
          flushInfo, "flushInfo must not be null if context is FLUSH");
    }
    if (context == Context.MERGE && readAdvice != ReadAdvice.SEQUENTIAL) {
      throw new IllegalArgumentException(
          "The MERGE context must use the SEQUENTIAL read access advice");
    }
    if ((context == Context.FLUSH || context == Context.DEFAULT)
        && readAdvice != ReadAdvice.NORMAL) {
      throw new IllegalArgumentException(
          "The FLUSH and DEFAULT contexts must use the NORMAL read access advice");
    }
  }

  private IOContext(ReadAdvice accessAdvice) {
    this(Context.READ, null, null, accessAdvice);
  }

  /** Creates an IOContext for flushing. */
  public IOContext(FlushInfo flushInfo) {
    this(Context.FLUSH, null, flushInfo, ReadAdvice.NORMAL);
  }

  /** Creates an IOContext for merging. */
  public IOContext(MergeInfo mergeInfo) {
    // Merges read input segments sequentially.
    this(Context.MERGE, mergeInfo, null, ReadAdvice.SEQUENTIAL);
  }

  /**
   * Return an updated {@link IOContext} that has the provided {@link ReadAdvice} if the {@link
   * Context} is a {@link Context#READ} context, otherwise return this existing instance. This helps
   * preserve a {@link ReadAdvice#SEQUENTIAL} advice for merging, which is always the right choice,
   * while allowing {@link IndexInput}s open for searching to use arbitrary {@link ReadAdvice}s.
   */
  public IOContext withReadAdvice(ReadAdvice advice) {
    if (context == Context.READ) {
      return new IOContext(advice);
    } else {
      return this;
    }
  }
}
