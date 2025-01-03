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

import java.util.Arrays;
import java.util.Objects;
import org.apache.lucene.util.Constants;

/**
 * IOContext holds additional details on the merge/search context. An IOContext object can never be
 * passed as a {@code null} parameter to either {@link
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

  /** Context is an enumerator which specifies the context in which the Directory is being used. */
  public enum Context {
    /** Context for reads and writes that are associated with a merge. */
    MERGE,
    /** Context for writes that are associated with a segment flush. */
    FLUSH,
    /** Default context, can be used for reading or writing. */
    DEFAULT
  };

  /**
   * A default context for normal reads/writes. Use {@link #withReadAdvice(ReadAdvice)} to specify
   * another {@link ReadAdvice}.
   *
   * <p>It will use {@link ReadAdvice#RANDOM} by default, unless set by system property {@code
   * org.apache.lucene.store.defaultReadAdvice}.
   */
  public static final IOContext DEFAULT = new IOContext(Constants.DEFAULT_READADVICE);

  /**
   * A default context for reads with {@link ReadAdvice#SEQUENTIAL}.
   *
   * <p>This context should only be used when the read operations will be performed in the same
   * thread as the thread that opens the underlying storage.
   */
  public static final IOContext READONCE = new IOContext(ReadAdvice.SEQUENTIAL);

  @SuppressWarnings("incomplete-switch")
  public IOContext {
    Objects.requireNonNull(context, "context must not be null");
    Objects.requireNonNull(readAdvice, "readAdvice must not be null");
    switch (context) {
      case MERGE ->
          Objects.requireNonNull(mergeInfo, "mergeInfo must not be null if context is MERGE");
      case FLUSH ->
          Objects.requireNonNull(flushInfo, "flushInfo must not be null if context is FLUSH");
    }
    if ((context == Context.FLUSH || context == Context.MERGE)
        && readAdvice != ReadAdvice.SEQUENTIAL) {
      throw new IllegalArgumentException(
          "The FLUSH and MERGE contexts must use the SEQUENTIAL read access advice");
    }
  }

  /** Creates a default {@link IOContext} for reading/writing with the given {@link ReadAdvice} */
  private IOContext(ReadAdvice accessAdvice) {
    this(Context.DEFAULT, null, null, accessAdvice);
  }

  /** Creates an {@link IOContext} for flushing. */
  public IOContext(FlushInfo flushInfo) {
    this(Context.FLUSH, null, flushInfo, ReadAdvice.SEQUENTIAL);
  }

  /** Creates an {@link IOContext} for merging. */
  public IOContext(MergeInfo mergeInfo) {
    // Merges read input segments sequentially.
    this(Context.MERGE, mergeInfo, null, ReadAdvice.SEQUENTIAL);
  }

  private static final IOContext[] READADVICE_TO_IOCONTEXT =
      Arrays.stream(ReadAdvice.values()).map(IOContext::new).toArray(IOContext[]::new);

  /**
   * Return an updated {@link IOContext} that has the provided {@link ReadAdvice} if the {@link
   * Context} is a {@link Context#DEFAULT} context, otherwise return this existing instance. This
   * helps preserve a {@link ReadAdvice#SEQUENTIAL} advice for merging, which is always the right
   * choice, while allowing {@link IndexInput}s open for searching to use arbitrary {@link
   * ReadAdvice}s.
   */
  public IOContext withReadAdvice(ReadAdvice advice) {
    if (context == Context.DEFAULT) {
      return READADVICE_TO_IOCONTEXT[advice.ordinal()];
    } else {
      return this;
    }
  }
}
