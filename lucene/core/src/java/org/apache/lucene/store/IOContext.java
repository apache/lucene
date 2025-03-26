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

import org.apache.lucene.util.Constants;

/**
 * IOContext holds additional details on the merge/search context. An IOContext object can never be
 * passed as a {@code null} parameter to either {@link
 * org.apache.lucene.store.Directory#openInput(String, IOContext)} or {@link
 * org.apache.lucene.store.Directory#createOutput(String, IOContext)}
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

  /**
   * A default context for normal reads/writes. Use {@link #withReadAdvice(ReadAdvice)} to specify
   * another {@link ReadAdvice}.
   *
   * <p>It will use {@link ReadAdvice#RANDOM} by default, unless set by system property {@code
   * org.apache.lucene.store.defaultReadAdvice}.
   */
  IOContext DEFAULT = new DefaultIOContext(Constants.DEFAULT_READADVICE);

  /**
   * A default context for reads with {@link ReadAdvice#SEQUENTIAL}.
   *
   * <p>This context should only be used when the read operations will be performed in the same
   * thread as the thread that opens the underlying storage.
   */
  IOContext READONCE = new DefaultIOContext(ReadAdvice.SEQUENTIAL);

  /** Returns an {@link IOContext} for merging with the specified {@link MergeInfo} */
  static IOContext merge(MergeInfo mergeInfo) {
    return new MergeIOContext(mergeInfo);
  }

  /** Returns an {@link IOContext} for flushing with the specified {@link FlushInfo} */
  static IOContext flush(FlushInfo flushInfo) {
    return new FlushIOContext(flushInfo);
  }

  /** The {@link Context} this context is for */
  Context context();

  /** Merge info, if {@link #context()} is {@link Context#MERGE} */
  MergeInfo mergeInfo();

  /** Flush info, if {@link #context()} is {@link Context#FLUSH} */
  FlushInfo flushInfo();

  /** Advice on the expected read access pattern */
  ReadAdvice readAdvice();

  /**
   * Return an updated {@link IOContext} that has the provided {@link ReadAdvice}, if the provided
   * advice is compatible with the current context, otherwise return this existing instance. This
   * helps preserve a {@link ReadAdvice#SEQUENTIAL} advice for merging, which is always the right
   * choice, while allowing {@link IndexInput}s open for searching to use arbitrary {@link
   * ReadAdvice}s.
   */
  IOContext withReadAdvice(ReadAdvice advice);
}
