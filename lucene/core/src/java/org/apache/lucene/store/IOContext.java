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
 */
public class IOContext {

  /**
   * Context is a enumerator which specifies the context in which the Directory is being used for.
   */
  public enum Context {
    MERGE,
    READ,
    FLUSH,
    DEFAULT
  };

  /** An object of a enumerator Context type */
  public final Context context;

  public final MergeInfo mergeInfo;

  public final FlushInfo flushInfo;

  /** This flag indicates that the file will be opened, then fully read sequentially then closed. */
  public final boolean readOnce;

  /**
   * This flag indicates that the file will be accessed randomly. If this flag is set, then readOnce
   * will be false.
   */
  public final boolean randomAccess;

  /**
   * This flag is used for files that are a small fraction of the total index size and are expected
   * to be heavily accessed in random-access fashion. Some {@link Directory} implementations may
   * choose to load such files into physical memory (e.g. Java heap) as a way to provide stronger
   * guarantees on query latency. If this flag is set, then {@link #randomAccess} will be true.
   */
  public final boolean load;

  public static final IOContext DEFAULT = new IOContext(Context.DEFAULT);

  public static final IOContext READONCE = new IOContext(true, false, false);

  public static final IOContext READ = new IOContext(false, false, false);

  public static final IOContext LOAD = new IOContext(false, true, true);

  public static final IOContext RANDOM = new IOContext(false, false, true);

  public IOContext() {
    this(false, false, false);
  }

  public IOContext(FlushInfo flushInfo) {
    assert flushInfo != null;
    this.context = Context.FLUSH;
    this.mergeInfo = null;
    this.readOnce = false;
    this.load = false;
    this.randomAccess = false;
    this.flushInfo = flushInfo;
  }

  public IOContext(Context context) {
    this(context, null);
  }

  private IOContext(boolean readOnce, boolean load, boolean randomAccess) {
    if (readOnce && randomAccess) {
      throw new IllegalArgumentException("cannot be both readOnce and randomAccess");
    }
    if (load && randomAccess == false) {
      throw new IllegalArgumentException("cannot be load but not randomAccess");
    }
    this.context = Context.READ;
    this.mergeInfo = null;
    this.readOnce = readOnce;
    this.load = load;
    this.randomAccess = randomAccess;
    this.flushInfo = null;
  }

  public IOContext(MergeInfo mergeInfo) {
    this(Context.MERGE, mergeInfo);
  }

  private IOContext(Context context, MergeInfo mergeInfo) {
    assert context != Context.MERGE || mergeInfo != null
        : "MergeInfo must not be null if context is MERGE";
    assert context != Context.FLUSH : "Use IOContext(FlushInfo) to create a FLUSH IOContext";
    this.context = context;
    this.readOnce = false;
    this.load = false;
    this.randomAccess = false;
    this.mergeInfo = mergeInfo;
    this.flushInfo = null;
  }

  /**
   * This constructor is used to initialize a {@link IOContext} instance with a new value for the
   * readOnce variable. This automatically sets {@link #randomAccess} and {@link #load} to {@code
   * false}.
   *
   * @param ctxt {@link IOContext} object whose information is used to create the new instance
   *     except the readOnce variable.
   * @param readOnce The new {@link IOContext} object will use this value for readOnce.
   */
  public IOContext(IOContext ctxt, boolean readOnce) {
    this.context = ctxt.context;
    this.mergeInfo = ctxt.mergeInfo;
    this.flushInfo = ctxt.flushInfo;
    this.readOnce = readOnce;
    this.randomAccess = false;
    this.load = false;
  }

  /**
   * Return an updated {@link IOContext} that has {@link IOContext#randomAccess} set to true if
   * {@link IOContext#context} is {@link Context#READ} or {@link Context#DEFAULT}. Otherwise, this
   * returns this instance. This helps preserve sequential access for merging, which is always the
   * right choice, while allowing {@link IndexInput}s open for searching to use random access.
   */
  public IOContext withRandomAccess() {
    if (context == Context.READ || context == Context.DEFAULT) {
      return RANDOM;
    } else {
      return this;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(context, flushInfo, mergeInfo, readOnce, load, randomAccess);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    IOContext other = (IOContext) obj;
    if (context != other.context) return false;
    if (!Objects.equals(flushInfo, other.flushInfo)) return false;
    if (!Objects.equals(mergeInfo, other.mergeInfo)) return false;
    if (readOnce != other.readOnce) return false;
    if (load != other.load) return false;
    if (randomAccess != other.randomAccess) return false;
    return true;
  }

  @Override
  public String toString() {
    return "IOContext [context="
        + context
        + ", mergeInfo="
        + mergeInfo
        + ", flushInfo="
        + flushInfo
        + ", readOnce="
        + readOnce
        + ", load="
        + load
        + ", randomAccess="
        + randomAccess
        + "]";
  }
}
