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
package org.apache.lucene.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * This class wraps {@link CollectorManager} and owns the collectors the manager creates. It is
 * convenient that clients of the class don't have to worry about keeping the list of collectors, as
 * well as about making the collector's type (C) compatible when reduce is called. Instances of this
 * class cache results of {@link CollectorManager#reduce(Collection)}.
 *
 * <p>Note that instance of this class ignores any {@link Collector} created by {@link
 * CollectorManager#newCollector()} directly, not through {@link #newCollector()}
 *
 * @lucene.experimental
 */
public final class CollectorOwner<C extends Collector, T> {

  private final CollectorManager<C, T> manager;

  private T result;
  private boolean reduced;

  // TODO: For IndexSearcher, the list doesn't have to be synchronized
  //  because we create new collectors sequentially. Drill sideways creates new collectors in
  //  DrillSidewaysQuery#Weight#bulkScorer which is already called concurrently.
  //  I think making the list synchronized here is not a huge concern, at the same time, do we want
  //  to do something about it?
  //  e.g. have boolean property in constructor that makes it threads friendly when set?
  private final List<C> collectors = Collections.synchronizedList(new ArrayList<>());

  public CollectorOwner(CollectorManager<C, T> manager) {
    this.manager = manager;
  }

  /** Return a new {@link Collector}. This must return a different instance on each call. */
  public C newCollector() throws IOException {
    C collector = manager.newCollector();
    collectors.add(collector);
    return collector;
  }

  public C getCollector(int i) {
    return collectors.get(i);
  }

  /** Returns result of {@link CollectorManager#reduce(Collection)}. The result is cached. */
  public T getResult() throws IOException {
    if (reduced == false) {
      result = manager.reduce(collectors);
      reduced = true;
    }
    return result;
  }
}
