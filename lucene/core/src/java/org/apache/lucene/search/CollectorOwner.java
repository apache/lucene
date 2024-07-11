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
import java.util.Collections;
import java.util.List;

/**
 * Like {@link CollectorManager}, but it owns the collectors its manager creates. Benefit is that
 * clients of the class don't have to worry about keeping the list of collectors, as well as about
 * making the collectors type (C) compatible when reduce is called.
 *
 * @lucene.experimental
 */
public final class CollectorOwner<C extends Collector, T> {

  private final CollectorManager<C, T> manager;

  private T result;
  private boolean reduced;

  // TODO: Normally, for IndexSearcher, we don't need parallelized write access to the list
  //  because we create new collectors sequentially. But drill sideways creates new collectors in
  //  DrillSidewaysQuery#Weight#bulkScorer which is already called concurrently.
  //  I think making the list sychronized here is not a huge concern, at the same time, do we want
  // to do something about it?
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

  /**
   * Reduce the results of individual collectors into a meaningful result. For instance a {@link
   * TopDocsCollector} would compute the {@link TopDocsCollector#topDocs() top docs} of each
   * collector and then merge them using {@link TopDocs#merge(int, TopDocs[])}. This method must be
   * called after collection is finished on all provided collectors.
   */
  public T reduce() throws IOException {
    result = manager.reduce(collectors);
    reduced = true;
    return result;
  }

  public static <C extends Collector, T> CollectorOwner<C, T> hire(CollectorManager<C, T> manager) {
    // TODO: can we guarantee that the manager didn't create any Collectors yet?
    //  Or should we expect the new owner to be able to reduce only the work the manager has done
    // after it was hired?
    return new CollectorOwner<>(manager);
  }

  public T getResult() {
    if (reduced == false) {
      throw new IllegalStateException("reduce() must be called first.");
    }
    return result;
  }
}
