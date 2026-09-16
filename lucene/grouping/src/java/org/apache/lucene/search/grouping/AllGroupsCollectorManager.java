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
package org.apache.lucene.search.grouping;

import java.util.Collection;
import java.util.HashSet;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.lucene.search.CollectorManager;

/**
 * A CollectorManager implementation for AllGroupsCollector.
 *
 * @lucene.experimental
 */
public class AllGroupsCollectorManager<T>
    implements CollectorManager<AllGroupsCollector<T>, Collection<T>> {

  private final Supplier<GroupSelector<T>> groupSelectorFactory;

  /**
   * Creates a new AllGroupsCollectorManager.
   *
   * @param groupSelectorFactory factory to create group selectors for each collector
   */
  public AllGroupsCollectorManager(Supplier<GroupSelector<T>> groupSelectorFactory) {
    this.groupSelectorFactory = groupSelectorFactory;
  }

  @Override
  public AllGroupsCollector<T> newCollector() {
    return new AllGroupsCollector<>(groupSelectorFactory.get());
  }

  @Override
  public Collection<T> reduce(Collection<AllGroupsCollector<T>> collectors) {
    // Merge groups from all collectors
    return collectors.stream()
        .flatMap(c -> c.getGroups().stream())
        .collect(Collectors.toCollection(HashSet::new));
  }
}
