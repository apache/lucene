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
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.CollectorManager;

/**
 * A CollectorManager implementation for AllGroupsCollector.
 *
 * @lucene.experimental
 */
public class AllGroupsCollectorManager
    implements CollectorManager<AllGroupsCollector<?>, Collection<?>> {

  private final String groupField;
  private final ValueSource valueSource;
  private final Map<Object, Object> valueSourceContext;

  /** Creates a new AllGroupsCollectorManager for TermGroupSelector. */
  public AllGroupsCollectorManager(String groupField) {
    this.groupField = groupField;
    this.valueSource = null;
    this.valueSourceContext = null;
  }

  /** Creates a new AllGroupsCollectorManager for ValueSourceGroupSelector. */
  public AllGroupsCollectorManager(
      ValueSource valueSource, Map<Object, Object> valueSourceContext) {
    this.groupField = null;
    this.valueSource = valueSource;
    this.valueSourceContext = valueSourceContext;
  }

  @Override
  public AllGroupsCollector<?> newCollector() {
    GroupSelector<?> newGroupSelector;
    if (groupField != null) {
      newGroupSelector = new TermGroupSelector(groupField);
    } else {
      newGroupSelector = new ValueSourceGroupSelector(valueSource, valueSourceContext);
    }

    return new AllGroupsCollector<>(newGroupSelector);
  }

  @Override
  public Collection<?> reduce(Collection<AllGroupsCollector<?>> collectors) {
    if (collectors.isEmpty()) {
      return Collections.emptyList();
    }

    if (collectors.size() == 1) {
      return collectors.iterator().next().getGroups();
    }

    // Merge groups from all collectors
    Set<Object> allGroups = new HashSet<>();
    for (AllGroupsCollector<?> collector : collectors) {
      Collection<?> groups = collector.getGroups();
      if (groups != null) {
        allGroups.addAll(groups);
      }
    }

    return allGroups;
  }
}
