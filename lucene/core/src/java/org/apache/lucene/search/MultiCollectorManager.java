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
import java.util.List;

/**
 * A {@link CollectorManager} implements which wrap a set of {@link CollectorManager} as {@link
 * MultiCollector} acts for {@link Collector}.
 */
public class MultiCollectorManager implements CollectorManager<Collector, Object[]> {

  private final CollectorManager<Collector, ?>[] collectorManagers;

  @SafeVarargs
  @SuppressWarnings({"varargs", "unchecked"})
  public MultiCollectorManager(
      final CollectorManager<? extends Collector, ?>... collectorManagers) {
    if (collectorManagers.length < 1) {
      throw new IllegalArgumentException("There must be at least one collector manager");
    }

    for (CollectorManager<? extends Collector, ?> collectorManager : collectorManagers) {
      if (collectorManager == null) {
        throw new IllegalArgumentException("Collector managers should all be non-null");
      }
    }

    this.collectorManagers = (CollectorManager[]) collectorManagers;
  }

  @Override
  public Collector newCollector() throws IOException {
    Collector[] collectors = new Collector[collectorManagers.length];
    for (int i = 0; i < collectorManagers.length; i++) {
      collectors[i] = collectorManagers[i].newCollector();
    }
    return MultiCollector.wrap(collectors);
  }

  @Override
  public Object[] reduce(Collection<Collector> reducableCollectors) throws IOException {
    final int size = reducableCollectors.size();
    final Object[] results = new Object[collectorManagers.length];
    for (int i = 0; i < collectorManagers.length; i++) {
      final List<Collector> reducableCollector = new ArrayList<>(size);
      for (Collector collector : reducableCollectors) {
        // MultiCollector will not actually wrap the collector if only one is provided, so we
        // check the instance type here:
        if (collector instanceof MultiCollector) {
          reducableCollector.add(((MultiCollector) collector).getCollectors()[i]);
        } else {
          reducableCollector.add(collector);
        }
      }
      results[i] = collectorManagers[i].reduce(reducableCollector);
    }
    return results;
  }
}
