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
package org.apache.lucene.search.join;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.Supplier;
import org.apache.lucene.search.CollectorManager;

class MergeableCollectorManager<C extends MergeableCollector<C>> implements CollectorManager<C, C> {
  private final Supplier<C> collectorSupplier;

  public MergeableCollectorManager(Supplier<C> collectorSupplier) {
    this.collectorSupplier = collectorSupplier;
  }

  @Override
  public C newCollector() {
    return collectorSupplier.get();
  }

  @Override
  public C reduce(Collection<C> collectors) throws IOException {
    C firstCollector = null;
    Iterator<C> iterator = collectors.iterator();
    while (iterator.hasNext()) {
      if (firstCollector == null) {
        firstCollector = iterator.next();
      } else {
        firstCollector.merge(iterator.next());
      }
    }
    return firstCollector;
  }
}
