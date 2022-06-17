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
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.index.LeafReaderContext;

/**
 * A wrapper that counts how many times {@link LeafCollector#collect} is called on a {@link
 * TopDocsCollector}.
 */
final class CountingCollectorWrapper extends FilterCollector {

  private final AtomicLong count;

  private CountingCollectorWrapper(Collector in, AtomicLong count) {
    super(in);
    this.count = count;
  }

  @Override
  public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
    return new FilterLeafCollector(super.getLeafCollector(context)) {
      @Override
      public void collect(int doc) throws IOException {
        super.collect(doc);
        count.incrementAndGet();
      }

      @Override
      public DocIdSetIterator competitiveIterator() throws IOException {
        return in.competitiveIterator();
      }
    };
  }

  /** Create a {@link CollectorManager}. */
  public static <S extends ScoreDoc, T extends TopDocs, C extends TopDocsCollector<S>>
      CollectorManager<CountingCollectorWrapper, T> createManager(
          CollectorManager<C, T> manager, AtomicLong count) {
    return new CollectorManager<CountingCollectorWrapper, T>() {

      @Override
      public CountingCollectorWrapper newCollector() throws IOException {
        return new CountingCollectorWrapper(manager.newCollector(), count);
      }

      @SuppressWarnings("unchecked")
      @Override
      public T reduce(Collection<CountingCollectorWrapper> collectors) throws IOException {
        return manager.reduce(collectors.stream().map(coll -> (C) coll.in).toList());
      }
    };
  }
}
