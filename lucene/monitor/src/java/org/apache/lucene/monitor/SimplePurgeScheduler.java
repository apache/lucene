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

package org.apache.lucene.monitor;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.util.NamedThreadFactory;

final class SimplePurgeScheduler implements PurgeScheduler {
  @Override
  public PurgeScheduledTask schedule(
      Runnable task, long purgeFrequency, TimeUnit purgeFrequencyUnits) {
    final NamedThreadFactory threadFactory = new NamedThreadFactory("cache-purge");
    final ScheduledExecutorService purgeExecutor =
        Executors.newSingleThreadScheduledExecutor(threadFactory);
    purgeExecutor.scheduleAtFixedRate(task, purgeFrequency, purgeFrequency, purgeFrequencyUnits);
    return purgeExecutor::shutdown;
  }
}
