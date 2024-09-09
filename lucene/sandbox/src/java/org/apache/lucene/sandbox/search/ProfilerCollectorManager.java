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

package org.apache.lucene.sandbox.search;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;

/** Collector manager for {@link ProfilerCollector} */
public abstract class ProfilerCollectorManager
    implements CollectorManager<ProfilerCollector, ProfilerCollectorResult> {

  private final String reason;

  /**
   * Creates a profiler collector manager provided a certain reason
   *
   * @param reason the reason for the collection
   */
  public ProfilerCollectorManager(String reason) {
    this.reason = reason;
  }

  /** Creates the collector to be wrapped with a {@link ProfilerCollector} */
  protected abstract Collector createCollector() throws IOException;

  @Override
  public final ProfilerCollector newCollector() throws IOException {
    return new ProfilerCollector(createCollector(), reason, List.of());
  }

  @Override
  public ProfilerCollectorResult reduce(Collection<ProfilerCollector> collectors)
      throws IOException {
    String name = null;
    String reason = null;
    long time = 0;

    for (ProfilerCollector collector : collectors) {
      assert name == null || name.equals(collector.getName());
      name = collector.getName();
      assert reason == null || reason.equals(collector.getReason());
      reason = collector.getReason();
      ProfilerCollectorResult profileResult = collector.getProfileResult();
      assert profileResult.getTime() == collector.getTime();
      time += profileResult.getTime();
    }

    return new ProfilerCollectorResult(name, reason, time, List.of());
  }
}
