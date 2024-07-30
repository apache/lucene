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
package org.apache.lucene.benchmark.byTask.tasks;

import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.benchmark.byTask.feeds.QueryMaker;
import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.TopScoreDocCollectorManager;

/** Does search w/ a custom collector */
public class SearchWithCollectorTask extends SearchTask {

  protected String clnName;

  public SearchWithCollectorTask(PerfRunData runData) {
    super(runData);
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    // check to make sure either the doc is being stored
    PerfRunData runData = getRunData();
    Config config = runData.getConfig();
    if (config.get("collector.class", null) != null) {
      throw new IllegalArgumentException(
          "collector.class is no longer supported as a config parameter, use collector.manager.class instead to provide a CollectorManager class name");
    }
    clnName = config.get("collector.manager.class", "");
  }

  @Override
  public boolean withCollector() {
    return true;
  }

  @Override
  protected CollectorManager<?, ?> createCollectorManager() throws Exception {
    CollectorManager<?, ?> collectorManager;
    if (clnName.equalsIgnoreCase("topScoreDoc") == true) {
      collectorManager = new TopScoreDocCollectorManager(numHits(), Integer.MAX_VALUE);
    } else if (clnName.length() > 0) {
      collectorManager =
          Class.forName(clnName).asSubclass(CollectorManager.class).getConstructor().newInstance();
    } else {
      collectorManager = super.createCollectorManager();
    }
    return collectorManager;
  }

  @Override
  public QueryMaker getQueryMaker() {
    return getRunData().getQueryMaker(this);
  }

  @Override
  public boolean withRetrieve() {
    return false;
  }

  @Override
  public boolean withSearch() {
    return true;
  }

  @Override
  public boolean withTraverse() {
    return false;
  }

  @Override
  public boolean withWarm() {
    return false;
  }
}
