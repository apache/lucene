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

import java.util.Properties;
import org.apache.lucene.benchmark.BenchmarkTestCase;
import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.benchmark.byTask.feeds.JoinDocMaker;
import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.FSDirectory;

public class TestJoinSearchTask extends BenchmarkTestCase {

  private static final int PRODUCTS = 300, SKUS_PER_PRODUCT = 4;

  public void testAlgorithmsAgreeBeforeAndAfterUpdates() throws Exception {
    for (String layout : new String[] {"products", "skus", "mixed", "block"}) {
      execBenchmark(
          new String[] {
            "doc.maker=" + JoinDocMaker.class.getName(),
            "join.layout=" + layout,
            "join.products=" + PRODUCTS,
            "join.skusPerProduct=" + SKUS_PER_PRODUCT,
            "work.dir=" + getWorkDirPath() + "/" + layout,
            "directory=FSDirectory",
            "max.buffered=" + (50 + random().nextInt(500)),
            "CreateIndex",
            "[ { AddJoinDocs > : * ] : 2",
            "CloseIndex",
          });
    }
    assertNumDocs("products", PRODUCTS);
    assertNumDocs("skus", PRODUCTS * SKUS_PER_PRODUCT);
    assertNumDocs("mixed", PRODUCTS * (SKUS_PER_PRODUCT + 1));
    assertNumDocs("block", PRODUCTS * (SKUS_PER_PRODUCT + 1));

    long base;
    try (PerfRunData runData = runData("both")) {
      base = assertAlgorithmsAgree(runData);
    }
    for (String side : new String[] {"products", "skus", "both"}) {
      try (PerfRunData runData = runData(side)) {
        for (String group : new String[] {"split", "mixed", "block"}) {
          JoinUpdateTask update = new JoinUpdateTask(runData);
          update.setParams(group);
          for (int i = 0; i < 3; i++) {
            update.runAndMaybeStats(false);
          }
        }
        // deterministic: data, update and query sequences are seeded by join.seed; the randomized
        // segment sizes and search threads don't affect hit counts
        assertNotEquals(side, base, assertAlgorithmsAgree(runData));
      }
    }
    try (PerfRunData runData = runData("both")) {
      assertEquals("updates are rolled back", base, assertAlgorithmsAgree(runData));
    }
  }

  private PerfRunData runData(String side) throws Exception {
    Properties props = new Properties();
    props.setProperty("join.dir", getWorkDirPath());
    props.setProperty("join.searchThreads", Integer.toString(random().nextInt(3)));
    props.setProperty("join.update.side", side);
    props.setProperty("join.update.batch", "50");
    props.setProperty("print.props", "false");
    return new PerfRunData(new Config(props));
  }

  private static long assertAlgorithmsAgree(PerfRunData runData) throws Exception {
    long expected = -1;
    for (String algorithm : new String[] {"term", "numeric", "globalOrd", "block"}) {
      JoinSearchTask task = new JoinSearchTask(runData);
      task.setParams(algorithm);
      for (int i = 0; i < 50; i++) {
        task.runAndMaybeStats(false);
      }
      if (expected < 0) {
        expected = task.getTotalHits();
        assertTrue(expected > 0);
      }
      assertEquals(algorithm, expected, task.getTotalHits());
    }
    return expected;
  }

  private void assertNumDocs(String layout, int expected) throws Exception {
    try (FSDirectory dir = FSDirectory.open(getWorkDir().resolve(layout).resolve("index"));
        DirectoryReader reader = DirectoryReader.open(dir)) {
      assertEquals(layout, expected, reader.numDocs());
    }
  }
}
