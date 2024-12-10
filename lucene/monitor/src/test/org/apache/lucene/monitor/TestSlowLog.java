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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestSlowLog extends LuceneTestCase {

  public void testAddQuery() {
    SlowLog slowLog = new SlowLog();
    long time1 = 1;
    long time2 = 2;
    long time3 = 3;
    slowLog.addQuery("query1", time1);
    slowLog.addQuery("query2", time2);
    slowLog.addQuery("query3", time3);

    Iterator<SlowLog.Entry> iterator = slowLog.iterator();
    long curQuery = 1;
    while (iterator.hasNext()) {
      SlowLog.Entry entry = iterator.next();
      assertEquals(entry.queryId, "query" + curQuery);
      assertEquals(entry.time, curQuery);
      curQuery += 1;
    }
  }

  public void testAddAllQueries() {
    SlowLog slowLog = new SlowLog();
    long time1 = 1;
    long time2 = 2;
    long time3 = 3;
    List<SlowLog.Entry> queries = new ArrayList<>();
    queries.add(new SlowLog.Entry("query1", time1));
    queries.add(new SlowLog.Entry("query2", time2));
    queries.add(new SlowLog.Entry("query3", time3));
    slowLog.addAll(queries);

    Iterator<SlowLog.Entry> iterator = slowLog.iterator();
    long curQuery = 1;
    while (iterator.hasNext()) {
      SlowLog.Entry entry = iterator.next();
      assertEquals(entry.queryId, "query" + curQuery);
      assertEquals(entry.time, curQuery);
      curQuery += 1;
    }
  }

  public void testToString() {
    SlowLog slowLog = new SlowLog();
    long time1 = 1;
    long time2 = 2;
    long time3 = 3;
    slowLog.addQuery("query1", time1);
    slowLog.addQuery("query2", time2);
    slowLog.addQuery("query3", time3);

    assertEquals(slowLog.toString(), "query1 [1ns]\nquery2 [2ns]\nquery3 [3ns]\n");
  }
}
