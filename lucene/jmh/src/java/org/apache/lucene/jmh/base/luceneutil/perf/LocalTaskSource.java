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
package org.apache.lucene.jmh.base.luceneutil.perf;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.BytesRef;

/** The type Local task source. */
// Serves up tasks from locally loaded list:
public class LocalTaskSource implements TaskSource {

  private final AtomicInteger nextTask = new AtomicInteger();
  private final Map<String, List<Task>> catToTasks;

  /**
   * Instantiates a new Local task source.
   *
   * @param indexState the index state
   * @param taskParser the task parser
   * @param tasksFile the tasks file
   * @param staticRandom the static randomhe num task per cat
   * @param doPKLookup the do pk lookup
   * @throws IOException the io exception
   * @throws ParseException the parse exception
   */
  public LocalTaskSource(
      IndexState indexState,
      TaskParser taskParser,
      String tasksFile,
      Random staticRandom,
      boolean doPKLookup)
      throws IOException, ParseException {

    Map<String, List<Task>> loadedTasks = loadTasks(taskParser, tasksFile);

    for (List<Task> tasks : loadedTasks.values()) {
      Collections.shuffle(tasks, staticRandom);
    }

    final IndexSearcher searcher = indexState.mgr.acquire();
    final int maxDoc;
    try {
      maxDoc = searcher.getIndexReader().maxDoc();
    } finally {
      indexState.mgr.release(searcher);
    }

    // Add PK tasks
    // System.out.println("WARNING: skip PK tasks");
    if (doPKLookup) {

      for (List<Task> tasks : loadedTasks.values()) {
        final int numPKTasks = (int) Math.min(maxDoc / 6000., tasks.size());
        for (int idx = 0; idx < numPKTasks; idx++) {
          final Set<BytesRef> pkSeenIDs = new HashSet<>();
          final Set<Integer> pkSeenIntIDs = new HashSet<>();
          tasks.add(new PKLookupTask(maxDoc, staticRandom, 4000, pkSeenIDs, idx));
          tasks.add(new PointsPKLookupTask(maxDoc, staticRandom, 4000, pkSeenIntIDs, idx));
        }
      }

      //      for (List<Task> tasks : loadedTasks.values()) {
      //        final Set<BytesRef> pkSeenSingleIDs = new HashSet<BytesRef>();
      //        for (int idx = 0; idx < numPKTasks * 100; idx++) {
      //          tasks.add(new SinglePKLookupTask(maxDoc, staticRandom, pkSeenSingleIDs, idx));
      //        }
      //      }
    }
    catToTasks = loadedTasks;
    System.out.println("TASK LEN=" + catToTasks.size());
  }

  @Override
  public Task nextTask(String category) {
    List<Task> catTasks = this.catToTasks.get(category);
    if (catTasks == null) {
      throw new IllegalArgumentException(
          "Category " + category + " not found in " + catToTasks.keySet());
    }
    int next = nextTask.getAndIncrement();
    if (next >= catTasks.size()) {
      nextTask.set(0);
      next = 0;
    }
    return catTasks.get(next);
  }

  /**
   * Load tasks map.
   *
   * @param taskParser the task parser
   * @param filePath the file path
   * @return the map
   * @throws IOException the io exception
   * @throws ParseException the parse exception
   */
  static Map<String, List<Task>> loadTasks(TaskParser taskParser, String filePath)
      throws IOException, ParseException {
    Map<String, List<Task>> taskMap = new HashMap<>();

    final BufferedReader taskFile =
        new BufferedReader(
            new InputStreamReader(
                Files.newInputStream(Paths.get(filePath)), StandardCharsets.UTF_8),
            16384);
    while (true) {
      String line = taskFile.readLine();
      if (line == null) {
        break;
      }
      line = line.trim();
      if (line.indexOf("#") == 0) {
        // Ignore comment lines
        continue;
      }
      if (line.length() == 0) {
        // Ignore blank lines
        continue;
      }
      Task task = taskParser.parseOneTask(line);

      taskMap.compute(
          task.getCategory(),
          (k, v) -> {
            if (v == null) {
              v = new ArrayList<>();
            }
            v.add(task);
            return v;
          });
    }
    taskFile.close();
    return taskMap;
  }
}
