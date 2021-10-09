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

/** The interface Task source. */
public interface TaskSource {

  /**
   * Next task task.
   *
   * @param category the category
   * @return the task
   * @throws InterruptedException the interrupted exception
   */
  Task nextTask(String category) throws InterruptedException;

  // --Commented out by Inspection START (10/7/21, 12:37 AM):
  //  /**
  //   * Task done.
  //   *
  //   * @throws IOException the io exception
  //   */
  //  void taskDone() throws IOException;
  // --Commented out by Inspection STOP (10/7/21, 12:37 AM)

  // --Commented out by Inspection START (10/7/21, 12:37 AM):
  //  /**
  //   * Gets all tasks.
  //   *
  //   * @return the all tasks
  //   */
  //  Map<String, List<Task>> getAllTasks();
  // --Commented out by Inspection STOP (10/7/21, 12:37 AM)
}
