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

import java.io.IOException;
import java.io.PrintStream;
import org.apache.lucene.search.TotalHits;

/** The type Task. */
// Abstract class representing a single task (one query,
// one batch of PK lookups, on respell).  Each Task
// instance is executed and results are recorded in it and
// then later verified/summarized:
public abstract class Task {
  // public String origString;

  /** The Task id. */
  public int taskID;

  /** The Total hit count. */
  public TotalHits totalHitCount;

  /** The Recv time ns. */
  public long recvTimeNS;

  /** Instantiates a new Task. */
  public Task() {}

  /**
   * Go.
   *
   * @param state the state
   * @throws IOException the io exception
   */
  public abstract void go(IndexState state) throws IOException;

  /**
   * Gets category.
   *
   * @return the category
   */
  public abstract String getCategory();

  @Override
  public abstract Task clone();

  /** The Run time nanos. */
  // these are set once the task is executed
  public long runTimeNanos;
  /** The Thread id. */
  public int threadID;

  /**
   * Checksum long.
   *
   * @return the long
   */
  // Called after go, to return "summary" of the results.
  // This may use volatile docIDs -- the checksum is just
  // used to verify the same task run multiple times got
  // the same result, ie that things are in fact thread
  // safe:
  public abstract long checksum();

  /**
   * Print results.
   *
   * @param out the out
   * @param state the state
   * @throws IOException the io exception
   */
  // Called after go to print details of the task & result
  // to stdout:
  public abstract void printResults(PrintStream out, IndexState state) throws IOException;

  /** The constant END_TASK. */
  // Sentinal
  static final Task END_TASK =
      new Task() {

        @Override
        public void go(IndexState state) {}

        @Override
        public String getCategory() {
          return null;
        }

        @Override
        public Task clone() {
          return null;
        }

        @Override
        public long checksum() {
          return 0L;
        }

        @Override
        public void printResults(PrintStream out, IndexState state) {}
      };
}
