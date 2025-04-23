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
package org.apache.lucene.index;

import java.io.IOException;
import java.util.List;

/**
 * This {@link IndexDeletionPolicy} implementation keeps the last N commits and removes all prior
 * commits after a new commit is done. This policy can be useful for maintaining a history of recent
 * commits while still managing index size.
 */
public class KeepLastNCommitsDeletionPolicy extends IndexDeletionPolicy {

  private final int numCommitsToKeep;

  /**
   * Constructor that sets the number of commits to keep.
   *
   * @param numCommitsToKeep the number of most recent commits to retain
   * @throws IllegalArgumentException if numCommitsToKeep is not positive
   */
  public KeepLastNCommitsDeletionPolicy(int numCommitsToKeep) {
    if (numCommitsToKeep <= 0) {
      throw new IllegalArgumentException("number of recent commits to keep must be positive");
    }
    this.numCommitsToKeep = numCommitsToKeep;
  }

  @Override
  public void onInit(List<? extends IndexCommit> commits) throws IOException {
    onCommit(commits);
  }

  /** Deletes all but the last N commits. */
  @Override
  public void onCommit(List<? extends IndexCommit> commits) throws IOException {
    // The commits list is already sorted from oldest to newest
    int size = commits.size();
    for (int i = 0; i < size - numCommitsToKeep; i++) {
      commits.get(i).delete();
    }
  }
}
