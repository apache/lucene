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

// --Commented out by Inspection START (10/7/21, 12:31 AM):
/// ** The type Keep no commits deletion policy. */
// public final class KeepNoCommitsDeletionPolicy extends IndexDeletionPolicy {
//
//  /** Sole constructor. */
//  public KeepNoCommitsDeletionPolicy() {}
//
//  @Override
//  public void onInit(List<? extends IndexCommit> commits) {
//    // Note that commits.size() should normally be 1:
//    onCommit(commits);
//  }
//
//  @Override
//  public void onCommit(List<? extends IndexCommit> commits) {
//    // Note that commits.size() should normally be 2 (if not
//    // called by onInit above):
//    for (IndexCommit commit : commits) {
//      commit.delete();
//    }
//  }
// }
// --Commented out by Inspection STOP (10/7/21, 12:31 AM)
