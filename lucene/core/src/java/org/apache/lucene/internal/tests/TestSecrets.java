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
package org.apache.lucene.internal.tests;

import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SegmentReader;

/** A set of static methods returning test secret accessors. */
public final class TestSecrets {
  private TestSecrets() {}

  /** Return the accessor to internal secrets for an {@link IndexWriter}. */
  public static IndexWriterSecrets getSecrets(IndexWriter writer) {
    return (IndexWriterSecrets) writer.getTestSecrets(IndexWriterSecrets.PRIVATE_ACCESS_TOKEN);
  }

  /** Return the accessor to internal secrets for an {@link IndexReader}. */
  public static IndexPackageSecrets getIndexPackageSecrets() {
    return (IndexPackageSecrets)
        org.apache.lucene.index.PackageSecrets.getTestSecrets(
            IndexPackageSecrets.PRIVATE_ACCESS_TOKEN);
  }

  /** Return the accessor to internal secrets for an {@link SegmentReader}. */
  public static SegmentReaderSecrets getSecrets(SegmentReader segmentReader) {
    return (SegmentReaderSecrets)
        segmentReader.getTestSecrets(SegmentReaderSecrets.PRIVATE_ACCESS_TOKEN);
  }

  /** Return the accessor to internal secrets for an {@link IndexWriter}. */
  public static ConcurrentMergeSchedulerSecrets getSecrets(ConcurrentMergeScheduler scheduler) {
    return (ConcurrentMergeSchedulerSecrets)
        scheduler.getTestSecrets(ConcurrentMergeSchedulerSecrets.PRIVATE_ACCESS_TOKEN);
  }
}
