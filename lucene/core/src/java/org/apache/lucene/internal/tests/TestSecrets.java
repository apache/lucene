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

import java.util.Objects;
import java.util.function.Consumer;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SegmentReader;

/**
 * A set of static methods returning accessors for internal, package-private functionality in
 * Lucene.
 */
public final class TestSecrets {
  /*
   * The JDK uses unsafe to ensure the secrets-initializing classes have their static blocks
   * invoked. We could just leverage the JLS and invoke a static method (or a constructor) on the
   * class but the method below seems simpler and has no side-effects.
   */
  static {
    Consumer<Class<?>> ensureInitialized =
        clazz -> {
          try {
            // A no-op forName call has a side-effect of initializing the class. This only happens
            // once and has no side-effects.
            Class.forName(clazz.getName());
          } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
          }
        };

    ensureInitialized.accept(ConcurrentMergeScheduler.class);
    ensureInitialized.accept(SegmentReaderAccess.class);
    ensureInitialized.accept(IndexWriter.class);
  }

  private static IndexPackageAccess indexPackageAccess;
  private static ConcurrentMergeSchedulerAccess cmsAccess;
  private static SegmentReaderAccess segmentReaderAccess;
  private static IndexWriterAccess indexWriterAccess;

  private TestSecrets() {}

  /** Return the accessor to internal secrets for an {@link IndexReader}. */
  public static IndexPackageAccess getIndexPackageAccess() {
    return Objects.requireNonNull(indexPackageAccess);
  }

  /** Return the accessor to internal secrets for an {@link ConcurrentMergeScheduler}. */
  public static ConcurrentMergeSchedulerAccess getConcurrentMergeSchedulerAccess() {
    return Objects.requireNonNull(cmsAccess);
  }

  /** Return the accessor to internal secrets for an {@link SegmentReader}. */
  public static SegmentReaderAccess getSegmentReaderAccess() {
    return Objects.requireNonNull(segmentReaderAccess);
  }

  /** Return the accessor to internal secrets for an {@link IndexWriter}. */
  public static IndexWriterAccess getIndexWriterAccess() {
    return Objects.requireNonNull(indexWriterAccess);
  }

  public static void setIndexWriterAccess(IndexWriterAccess indexWriterAccess) {
    ensureNull(TestSecrets.indexWriterAccess);
    TestSecrets.indexWriterAccess = indexWriterAccess;
  }

  public static void setIndexPackageAccess(IndexPackageAccess indexPackageAccess) {
    ensureNull(TestSecrets.indexPackageAccess);
    TestSecrets.indexPackageAccess = indexPackageAccess;
  }

  public static void setConcurrentMergeSchedulerAccess(ConcurrentMergeSchedulerAccess cmsAccess) {
    ensureNull(TestSecrets.cmsAccess);
    TestSecrets.cmsAccess = cmsAccess;
  }

  public static void setSegmentReaderAccess(SegmentReaderAccess segmentReaderAccess) {
    ensureNull(TestSecrets.segmentReaderAccess);
    TestSecrets.segmentReaderAccess = segmentReaderAccess;
  }

  private static void ensureNull(Object ob) {
    if (ob != null) {
      throw new AssertionError("The accessor is already set.");
    }
  }
}
