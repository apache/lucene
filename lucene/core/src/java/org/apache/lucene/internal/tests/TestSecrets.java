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

import java.lang.StackWalker.StackFrame;
import java.util.Objects;
import java.util.function.Consumer;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.FilterIndexInput;

/**
 * A set of static methods returning accessors for internal, package-private functionality in
 * Lucene. All getters may only be called by the Lucene Test Framework module. Setters are
 * initialized once on startup.
 */
public final class TestSecrets {
  static {
    Consumer<Class<?>> ensureInitialized =
        clazz -> {
          try {
            // A no-op forName here has a side-effect of ensuring the class is loaded and
            // initialized.
            // This only happens once. We could just leverage the JLS and invoke a static
            // method (or a constructor) on the target class but the method below seems simpler.
            // TODO: In Java 15 there's MethodHandles.lookup().ensureInitialized(clazz)
            Class.forName(clazz.getName());
          } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
          }
        };

    ensureInitialized.accept(ConcurrentMergeScheduler.class);
    ensureInitialized.accept(SegmentReader.class);
    ensureInitialized.accept(IndexWriter.class);
    ensureInitialized.accept(FilterIndexInput.class);
  }

  private static IndexPackageAccess indexPackageAccess;
  private static ConcurrentMergeSchedulerAccess cmsAccess;
  private static SegmentReaderAccess segmentReaderAccess;
  private static IndexWriterAccess indexWriterAccess;
  private static FilterIndexInputAccess filterIndexInputAccess;

  private TestSecrets() {}

  /** Return the accessor to internal secrets for an {@link IndexReader}. */
  public static IndexPackageAccess getIndexPackageAccess() {
    ensureCaller();
    return Objects.requireNonNull(indexPackageAccess);
  }

  /** Return the accessor to internal secrets for an {@link ConcurrentMergeScheduler}. */
  public static ConcurrentMergeSchedulerAccess getConcurrentMergeSchedulerAccess() {
    ensureCaller();
    return Objects.requireNonNull(cmsAccess);
  }

  /** Return the accessor to internal secrets for an {@link SegmentReader}. */
  public static SegmentReaderAccess getSegmentReaderAccess() {
    ensureCaller();
    return Objects.requireNonNull(segmentReaderAccess);
  }

  /** Return the accessor to internal secrets for an {@link IndexWriter}. */
  public static IndexWriterAccess getIndexWriterAccess() {
    ensureCaller();
    return Objects.requireNonNull(indexWriterAccess);
  }

  /** Return the accessor to internal secrets for an {@link FilterIndexInput}. */
  public static FilterIndexInputAccess getFilterInputIndexAccess() {
    ensureCaller();
    return Objects.requireNonNull(filterIndexInputAccess);
  }

  /** For internal initialization only. */
  public static void setIndexWriterAccess(IndexWriterAccess indexWriterAccess) {
    ensureNull(TestSecrets.indexWriterAccess);
    TestSecrets.indexWriterAccess = indexWriterAccess;
  }

  /** For internal initialization only. */
  public static void setIndexPackageAccess(IndexPackageAccess indexPackageAccess) {
    ensureNull(TestSecrets.indexPackageAccess);
    TestSecrets.indexPackageAccess = indexPackageAccess;
  }

  /** For internal initialization only. */
  public static void setConcurrentMergeSchedulerAccess(ConcurrentMergeSchedulerAccess cmsAccess) {
    ensureNull(TestSecrets.cmsAccess);
    TestSecrets.cmsAccess = cmsAccess;
  }

  /** For internal initialization only. */
  public static void setSegmentReaderAccess(SegmentReaderAccess segmentReaderAccess) {
    ensureNull(TestSecrets.segmentReaderAccess);
    TestSecrets.segmentReaderAccess = segmentReaderAccess;
  }

  /** For internal initialization only. */
  public static void setFilterInputIndexAccess(FilterIndexInputAccess filterIndexInputAccess) {
    ensureNull(TestSecrets.filterIndexInputAccess);
    TestSecrets.filterIndexInputAccess = filterIndexInputAccess;
  }

  private static void ensureNull(Object ob) {
    if (ob != null) {
      throw new AssertionError(
          "The accessor is already set. It can only be called from inside Lucene Core.");
    }
  }

  private static void ensureCaller() {
    final boolean validCaller =
        StackWalker.getInstance()
            .walk(
                s ->
                    s.skip(2)
                        .limit(1)
                        .map(StackFrame::getClassName)
                        .allMatch(c -> c.startsWith("org.apache.lucene.tests.")));
    if (!validCaller) {
      throw new UnsupportedOperationException(
          "Lucene TestSecrets can only be used by the test-framework.");
    }
  }
}
