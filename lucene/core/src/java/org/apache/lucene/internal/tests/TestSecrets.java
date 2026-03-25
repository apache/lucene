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

import java.lang.StackWalker.Option;
import java.lang.StackWalker.StackFrame;
import java.lang.invoke.MethodHandles;
import java.util.Objects;
import java.util.Set;
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

  private static final MethodHandles.Lookup LOOKUP = MethodHandles.lookup();
  private static final StackWalker STACKWALKER =
      StackWalker.getInstance(Set.of(Option.DROP_METHOD_INFO), 3);

  private static void ensureInitialized(Class<?> clazz) {
    try {
      LOOKUP.ensureInitialized(clazz);
    } catch (IllegalAccessException e) {
      throw new AssertionError(e);
    }
  }

  @SuppressWarnings("NonFinalStaticField")
  private static IndexPackageAccess indexPackageAccess;

  @SuppressWarnings("NonFinalStaticField")
  private static ConcurrentMergeSchedulerAccess cmsAccess;

  @SuppressWarnings("NonFinalStaticField")
  private static SegmentReaderAccess segmentReaderAccess;

  @SuppressWarnings("NonFinalStaticField")
  private static IndexWriterAccess indexWriterAccess;

  @SuppressWarnings("NonFinalStaticField")
  private static FilterIndexInputAccess filterIndexInputAccess;

  private TestSecrets() {}

  /** Return the accessor to internal secrets for an {@link IndexReader}. */
  public static IndexPackageAccess getIndexPackageAccess() {
    ensureCallerForGetter();
    if (indexWriterAccess == null) {
      ensureInitialized(IndexWriter.class);
    }
    return Objects.requireNonNull(indexPackageAccess);
  }

  /** Return the accessor to internal secrets for an {@link ConcurrentMergeScheduler}. */
  public static ConcurrentMergeSchedulerAccess getConcurrentMergeSchedulerAccess() {
    ensureCallerForGetter();
    if (cmsAccess == null) {
      ensureInitialized(ConcurrentMergeScheduler.class);
    }
    return Objects.requireNonNull(cmsAccess);
  }

  /** Return the accessor to internal secrets for an {@link SegmentReader}. */
  public static SegmentReaderAccess getSegmentReaderAccess() {
    ensureCallerForGetter();
    if (segmentReaderAccess == null) {
      ensureInitialized(SegmentReader.class);
    }
    return Objects.requireNonNull(segmentReaderAccess);
  }

  /** Return the accessor to internal secrets for an {@link IndexWriter}. */
  public static IndexWriterAccess getIndexWriterAccess() {
    ensureCallerForGetter();
    if (indexWriterAccess == null) {
      ensureInitialized(IndexWriter.class);
    }
    return Objects.requireNonNull(indexWriterAccess);
  }

  /** Return the accessor to internal secrets for an {@link FilterIndexInput}. */
  public static FilterIndexInputAccess getFilterInputIndexAccess() {
    ensureCallerForGetter();
    if (filterIndexInputAccess == null) {
      ensureInitialized(FilterIndexInput.class);
    }
    return Objects.requireNonNull(filterIndexInputAccess);
  }

  /** For internal initialization only. */
  public static void setIndexWriterAccess(IndexWriterAccess indexWriterAccess) {
    ensureCallerForSetter(IndexWriter.class, TestSecrets.indexWriterAccess);
    TestSecrets.indexWriterAccess = indexWriterAccess;
  }

  /** For internal initialization only. */
  public static void setIndexPackageAccess(IndexPackageAccess indexPackageAccess) {
    ensureCallerForSetter(IndexWriter.class, TestSecrets.indexPackageAccess);
    TestSecrets.indexPackageAccess = indexPackageAccess;
  }

  /** For internal initialization only. */
  public static void setConcurrentMergeSchedulerAccess(ConcurrentMergeSchedulerAccess cmsAccess) {
    ensureCallerForSetter(ConcurrentMergeScheduler.class, TestSecrets.cmsAccess);
    TestSecrets.cmsAccess = cmsAccess;
  }

  /** For internal initialization only. */
  public static void setSegmentReaderAccess(SegmentReaderAccess segmentReaderAccess) {
    ensureCallerForSetter(SegmentReader.class, TestSecrets.segmentReaderAccess);
    TestSecrets.segmentReaderAccess = segmentReaderAccess;
  }

  /** For internal initialization only. */
  public static void setFilterInputIndexAccess(FilterIndexInputAccess filterIndexInputAccess) {
    ensureCallerForSetter(FilterIndexInput.class, TestSecrets.filterIndexInputAccess);
    TestSecrets.filterIndexInputAccess = filterIndexInputAccess;
  }

  private static void ensureCallerForSetter(Class<?> allowedCaller, Object needsNull) {
    final boolean validCaller =
        STACKWALKER.walk(
            s ->
                s.skip(2)
                    .limit(1)
                    .map(StackFrame::getClassName)
                        .allMatch(allowedCaller.getName()::equals));
    if (!validCaller || needsNull != null) {
      throw new IllegalCallerException(
          "The accessor can only be set once by " + allowedCaller.getName() + ".");
    }
  }

  private static void ensureCallerForGetter() {
    final boolean validCaller =
        STACKWALKER.walk(
            s ->
                s.skip(2)
                    .limit(1)
                    .map(StackFrame::getClassName)
                    .allMatch(
                        c ->
                            c.startsWith("org.apache.lucene.tests.")
                                || c.equals("org.apache.lucene.index.TestClassloadingDeadlock")));
    if (!validCaller) {
      throw new IllegalCallerException(
          "Lucene TestSecrets can only be used by the test-framework.");
    }
  }
}
