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
package org.apache.lucene.tests.codecs.asserting;

import com.carrotsearch.randomizedtesting.RandomizedContext;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.stream.Collectors;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

/** Acts like the default codec but with additional asserts. */
public class AssertingCodec extends FilterCodec {

  /** Enum representing asserting format types that can be suppressed. */
  public enum Format {
    STORED_FIELDS,
    TERM_VECTORS,
    NORMS,
    LIVE_DOCS,
    POINTS,
    KNN_VECTORS
  }

  private final EnumSet<Format> suppressedFormats;

  private boolean isSuppressed(Format format) {
    return suppressedFormats.contains(format);
  }

  static void assertThread(String object, Thread creationThread) {
    if (creationThread != Thread.currentThread()) {
      throw new AssertionError(
          object
              + " are only supposed to be consumed in "
              + "the thread in which they have been acquired. But was acquired in "
              + creationThread
              + " and consumed in "
              + Thread.currentThread()
              + ".");
    }
  }

  private final PostingsFormat postings =
      new PerFieldPostingsFormat() {
        @Override
        public PostingsFormat getPostingsFormatForField(String field) {
          return AssertingCodec.this.getPostingsFormatForField(field);
        }
      };

  private final DocValuesFormat docValues =
      new PerFieldDocValuesFormat() {
        @Override
        public DocValuesFormat getDocValuesFormatForField(String field) {
          return AssertingCodec.this.getDocValuesFormatForField(field);
        }
      };

  private final KnnVectorsFormat knnVectorsFormat =
      new PerFieldKnnVectorsFormat() {
        @Override
        public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
          return AssertingCodec.this.getKnnVectorsFormatForField(field);
        }
      };

  private final TermVectorsFormat vectors = new AssertingTermVectorsFormat();
  private final StoredFieldsFormat storedFields = new AssertingStoredFieldsFormat();
  private final NormsFormat norms = new AssertingNormsFormat();
  private final LiveDocsFormat liveDocs = new AssertingLiveDocsFormat();
  private final PostingsFormat defaultFormat = new AssertingPostingsFormat();
  private final DocValuesFormat defaultDVFormat = new AssertingDocValuesFormat();
  private final PointsFormat pointsFormat = new AssertingPointsFormat();
  private final KnnVectorsFormat defaultKnnVectorsFormat = new AssertingKnnVectorsFormat();

  public AssertingCodec() {
    super("Asserting", TestUtil.getDefaultCodec());

    var suppressedFormats = EnumSet.noneOf(AssertingCodec.Format.class);
    Class<?> targetClass;
    try {
      targetClass = RandomizedContext.current().getTargetClass();
    } catch (IllegalStateException _) {
      // Not under any randomized context.
      targetClass = null;
    }

    if (targetClass != null
        && targetClass.isAnnotationPresent(LuceneTestCase.SuppressAssertingFormats.class)) {
      suppressedFormats.addAll(
          Arrays.asList(
              targetClass.getAnnotation(LuceneTestCase.SuppressAssertingFormats.class).value()));
    }

    this.suppressedFormats = suppressedFormats;
  }

  @Override
  public PostingsFormat postingsFormat() {
    return postings;
  }

  @Override
  public TermVectorsFormat termVectorsFormat() {
    return isSuppressed(Format.TERM_VECTORS) ? delegate.termVectorsFormat() : vectors;
  }

  @Override
  public StoredFieldsFormat storedFieldsFormat() {
    return isSuppressed(Format.STORED_FIELDS) ? delegate.storedFieldsFormat() : storedFields;
  }

  @Override
  public DocValuesFormat docValuesFormat() {
    return docValues;
  }

  @Override
  public NormsFormat normsFormat() {
    return isSuppressed(Format.NORMS) ? delegate.normsFormat() : norms;
  }

  @Override
  public LiveDocsFormat liveDocsFormat() {
    return isSuppressed(Format.LIVE_DOCS) ? delegate.liveDocsFormat() : liveDocs;
  }

  @Override
  public PointsFormat pointsFormat() {
    return isSuppressed(Format.POINTS) ? delegate.pointsFormat() : pointsFormat;
  }

  @Override
  public KnnVectorsFormat knnVectorsFormat() {
    return isSuppressed(Format.KNN_VECTORS) ? delegate.knnVectorsFormat() : knnVectorsFormat;
  }

  @Override
  public String toString() {
    return "Asserting("
        + delegate
        + (suppressedFormats.isEmpty()
            ? ""
            : ", suppressed: "
                + suppressedFormats.stream().map(Enum::toString).collect(Collectors.joining(", ")))
        + ")";
  }

  /**
   * Returns the postings format that should be used for writing new segments of <code>field</code>.
   *
   * <p>The default implementation always returns "Asserting"
   */
  public PostingsFormat getPostingsFormatForField(String field) {
    return defaultFormat;
  }

  /**
   * Returns the docvalues format that should be used for writing new segments of <code>field</code>
   * .
   *
   * <p>The default implementation always returns "Asserting"
   */
  public DocValuesFormat getDocValuesFormatForField(String field) {
    return defaultDVFormat;
  }

  /**
   * Returns the vectors format that should be used for writing new segments of <code>field</code>.
   *
   * <p>The default implementation always returns "Asserting"
   */
  public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
    return defaultKnnVectorsFormat;
  }
}
