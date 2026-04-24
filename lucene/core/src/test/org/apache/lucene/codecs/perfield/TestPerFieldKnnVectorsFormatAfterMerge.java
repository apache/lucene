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
package org.apache.lucene.codecs.perfield;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

/** Tests for the afterMerge() lifecycle hook on PerFieldKnnVectorsFormat. */
public class TestPerFieldKnnVectorsFormatAfterMerge extends LuceneTestCase {

  /** Writes numSegments single-doc segments with a vector field, using NoMergePolicy. */
  private void writeSegments(Directory dir, KnnVectorsFormat format, int numSegments)
      throws IOException {
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(codecWithFormat(format));
    iwc.setMergePolicy(NoMergePolicy.INSTANCE);
    try (IndexWriter iw = new IndexWriter(dir, iwc)) {
      for (int i = 0; i < numSegments; i++) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("field", new float[] {i, i + 1, i + 2}));
        iw.addDocument(doc);
        iw.commit();
      }
    }
  }

  private static FilterCodec codecWithFormat(KnnVectorsFormat format) {
    Codec defaultCodec = TestUtil.getDefaultCodec();
    return new FilterCodec(defaultCodec.getName(), defaultCodec) {
      @Override
      public KnnVectorsFormat knnVectorsFormat() {
        return format;
      }
    };
  }

  /** afterMerge() on the format must be called exactly once when a merge completes. */
  public void testAfterMergeCalledOnMerge() throws IOException {
    AtomicInteger afterMergeCount = new AtomicInteger();

    PerFieldKnnVectorsFormat format =
        new PerFieldKnnVectorsFormat() {
          @Override
          public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
            return TestUtil.getDefaultKnnVectorsFormat();
          }

          @Override
          protected void afterMerge() {
            afterMergeCount.incrementAndGet();
          }
        };

    try (Directory dir = newDirectory()) {
      writeSegments(dir, format, 3);
      assertEquals("afterMerge should not be called during flush", 0, afterMergeCount.get());

      // Force merge triggers afterMerge
      IndexWriterConfig mergeConfig = new IndexWriterConfig(new MockAnalyzer(random()));
      mergeConfig.setCodec(codecWithFormat(format));
      try (IndexWriter iw = new IndexWriter(dir, mergeConfig)) {
        iw.forceMerge(1);
      }

      assertEquals("afterMerge should be called exactly once per merge", 1, afterMergeCount.get());
    }
  }

  /** afterMerge() must not be called during a normal flush (no merge). */
  public void testAfterMergeNotCalledOnFlush() throws IOException {
    AtomicInteger afterMergeCount = new AtomicInteger();

    PerFieldKnnVectorsFormat format =
        new PerFieldKnnVectorsFormat() {
          @Override
          public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
            return TestUtil.getDefaultKnnVectorsFormat();
          }

          @Override
          protected void afterMerge() {
            afterMergeCount.incrementAndGet();
          }
        };

    try (Directory dir = newDirectory()) {
      writeSegments(dir, format, 3);
      assertEquals("afterMerge must not be called on flush-only writes", 0, afterMergeCount.get());
    }
  }

  /**
   * afterMerge() is called in a finally block, so it must fire even when mergeOneField throws. We
   * verify this by using a format that wraps the delegate writer to throw during merge, then
   * checking that afterMerge() was still invoked.
   */
  public void testAfterMergeCalledEvenOnException() throws IOException {
    AtomicInteger afterMergeCount = new AtomicInteger();

    PerFieldKnnVectorsFormat format =
        new PerFieldKnnVectorsFormat() {
          @Override
          public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
            return new ThrowingOnMergeKnnVectorsFormat(TestUtil.getDefaultKnnVectorsFormat());
          }

          @Override
          protected void afterMerge() {
            afterMergeCount.incrementAndGet();
          }
        };

    try (Directory dir = newDirectory()) {
      // Write segments using a normal format so data is valid on disk
      PerFieldKnnVectorsFormat normalFormat =
          new PerFieldKnnVectorsFormat() {
            @Override
            public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
              return TestUtil.getDefaultKnnVectorsFormat();
            }
          };
      writeSegments(dir, normalFormat, 3);

      // Use SerialMergeScheduler so the merge runs on the calling thread and the
      // exception propagates directly (instead of being caught by ConcurrentMergeScheduler)
      IndexWriterConfig mergeConfig = new IndexWriterConfig(new MockAnalyzer(random()));
      mergeConfig.setCodec(codecWithFormat(format));
      mergeConfig.setMergeScheduler(new org.apache.lucene.index.SerialMergeScheduler());
      IndexWriter iw = new IndexWriter(dir, mergeConfig);
      try {
        expectThrows(IOException.class, () -> iw.forceMerge(1));
      } finally {
        // IndexWriter may be in a tragic state after the merge failure, so rollback
        try {
          iw.rollback();
        } catch (
            @SuppressWarnings("unused")
            Exception ignored) {
          // expected — writer may already be closed or in a tragic state
        }
      }

      assertEquals(
          "afterMerge must be called even when mergeOneField throws", 1, afterMergeCount.get());
    }
  }

  /**
   * A KnnVectorsFormat wrapper that delegates everything normally except mergeOneField, which always
   * throws IOException.
   */
  private static class ThrowingOnMergeKnnVectorsFormat extends KnnVectorsFormat {
    private final KnnVectorsFormat delegate;

    ThrowingOnMergeKnnVectorsFormat(KnnVectorsFormat delegate) {
      super(delegate.getName());
      this.delegate = delegate;
    }

    @Override
    public org.apache.lucene.codecs.KnnVectorsWriter fieldsWriter(
        org.apache.lucene.index.SegmentWriteState state) throws IOException {
      org.apache.lucene.codecs.KnnVectorsWriter delegateWriter = delegate.fieldsWriter(state);
      return new org.apache.lucene.codecs.KnnVectorsWriter() {
        @Override
        public org.apache.lucene.codecs.KnnFieldVectorsWriter<?> addField(
            org.apache.lucene.index.FieldInfo fieldInfo) throws IOException {
          return delegateWriter.addField(fieldInfo);
        }

        @Override
        public void flush(int maxDoc, org.apache.lucene.index.Sorter.DocMap sortMap)
            throws IOException {
          delegateWriter.flush(maxDoc, sortMap);
        }

        @Override
        public void mergeOneField(
            org.apache.lucene.index.FieldInfo fieldInfo,
            org.apache.lucene.index.MergeState mergeState)
            throws IOException {
          throw new IOException("simulated merge failure");
        }

        @Override
        public void finish() throws IOException {
          delegateWriter.finish();
        }

        @Override
        public void close() throws IOException {
          delegateWriter.close();
        }

        @Override
        public long ramBytesUsed() {
          return delegateWriter.ramBytesUsed();
        }
      };
    }

    @Override
    public org.apache.lucene.codecs.KnnVectorsReader fieldsReader(
        org.apache.lucene.index.SegmentReadState state) throws IOException {
      return delegate.fieldsReader(state);
    }

    @Override
    public int getMaxDimensions(String fieldName) {
      return delegate.getMaxDimensions(fieldName);
    }
  }
}
