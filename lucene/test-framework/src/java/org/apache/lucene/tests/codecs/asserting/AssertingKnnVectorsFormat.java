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

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.HnswGraphProvider;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.tests.search.AssertingAcceptDocs;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.hnsw.HnswGraph;

/** Wraps the default KnnVectorsFormat and provides additional assertions. */
public class AssertingKnnVectorsFormat extends KnnVectorsFormat {

  private final KnnVectorsFormat delegate = TestUtil.getDefaultKnnVectorsFormat();

  public AssertingKnnVectorsFormat() {
    super("Asserting");
  }

  @Override
  public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
    return new AssertingKnnVectorsWriter(delegate.fieldsWriter(state));
  }

  @Override
  public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
    return new AssertingKnnVectorsReader(delegate.fieldsReader(state), state.fieldInfos);
  }

  @Override
  public int getMaxDimensions(String fieldName) {
    return KnnVectorsFormat.DEFAULT_MAX_DIMENSIONS;
  }

  @Override
  public String toString() {
    return "AssertingKnnVectorsFormat{" + "delegate=" + delegate + '}';
  }

  static class AssertingKnnVectorsWriter extends KnnVectorsWriter {
    final KnnVectorsWriter delegate;

    AssertingKnnVectorsWriter(KnnVectorsWriter delegate) {
      assert delegate != null;
      this.delegate = delegate;
    }

    @Override
    public KnnFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
      return delegate.addField(fieldInfo);
    }

    @Override
    public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
      delegate.flush(maxDoc, sortMap);
    }

    @Override
    public void mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
      assert fieldInfo != null;
      assert mergeState != null;
      delegate.mergeOneField(fieldInfo, mergeState);
    }

    @Override
    public void finish() throws IOException {
      delegate.finish();
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }

    @Override
    public long ramBytesUsed() {
      return delegate.ramBytesUsed();
    }
  }

  /** Wraps a AssertingKnnVectorsReader providing additional assertions. */
  public static class AssertingKnnVectorsReader extends KnnVectorsReader
      implements HnswGraphProvider {
    public final KnnVectorsReader delegate;
    private final FieldInfos fis;
    private final AtomicInteger mergeInstanceCount = new AtomicInteger();
    private final AtomicInteger finishMergeCount = new AtomicInteger();

    private AssertingKnnVectorsReader(KnnVectorsReader delegate, FieldInfos fis) {
      assert delegate != null;
      this.delegate = delegate;
      this.fis = fis;
    }

    @Override
    public void checkIntegrity() throws IOException {
      delegate.checkIntegrity();
    }

    @Override
    public FloatVectorValues getFloatVectorValues(String field) throws IOException {
      FieldInfo fi = fis.fieldInfo(field);
      assert fi != null
          && fi.getVectorDimension() > 0
          && fi.getVectorEncoding() == VectorEncoding.FLOAT32;
      FloatVectorValues floatValues = delegate.getFloatVectorValues(field);
      assert floatValues != null;
      assert floatValues.iterator().docID() == -1;
      assert floatValues.size() >= 0;
      assert floatValues.dimension() > 0;
      return floatValues;
    }

    @Override
    public ByteVectorValues getByteVectorValues(String field) throws IOException {
      FieldInfo fi = fis.fieldInfo(field);
      assert fi != null
          && fi.getVectorDimension() > 0
          && fi.getVectorEncoding() == VectorEncoding.BYTE;
      ByteVectorValues values = delegate.getByteVectorValues(field);
      assert values != null;
      assert values.iterator().docID() == -1;
      assert values.size() >= 0;
      assert values.dimension() > 0;
      return values;
    }

    @Override
    public void search(
        String field, float[] target, KnnCollector knnCollector, AcceptDocs acceptDocs)
        throws IOException {
      FieldInfo fi = fis.fieldInfo(field);
      assert fi != null
          && fi.getVectorDimension() > 0
          && fi.getVectorEncoding() == VectorEncoding.FLOAT32;
      acceptDocs = AssertingAcceptDocs.wrap(acceptDocs);
      delegate.search(field, target, knnCollector, acceptDocs);
    }

    @Override
    public void search(
        String field, byte[] target, KnnCollector knnCollector, AcceptDocs acceptDocs)
        throws IOException {
      FieldInfo fi = fis.fieldInfo(field);
      assert fi != null
          && fi.getVectorDimension() > 0
          && fi.getVectorEncoding() == VectorEncoding.BYTE;
      acceptDocs = AssertingAcceptDocs.wrap(acceptDocs);
      delegate.search(field, target, knnCollector, acceptDocs);
    }

    @Override
    public KnnVectorsReader getMergeInstance() throws IOException {
      var mergeVectorsReader = delegate.getMergeInstance();
      assert mergeVectorsReader != null;
      mergeInstanceCount.incrementAndGet();
      AtomicInteger parentMergeFinishCount = this.finishMergeCount;

      return new AssertingKnnVectorsReader(mergeVectorsReader, AssertingKnnVectorsReader.this.fis) {
        private boolean finished;

        @Override
        public void search(
            String field, float[] target, KnnCollector knnCollector, AcceptDocs acceptDocs) {
          assert false : "This instance should only be used for merging";
        }

        @Override
        public void search(
            String field, byte[] target, KnnCollector knnCollector, AcceptDocs acceptDocs) {
          assert false : "This instance should only be used for merging";
        }

        @Override
        public KnnVectorsReader getMergeInstance() {
          assert false; // merging from a merge instance it not allowed
          return null;
        }

        @Override
        public void finishMerge() throws IOException {
          assert !finished : "Merging already finished";
          finished = true;
          delegate.finishMerge();
          parentMergeFinishCount.incrementAndGet();
        }

        @Override
        public void close() {
          assert false; // closing the merge instance it not allowed
        }
      };
    }

    @Override
    public void finishMerge() throws IOException {
      assert false; // can only finish merge on the merge instance
    }

    @Override
    public Map<String, Long> getOffHeapByteSize(FieldInfo fieldInfo) {
      return delegate.getOffHeapByteSize(fieldInfo);
    }

    @Override
    public void close() throws IOException {
      delegate.close();
      delegate.close(); // impls should be able to handle multiple closes
      assert mergeInstanceCount.get() == finishMergeCount.get();
    }

    @Override
    public HnswGraph getGraph(String field) throws IOException {
      if (delegate instanceof HnswGraphProvider) {
        return ((HnswGraphProvider) delegate).getGraph(field);
      } else {
        return null;
      }
    }
  }
}
