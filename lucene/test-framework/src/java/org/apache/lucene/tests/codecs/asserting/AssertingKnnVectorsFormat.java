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
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.Bits;

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

  static class AssertingKnnVectorsReader extends KnnVectorsReader {
    final KnnVectorsReader delegate;
    final FieldInfos fis;

    AssertingKnnVectorsReader(KnnVectorsReader delegate, FieldInfos fis) {
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
      assert floatValues.docID() == -1;
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
      assert values.docID() == -1;
      assert values.size() >= 0;
      assert values.dimension() > 0;
      return values;
    }

    @Override
    public TopDocs search(String field, float[] target, int k, Bits acceptDocs, int visitedLimit)
        throws IOException {
      FieldInfo fi = fis.fieldInfo(field);
      assert fi != null
          && fi.getVectorDimension() > 0
          && fi.getVectorEncoding() == VectorEncoding.FLOAT32;
      TopDocs hits = delegate.search(field, target, k, acceptDocs, visitedLimit);
      assert hits != null;
      assert hits.scoreDocs.length <= k;
      return hits;
    }

    @Override
    public TopDocs search(String field, byte[] target, int k, Bits acceptDocs, int visitedLimit)
        throws IOException {
      FieldInfo fi = fis.fieldInfo(field);
      assert fi != null
          && fi.getVectorDimension() > 0
          && fi.getVectorEncoding() == VectorEncoding.BYTE;
      TopDocs hits = delegate.search(field, target, k, acceptDocs, visitedLimit);
      assert hits != null;
      assert hits.scoreDocs.length <= k;
      return hits;
    }

    @Override
    public void close() throws IOException {
      delegate.close();
      delegate.close();
    }

    @Override
    public long ramBytesUsed() {
      return delegate.ramBytesUsed();
    }
  }
}
