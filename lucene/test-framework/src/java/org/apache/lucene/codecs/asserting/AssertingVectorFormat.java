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

package org.apache.lucene.codecs.asserting;

import java.io.IOException;
import org.apache.lucene.codecs.VectorFormat;
import org.apache.lucene.codecs.VectorReader;
import org.apache.lucene.codecs.VectorWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.TestUtil;

/** Wraps the default VectorFormat and provides additional assertions. */
public class AssertingVectorFormat extends VectorFormat {

  private final VectorFormat delegate = TestUtil.getDefaultVectorFormat();

  public AssertingVectorFormat() {
    super("Asserting");
  }

  @Override
  public VectorWriter fieldsWriter(SegmentWriteState state) throws IOException {
    return new AssertingVectorWriter(delegate.fieldsWriter(state));
  }

  @Override
  public VectorReader fieldsReader(SegmentReadState state) throws IOException {
    return new AssertingVectorReader(delegate.fieldsReader(state));
  }

  static class AssertingVectorWriter extends VectorWriter {
    final VectorWriter delegate;

    AssertingVectorWriter(VectorWriter delegate) {
      assert delegate != null;
      this.delegate = delegate;
    }

    @Override
    public void writeField(FieldInfo fieldInfo, VectorValues values) throws IOException {
      assert fieldInfo != null;
      assert values != null;
      delegate.writeField(fieldInfo, values);
    }

    @Override
    public void finish() throws IOException {
      delegate.finish();
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }
  }

  static class AssertingVectorReader extends VectorReader {
    final VectorReader delegate;

    AssertingVectorReader(VectorReader delegate) {
      assert delegate != null;
      this.delegate = delegate;
    }

    @Override
    public void checkIntegrity() throws IOException {
      delegate.checkIntegrity();
    }

    @Override
    public VectorValues getVectorValues(String field) throws IOException {
      VectorValues values = delegate.getVectorValues(field);
      if (values != null) {
        assert values.docID() == -1;
        assert values.size() > 0;
        assert values.dimension() > 0;
        assert values.similarityFunction() != null;
      }
      return values;
    }

    @Override
    public TopDocs search(String field, float[] target, int k, int fanout) throws IOException {
      TopDocs hits = delegate.search(field, target, k, fanout);
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
