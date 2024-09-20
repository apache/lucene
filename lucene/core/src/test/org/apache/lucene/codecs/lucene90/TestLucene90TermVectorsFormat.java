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
package org.apache.lucene.codecs.lucene90;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.TermVectors;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.tests.codecs.compressing.dummy.DummyCompressingCodec;
import org.apache.lucene.tests.index.BaseTermVectorsFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;

public class TestLucene90TermVectorsFormat extends BaseTermVectorsFormatTestCase {
  @Override
  protected Codec getCodec() {
    return TestUtil.getDefaultCodec();
  }

  private static class CountingPrefetchDirectory extends FilterDirectory {

    private final AtomicInteger counter;

    CountingPrefetchDirectory(Directory in, AtomicInteger counter) {
      super(in);
      this.counter = counter;
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
      return new CountingPrefetchIndexInput(super.openInput(name, context), counter);
    }
  }

  private static class CountingPrefetchIndexInput extends FilterIndexInput {

    private final AtomicInteger counter;

    public CountingPrefetchIndexInput(IndexInput input, AtomicInteger counter) {
      super(input.toString(), input);
      this.counter = counter;
    }

    @Override
    public void prefetch(long offset, long length) throws IOException {
      in.prefetch(offset, length);
      counter.incrementAndGet();
    }

    @Override
    public IndexInput clone() {
      return new CountingPrefetchIndexInput(in.clone(), counter);
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
      return new CountingPrefetchIndexInput(in.slice(sliceDescription, offset, length), counter);
    }
  }

  public void testSkipRedundantPrefetches() throws IOException {
    // Use the "dummy" codec, which has the same base class as Lucene90StoredFieldsFormat but allows
    // configuring the number of docs per chunk.
    Codec codec = new DummyCompressingCodec(1 << 10, 2, false, 16);
    try (Directory origDir = newDirectory()) {
      AtomicInteger counter = new AtomicInteger();
      Directory dir = new CountingPrefetchDirectory(origDir, counter);
      try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig().setCodec(codec))) {
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.setStoreTermVectors(true);
        for (int i = 0; i < 100; ++i) {
          Document doc = new Document();
          doc.add(new Field("content", Integer.toString(i), ft));
          w.addDocument(doc);
        }
        w.forceMerge(1);
      }

      try (IndexReader reader = DirectoryReader.open(dir)) {
        TermVectors termVectors = reader.termVectors();
        counter.set(0);
        assertEquals(0, counter.get());
        termVectors.prefetch(0);
        assertEquals(1, counter.get());
        termVectors.prefetch(1);
        // This format has 2 docs per block, so the second prefetch is skipped
        assertEquals(1, counter.get());
        termVectors.prefetch(15);
        assertEquals(2, counter.get());
        termVectors.prefetch(14);
        // 14 is in the same block as 15, so the prefetch was skipped
        assertEquals(2, counter.get());
        // Already prefetched in the past, so skipped again
        termVectors.prefetch(1);
        assertEquals(2, counter.get());
      }
    }
  }
}
