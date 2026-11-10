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
package org.apache.lucene.index;

import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;

/**
 * Tests {@link org.apache.lucene.codecs.KnnVectorsReader#getVectorCount} on codec reader wrappers.
 */
public class TestGetVectorCountCodecWrappers extends LuceneTestCase {

  public void testSlowCompositeCodecReaderWrapperGetVectorCount() throws Exception {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        Document doc = new Document();
        doc.add(
            new KnnFloatVectorField(
                "f", new float[] {1, 0, 0, 0}, VectorSimilarityFunction.DOT_PRODUCT));
        w.addDocument(doc);
        w.commit();
        doc = new Document();
        doc.add(
            new KnnFloatVectorField(
                "f", new float[] {0, 1, 0, 0}, VectorSimilarityFunction.DOT_PRODUCT));
        w.addDocument(doc);
        w.commit();
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals(2, reader.leaves().size());
        List<CodecReader> codecReaders = new ArrayList<>(2);
        int expected = 0;
        for (LeafReaderContext ctx : reader.leaves()) {
          codecReaders.add((CodecReader) ctx.reader());
          FloatVectorValues values = ctx.reader().getFloatVectorValues("f");
          expected += values.size();
        }
        CodecReader composite = SlowCompositeCodecReaderWrapper.wrap(codecReaders);
        FieldInfo fieldInfo = composite.getFieldInfos().fieldInfo("f");
        assertEquals(expected, composite.getVectorReader().getVectorCount(fieldInfo));
      }
    }
  }
}
