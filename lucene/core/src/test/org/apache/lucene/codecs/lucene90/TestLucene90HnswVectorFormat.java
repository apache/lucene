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

import java.util.List;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.VectorFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.VectorField;
import org.apache.lucene.index.BaseVectorFormatTestCase;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.hnsw.HnswGraphBuilder;

public class TestLucene90HnswVectorFormat extends BaseVectorFormatTestCase {

  @Override
  protected Codec getCodec() {
    return TestUtil.getDefaultCodec();
  }

  public void testCodecParameters() throws Exception {
    Directory dir = newDirectory();

    // Create a segment using the default parameters
    IndexWriterConfig iwc = newIndexWriterConfig();
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new VectorField("field", new float[4], VectorValues.SimilarityFunction.DOT_PRODUCT));
    w.addDocument(doc);
    w.close();

    // Write another segment using different parameters
    int newMaxConn = HnswGraphBuilder.DEFAULT_MAX_CONN + 3;
    int newBeamWidth = HnswGraphBuilder.DEFAULT_BEAM_WIDTH + 42;
    IndexWriterConfig iwc2 =
        newIndexWriterConfig()
            .setCodec(
                new Lucene90Codec() {
                  @Override
                  public VectorFormat getVectorFormatForField(String field) {
                    return new Lucene90HnswVectorFormat(newMaxConn, newBeamWidth);
                  }
                });
    IndexWriter w2 = new IndexWriter(dir, iwc2);
    Document doc2 = new Document();
    doc2.add(new VectorField("field", new float[4], VectorValues.SimilarityFunction.DOT_PRODUCT));
    w2.addDocument(doc2);
    w2.close();

    // Check that we record the parameters that were used in the segment infos
    List<SegmentCommitInfo> commitInfos = SegmentInfos.readLatestCommit(dir).asList();
    assertEquals(2, commitInfos.size());

    SegmentInfo si = commitInfos.get(0).info;
    assertEquals(
        String.valueOf(HnswGraphBuilder.DEFAULT_MAX_CONN),
        si.getAttribute(Lucene90HnswVectorFormat.MAX_CONN_KEY));
    assertEquals(
        String.valueOf(HnswGraphBuilder.DEFAULT_BEAM_WIDTH),
        si.getAttribute(Lucene90HnswVectorFormat.BEAM_WIDTH_KEY));

    SegmentInfo si2 = commitInfos.get(1).info;
    assertEquals(
        String.valueOf(newMaxConn), si2.getAttribute(Lucene90HnswVectorFormat.MAX_CONN_KEY));
    assertEquals(
        String.valueOf(newBeamWidth), si2.getAttribute(Lucene90HnswVectorFormat.BEAM_WIDTH_KEY));

    dir.close();
  }
}
