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

package org.apache.lucene.jmh.base.luceneutil.perf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.misc.index.IndexRearranger;
import org.apache.lucene.store.Directory;

/** The type Bench rearranger. */
public class BenchRearranger {

  private static final String ID_FIELD = "id";

  /** Instantiates a new Bench rearranger. */
  private BenchRearranger() {
    /* TODO document why this constructor is empty */
  }

  /**
   * Perform rearrange based on given arrangement * rearrange /100 gives how many large segments
   * desired * (rearrange % 100) / 10 gives how many medium segments desired * rearrange % 10 gives
   * how many small segments desired where large segments has roughly 10 times documents as medium
   * segments, medium has 10 times documents as small segments documents distribution is determined
   * by {@link BenchRearranger#ID_FIELD}
   *
   * @param inputDir input directory, will not be modified
   * @param outputDir output directory, will create new index under this dir
   * @param iwc IndexWriterConfig, will be configured with {@link NoMergePolicy} when rearrange
   * @param arrangement an integer, will be parsed as explained above
   * @throws Exception when rearrange
   */
  public static void rearrange(
      Directory inputDir, Directory outputDir, IndexWriterConfig iwc, int arrangement)
      throws Exception {
    HashMap<String, String> commitData = new HashMap<>();
    if (arrangement > 1) {
      commitData.put("userData", "multi");
    } else {
      commitData.put("userData", "single");
    }
    try (IndexReader reader = DirectoryReader.open(inputDir)) {
      int large = arrangement / 100;
      int medium = (arrangement % 100) / 10;
      int small = arrangement % 10;
      IndexRearranger rearranger =
          new IndexRearranger(
              inputDir, outputDir, iwc, getDocumentSelectors(reader, large, medium, small));
      rearranger.execute();
    }
    IndexWriterConfig config = new IndexWriterConfig(null);
    config.setOpenMode(IndexWriterConfig.OpenMode.APPEND);
    config.setMergePolicy(NoMergePolicy.INSTANCE);
    // TODO: expose a callable argument to IndexRearranger to set live commit data
    try (IndexWriter writer = new IndexWriter(outputDir, config)) {
      writer.setLiveCommitData(commitData.entrySet());
      writer.commit();
    }
  }

  private static List<IndexRearranger.DocumentSelector> getDocumentSelectors(
      IndexReader reader, int large, int medium, int small) {
    int numAllDocs = 0;
    int totalChunks = large * 100 + medium * 10 + small;
    for (LeafReaderContext context : reader.leaves()) {
      numAllDocs += context.reader().numDocs();
    }
    int docPerChunk = numAllDocs / totalChunks;
    int docResidual = numAllDocs % totalChunks;
    int upto = 0;
    List<IndexRearranger.DocumentSelector> selectors = new ArrayList<>();
    for (int i = 0; i < large; i++) {
      int nextSeg = upto + 100 * docPerChunk + Math.min(100, docResidual);
      selectors.add(new StringFieldDocSelector(ID_FIELD, getIdSetOfRange(upto, nextSeg)));
      docResidual = Math.max(0, docResidual - 100);
      upto = nextSeg;
    }

    for (int i = 0; i < medium; i++) {
      int nextSeg = upto + 10 * docPerChunk + Math.min(10, docResidual);
      selectors.add(new StringFieldDocSelector(ID_FIELD, getIdSetOfRange(upto, nextSeg)));
      docResidual = Math.max(0, docResidual - 10);
      upto = nextSeg;
    }

    for (int i = 0; i < small; i++) {
      int nextSeg = upto + docPerChunk + Math.min(1, docResidual);
      selectors.add(new StringFieldDocSelector(ID_FIELD, getIdSetOfRange(upto, nextSeg)));
      docResidual = Math.max(0, docResidual - 1);
      upto = nextSeg;
    }
    assert docResidual == 0;
    assert upto == numAllDocs;
    return selectors;
  }

  private static HashSet<String> getIdSetOfRange(int from, int to) {
    HashSet<String> rtn = new HashSet<>();
    for (int i = from; i < to; i++) {
      rtn.add(LineFileDocs.intToID(i));
    }
    return rtn;
  }
}
