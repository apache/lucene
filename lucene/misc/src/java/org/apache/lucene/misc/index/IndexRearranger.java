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

package org.apache.lucene.misc.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterCodecReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.NamedThreadFactory;

/**
 * Copy and rearrange index according to document selectors, from input dir to output dir. Length of
 * documentSelectors determines how many segments there will be.
 *
 * <p>Rearranging works in 3 steps: 1. Assume all docs in the original index are live and create the
 * rearranged index using the segment selectors. 2. Go through the rearranged index and apply
 * deletes requested by the deletes selector. 3. Reorder the segments to match the order of the
 * selectors and check the validity of the rearranged index.
 *
 * <p>NB: You can't produce segments that only contain deletes. If you select all documents in a
 * segment for deletion, the entire segment will be discarded.
 *
 * <p>Example use case: You are testing search performance after a change to indexing. You can index
 * the same content using the old and new indexers and then rearrange one of them to the shape of
 * the other. Using rearrange will give more accurate measurements, since you will not be
 * introducing noise from index geometry.
 *
 * <p>TODO: another possible (faster) approach to do this is to manipulate FlushPolicy and
 * MergePolicy at indexing time to create small desired segments first and merge them accordingly
 * for details please see: https://markmail.org/message/lbtdntclpnocmfuf
 *
 * @lucene.experimental
 */
public class IndexRearranger {
  protected final Directory input, output;
  protected final IndexWriterConfig config;

  // Each of these selectors will produce a segment in the rearranged index.
  // The segments will appear in the index in the order of the selectors that produced them.
  protected final List<DocumentSelector> segmentSelectors;

  // Documents selected here will be marked for deletion in the rearranged index, but not merged
  // away.
  protected final DocumentSelector deletedDocsSelector;

  /**
   * All args constructor
   *
   * @param input input dir
   * @param output output dir
   * @param config index writer config
   * @param segmentSelectors specify which documents are desired in the rearranged index segments;
   *     each selector corresponds to one segment
   * @param deletedDocsSelector specify which documents are to be marked for deletion in the
   *     rearranged index; this selector should be thread-safe
   */
  public IndexRearranger(
      Directory input,
      Directory output,
      IndexWriterConfig config,
      List<DocumentSelector> segmentSelectors,
      DocumentSelector deletedDocsSelector) {
    this.input = input;
    this.output = output;
    this.config = config;
    this.segmentSelectors = segmentSelectors;
    this.deletedDocsSelector = deletedDocsSelector;
  }

  /** Constructor with no deletes to apply */
  public IndexRearranger(
      Directory input,
      Directory output,
      IndexWriterConfig config,
      List<DocumentSelector> segmentSelectors) {
    this(input, output, config, segmentSelectors, null);
  }

  public void execute() throws Exception {
    ExecutorService executor =
        Executors.newFixedThreadPool(
            Math.min(Runtime.getRuntime().availableProcessors(), segmentSelectors.size()),
            new NamedThreadFactory("rearranger"));

    IndexWriterConfig createSegmentsConfig = new IndexWriterConfig(config.getAnalyzer());
    IndexWriterConfig applyDeletesConfig = new IndexWriterConfig(config.getAnalyzer());

    // Do not merge - each addIndexes call creates one segment
    createSegmentsConfig.setMergePolicy(NoMergePolicy.INSTANCE);
    applyDeletesConfig.setMergePolicy(NoMergePolicy.INSTANCE);

    try (IndexWriter writer = new IndexWriter(output, createSegmentsConfig);
        IndexReader reader = DirectoryReader.open(input)) {
      createRearrangedIndex(writer, reader, segmentSelectors, executor);
    }
    finalizeRearrange(output, segmentSelectors);

    try (IndexWriter writer = new IndexWriter(output, applyDeletesConfig);
        IndexReader reader = DirectoryReader.open(writer)) {
      applyDeletes(writer, reader, deletedDocsSelector, executor);
    }
    executor.shutdown();
  }

  /**
   * Place segments in the order of their respective selectors and ensure the rearrange was
   * performed correctly.
   */
  private static void finalizeRearrange(Directory output, List<DocumentSelector> segmentSelectors)
      throws IOException {
    List<SegmentCommitInfo> ordered = new ArrayList<>();
    try (IndexReader reader = DirectoryReader.open(output)) {
      for (DocumentSelector ds : segmentSelectors) {
        int foundLeaf = -1;
        for (LeafReaderContext context : reader.leaves()) {
          SegmentReader sr = (SegmentReader) context.reader();
          int docFound = ds.getFilteredDocs(sr).nextSetBit(0);
          if (docFound != DocIdSetIterator.NO_MORE_DOCS) {
            // Each document can be mapped to one segment at most
            if (foundLeaf != -1) {
              throw new IllegalStateException(
                  "Document selector "
                      + ds
                      + " has matched more than 1 segments. Matched segments order: "
                      + foundLeaf
                      + ", "
                      + context.ord);
            }
            foundLeaf = context.ord;
            ordered.add(sr.getSegmentInfo());
          }
        }
        assert foundLeaf != -1;
      }
    }
    SegmentInfos sis = SegmentInfos.readLatestCommit(output);
    sis.clear();
    sis.addAll(ordered);
    sis.commit(output);
  }

  /**
   * Create the rearranged index as described by the segment selectors. Assume all documents in the
   * original index are live.
   */
  private static void createRearrangedIndex(
      IndexWriter writer,
      IndexReader reader,
      List<DocumentSelector> selectors,
      ExecutorService executor)
      throws ExecutionException, InterruptedException {

    ArrayList<Future<Void>> futures = new ArrayList<>();
    for (DocumentSelector selector : selectors) {
      Callable<Void> addSegment =
          () -> {
            addOneSegment(writer, reader, selector);
            return null;
          };
      futures.add(executor.submit(addSegment));
    }

    for (Future<Void> future : futures) {
      future.get();
    }
  }

  private static void addOneSegment(
      IndexWriter writer, IndexReader reader, DocumentSelector selector) throws IOException {
    CodecReader[] readers = new CodecReader[reader.leaves().size()];
    for (LeafReaderContext context : reader.leaves()) {
      readers[context.ord] =
          new DocSelectorFilteredCodecReader((CodecReader) context.reader(), selector);
    }
    writer.addIndexes(readers);
  }

  private static void applyDeletes(
      IndexWriter writer, IndexReader reader, DocumentSelector selector, ExecutorService executor)
      throws ExecutionException, InterruptedException {
    if (selector == null) {
      // There are no deletes to be applied
      return;
    }

    ArrayList<Future<Void>> futures = new ArrayList<>();
    for (LeafReaderContext context : reader.leaves()) {
      Callable<Void> applyDeletesToSegment =
          () -> {
            applyDeletesToOneSegment(writer, (CodecReader) context.reader(), selector);
            return null;
          };
      futures.add(executor.submit(applyDeletesToSegment));
    }

    for (Future<Void> future : futures) {
      future.get();
    }
  }

  private static void applyDeletesToOneSegment(
      IndexWriter writer, CodecReader segmentReader, DocumentSelector selector) throws IOException {
    Bits deletedDocs = selector.getFilteredDocs(segmentReader);
    for (int docid = 0; docid < segmentReader.maxDoc(); ++docid) {
      if (deletedDocs.get(docid)) {
        if (writer.tryDeleteDocument(segmentReader, docid) == -1) {
          throw new IllegalStateException(
              "tryDeleteDocument has failed. This should never happen, since merging is disabled.");
        }
      }
    }
  }

  private static class DocSelectorFilteredCodecReader extends FilterCodecReader {

    BitSet filteredLiveDocs;
    int numDocs;

    public DocSelectorFilteredCodecReader(CodecReader in, DocumentSelector selector)
        throws IOException {
      super(in);
      filteredLiveDocs = selector.getFilteredDocs(in);
      numDocs = filteredLiveDocs.cardinality();
    }

    @Override
    public int numDocs() {
      return numDocs;
    }

    @Override
    public Bits getLiveDocs() {
      return filteredLiveDocs;
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
      return in.getCoreCacheHelper();
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
      return null;
    }
  }

  /** Select document within a CodecReader */
  public interface DocumentSelector {
    BitSet getFilteredDocs(CodecReader reader) throws IOException;
  }
}
