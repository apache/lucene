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
package org.apache.lucene.search.grouping;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.NamedThreadFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/** Base class for grouping related tests. */
// TODO (MvG) : The grouping tests contain a lot of code duplication. Try to move the common code to
// this class..
public abstract class AbstractGroupingTestCase extends LuceneTestCase {

  protected static ExecutorService testExecutor;

  @BeforeClass
  public static void setUpExecutor() {
    testExecutor = Executors.newFixedThreadPool(2, new NamedThreadFactory("GroupingTest"));
  }

  @AfterClass
  public static void tearDownExecutor() {
    TestUtil.shutdownExecutorService(testExecutor);
    testExecutor = null;
  }

  /**
   * Creates an IndexSearcher with random concurrency. For intra-segment concurrency, slices are
   * aligned on block boundaries using the provided lastDocPerGroup query so that no doc block is
   * ever split across slices.
   */
  protected IndexSearcher newIndexSearcher(IndexReader reader, Query lastDocPerGroupQuery)
      throws IOException {
    IndexSearcher tmp = new IndexSearcher(reader);
    Weight lastDocPerGroup =
        tmp.createWeight(tmp.rewrite(lastDocPerGroupQuery), ScoreMode.COMPLETE_NO_SCORES, 1);
    Concurrency concurrency =
        RandomPicks.randomFrom(
            random(),
            new Concurrency[] {
              Concurrency.NONE, Concurrency.INTER_SEGMENT, Concurrency.INTRA_SEGMENT
            });
    if (concurrency == Concurrency.NONE) {
      return new IndexSearcher(reader);
    }
    return new BlockGroupingIndexSearcher(
        reader.getContext(), testExecutor, lastDocPerGroup, concurrency);
  }

  /** An IndexSearcher that splits slices at block boundaries for safe concurrent block grouping. */
  protected static class BlockGroupingIndexSearcher extends IndexSearcher {
    private final Weight lastDocPerGroup;
    private final Concurrency concurrency;

    public BlockGroupingIndexSearcher(
        IndexReaderContext parent,
        Executor executor,
        Weight lastDocPerGroup,
        Concurrency concurrency) {
      super(parent, executor);
      this.lastDocPerGroup = lastDocPerGroup;
      this.concurrency = concurrency;
    }

    @Override
    public LeafSlice[] slices(List<LeafReaderContext> leaves) {
      if (concurrency == Concurrency.INTER_SEGMENT) {
        return super.slices(leaves);
      }
      List<LeafSlice> slices = new ArrayList<>();
      for (LeafReaderContext leaf : leaves) {
        List<Integer> blockEnds = new ArrayList<>();
        try {
          Scorer scorer = lastDocPerGroup.scorer(leaf);
          if (scorer != null) {
            DocIdSetIterator it = scorer.iterator();
            int doc;
            while ((doc = it.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
              blockEnds.add(doc);
            }
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        if (blockEnds.isEmpty()) {
          slices.add(
              new LeafSlice(List.of(LeafReaderContextPartition.createForEntireSegment(leaf))));
          continue;
        }
        int minDoc = 0;
        int i = 0;
        while (i < blockEnds.size()) {
          int blocksInSlice = 1 + random().nextInt(Math.max(1, blockEnds.size() - i));
          int sliceEnd = blockEnds.get(Math.min(i + blocksInSlice, blockEnds.size()) - 1) + 1;
          int maxDoc = Math.min(sliceEnd, leaf.reader().maxDoc());
          if (minDoc < maxDoc) {
            slices.add(
                new LeafSlice(
                    List.of(LeafReaderContextPartition.createFromAndTo(leaf, minDoc, maxDoc))));
          }
          minDoc = maxDoc;
          i += blocksInSlice;
        }
        if (minDoc < leaf.reader().maxDoc()) {
          slices.add(
              new LeafSlice(
                  List.of(
                      LeafReaderContextPartition.createFromAndTo(
                          leaf, minDoc, leaf.reader().maxDoc()))));
        }
      }
      return slices.toArray(LeafSlice[]::new);
    }
  }

  protected String generateRandomNonEmptyString() {
    String randomValue;
    do {
      // B/c of DV based impl we can't see the difference between an empty string and a null value.
      // For that reason we don't generate empty string
      // groups.
      randomValue = TestUtil.randomRealisticUnicodeString(random());
      // randomValue = _TestUtil.randomSimpleString(random());
    } while (randomValue.isEmpty());
    return randomValue;
  }

  protected static void assertScoreDocsEquals(ScoreDoc[] expected, ScoreDoc[] actual) {
    assertEquals(expected.length, actual.length);
    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i].doc, actual[i].doc);
      assertEquals(expected[i].score, actual[i].score, 0);
    }
  }

  protected static class Shard implements Closeable {

    final Directory directory;
    final RandomIndexWriter writer;
    IndexSearcher searcher;

    Shard() throws IOException {
      this.directory = newDirectory();
      this.writer =
          new RandomIndexWriter(
              random(),
              directory,
              newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));
    }

    IndexSearcher getIndexSearcher() throws IOException {
      if (searcher == null) {
        searcher = new IndexSearcher(this.writer.getReader());
      }
      return searcher;
    }

    @Override
    public void close() throws IOException {
      if (searcher != null) {
        searcher.getIndexReader().close();
      }
      IOUtils.close(writer, directory);
    }
  }
}
