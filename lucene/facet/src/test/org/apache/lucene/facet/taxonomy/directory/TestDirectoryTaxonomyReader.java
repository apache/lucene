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
package org.apache.lucene.facet.taxonomy.directory;

import static org.apache.lucene.facet.taxonomy.TaxonomyReader.INVALID_ORDINAL;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.ParallelTaxonomyArrays;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.TaxonomyReader.ChildrenIterator;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.LogMergePolicy;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.util.IOUtils;
import org.junit.Test;

public class TestDirectoryTaxonomyReader extends FacetTestCase {

  private static FacetLabel ILLEGAL_PATH =
      new FacetLabel("PATH_THAT_CAUSED_IllegalArgumentException");

  @Test
  public void testCloseAfterIncRef() throws Exception {
    Directory dir = newDirectory();
    DirectoryTaxonomyWriter ltw = new DirectoryTaxonomyWriter(dir);
    ltw.addCategory(new FacetLabel("a"));
    ltw.close();

    DirectoryTaxonomyReader ltr = new DirectoryTaxonomyReader(dir);
    ltr.incRef();
    ltr.close();

    // should not fail as we incRef() before close
    ltr.getSize();
    ltr.decRef();

    dir.close();
  }

  @Test
  public void testCloseTwice() throws Exception {
    Directory dir = newDirectory();
    DirectoryTaxonomyWriter ltw = new DirectoryTaxonomyWriter(dir);
    ltw.addCategory(new FacetLabel("a"));
    ltw.close();

    DirectoryTaxonomyReader ltr = new DirectoryTaxonomyReader(dir);
    ltr.close();
    ltr.close(); // no exception should be thrown

    dir.close();
  }

  @Test
  public void testOpenIfChangedResult() throws Exception {
    Directory dir = null;
    DirectoryTaxonomyWriter ltw = null;
    DirectoryTaxonomyReader ltr = null;

    try {
      dir = newDirectory();
      ltw = new DirectoryTaxonomyWriter(dir);

      ltw.addCategory(new FacetLabel("a"));
      ltw.commit();

      ltr = new DirectoryTaxonomyReader(dir);
      assertNull("Nothing has changed", TaxonomyReader.openIfChanged(ltr));

      ltw.addCategory(new FacetLabel("b"));
      ltw.commit();

      DirectoryTaxonomyReader newtr = TaxonomyReader.openIfChanged(ltr);
      assertNotNull("changes were committed", newtr);
      assertNull("Nothing has changed", TaxonomyReader.openIfChanged(newtr));
      newtr.close();
    } finally {
      IOUtils.close(ltw, ltr, dir);
    }
  }

  @Test
  public void testAlreadyClosed() throws Exception {
    Directory dir = newDirectory();
    DirectoryTaxonomyWriter ltw = new DirectoryTaxonomyWriter(dir);
    ltw.addCategory(new FacetLabel("a"));
    ltw.close();

    DirectoryTaxonomyReader ltr = new DirectoryTaxonomyReader(dir);
    ltr.close();
    expectThrows(
        AlreadyClosedException.class,
        () -> {
          ltr.getSize();
        });

    dir.close();
  }

  /** recreating a taxonomy should work well with a freshly opened taxonomy reader */
  @Test
  public void testFreshReadRecreatedTaxonomy() throws Exception {
    doTestReadRecreatedTaxonomy(random(), true);
  }

  @Test
  public void testOpenIfChangedReadRecreatedTaxonomy() throws Exception {
    doTestReadRecreatedTaxonomy(random(), false);
  }

  private void doTestReadRecreatedTaxonomy(Random random, boolean closeReader) throws Exception {
    Directory dir = null;
    TaxonomyWriter tw = null;
    TaxonomyReader tr = null;

    // prepare a few categories
    int n = 10;
    FacetLabel[] cp = new FacetLabel[n];
    for (int i = 0; i < n; i++) {
      cp[i] = new FacetLabel("a", Integer.toString(i));
    }

    try {
      dir = newDirectory();

      tw = new DirectoryTaxonomyWriter(dir);
      tw.addCategory(new FacetLabel("a"));
      tw.close();

      tr = new DirectoryTaxonomyReader(dir);
      int baseNumCategories = tr.getSize();

      for (int i = 0; i < n; i++) {
        int k = random.nextInt(n);
        tw = new DirectoryTaxonomyWriter(dir, OpenMode.CREATE);
        for (int j = 0; j <= k; j++) {
          tw.addCategory(cp[j]);
        }
        tw.close();
        if (closeReader) {
          tr.close();
          tr = new DirectoryTaxonomyReader(dir);
        } else {
          TaxonomyReader newtr = TaxonomyReader.openIfChanged(tr);
          assertNotNull(newtr);
          tr.close();
          tr = newtr;
        }
        assertEquals(
            "Wrong #categories in taxonomy (i=" + i + ", k=" + k + ")",
            baseNumCategories + 1 + k,
            tr.getSize());
      }
    } finally {
      IOUtils.close(tr, tw, dir);
    }
  }

  @Test
  public void testOpenIfChangedAndRefCount() throws Exception {
    Directory dir = new ByteBuffersDirectory(); // no need for random directories here

    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(dir);
    taxoWriter.addCategory(new FacetLabel("a"));
    taxoWriter.commit();

    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(dir);
    assertEquals("wrong refCount", 1, taxoReader.getRefCount());

    taxoReader.incRef();
    assertEquals("wrong refCount", 2, taxoReader.getRefCount());

    taxoWriter.addCategory(new FacetLabel("a", "b"));
    taxoWriter.commit();
    TaxonomyReader newtr = TaxonomyReader.openIfChanged(taxoReader);
    assertNotNull(newtr);
    taxoReader.close();
    taxoReader = newtr;
    assertEquals("wrong refCount", 1, taxoReader.getRefCount());

    taxoWriter.close();
    taxoReader.close();
    dir.close();
  }

  @Test
  public void testOpenIfChangedManySegments() throws Exception {
    // test openIfChanged() when the taxonomy contains many segments
    Directory dir = newDirectory();

    DirectoryTaxonomyWriter writer =
        new DirectoryTaxonomyWriter(dir) {
          @Override
          protected IndexWriterConfig createIndexWriterConfig(OpenMode openMode) {
            IndexWriterConfig conf = super.createIndexWriterConfig(openMode);
            LogMergePolicy lmp = (LogMergePolicy) conf.getMergePolicy();
            lmp.setMergeFactor(2);
            return conf;
          }
        };
    TaxonomyReader reader = new DirectoryTaxonomyReader(writer);

    int numRounds = random().nextInt(10) + 10;
    int numCategories = 1; // one for root
    for (int i = 0; i < numRounds; i++) {
      int numCats = random().nextInt(4) + 1;
      for (int j = 0; j < numCats; j++) {
        writer.addCategory(new FacetLabel(Integer.toString(i), Integer.toString(j)));
      }
      numCategories += numCats + 1 /* one for round-parent */;
      TaxonomyReader newtr = TaxonomyReader.openIfChanged(reader);
      assertNotNull(newtr);
      reader.close();
      reader = newtr;

      // assert categories
      assertEquals(numCategories, reader.getSize());
      int roundOrdinal = reader.getOrdinal(new FacetLabel(Integer.toString(i)));
      ParallelTaxonomyArrays.IntArray parents = reader.getParallelTaxonomyArrays().parents();
      assertEquals(0, parents.get(roundOrdinal)); // round's parent is root
      for (int j = 0; j < numCats; j++) {
        int ord = reader.getOrdinal(new FacetLabel(Integer.toString(i), Integer.toString(j)));
        assertEquals(roundOrdinal, parents.get(ord)); // round's parent is root
      }
    }

    reader.close();
    writer.close();
    dir.close();
  }

  @Test
  public void testOpenIfChangedMergedSegment() throws Exception {
    // test openIfChanged() when all index segments were merged - used to be
    // a bug in ParentArray, caught by testOpenIfChangedManySegments - only
    // this test is not random
    Directory dir = newDirectory();

    // hold onto IW to forceMerge
    // note how we don't close it, since DTW will close it.
    final IndexWriter iw =
        new IndexWriter(
            dir,
            new IndexWriterConfig(new MockAnalyzer(random()))
                .setMergePolicy(new LogByteSizeMergePolicy()));
    DirectoryTaxonomyWriter writer =
        new DirectoryTaxonomyWriter(dir) {
          @Override
          protected IndexWriter openIndexWriter(Directory directory, IndexWriterConfig config)
              throws IOException {
            return iw;
          }
        };

    TaxonomyReader reader = new DirectoryTaxonomyReader(writer);
    assertEquals(1, reader.getSize());
    assertEquals(1, reader.getParallelTaxonomyArrays().parents().length());

    // add category and call forceMerge -- this should flush IW and merge segments down to 1
    // in ParentArray.initFromReader, this used to fail assuming there are no parents.
    writer.addCategory(new FacetLabel("1"));
    iw.forceMerge(1);

    // now calling openIfChanged should trip on the bug
    TaxonomyReader newtr = TaxonomyReader.openIfChanged(reader);
    assertNotNull(newtr);
    reader.close();
    reader = newtr;
    assertEquals(2, reader.getSize());
    assertEquals(2, reader.getParallelTaxonomyArrays().parents().length());

    reader.close();
    writer.close();
    dir.close();
  }

  @Test
  public void testOpenIfChangedNoChangesButSegmentMerges() throws Exception {
    // test openIfChanged() when the taxonomy hasn't really changed, but segments
    // were merged. The NRT reader will be reopened, and ParentArray used to assert
    // that the new reader contains more ordinals than were given from the old
    // TaxReader version
    Directory dir = newDirectory();

    // hold onto IW to forceMerge
    // note how we don't close it, since DTW will close it.
    final IndexWriter iw =
        new IndexWriter(
            dir,
            new IndexWriterConfig(new MockAnalyzer(random()))
                .setMergePolicy(new LogByteSizeMergePolicy()));
    DirectoryTaxonomyWriter writer =
        new DirectoryTaxonomyWriter(dir) {
          @Override
          protected IndexWriter openIndexWriter(Directory directory, IndexWriterConfig config)
              throws IOException {
            return iw;
          }
        };

    // add a category so that the following DTR open will cause a flush and
    // a new segment will be created
    writer.addCategory(new FacetLabel("a"));

    TaxonomyReader reader = new DirectoryTaxonomyReader(writer);
    assertEquals(2, reader.getSize());
    assertEquals(2, reader.getParallelTaxonomyArrays().parents().length());

    // merge all the segments so that NRT reader thinks there's a change
    iw.forceMerge(1);

    // now calling openIfChanged should trip on the wrong assert in ParetArray's ctor
    TaxonomyReader newtr = TaxonomyReader.openIfChanged(reader);
    assertNotNull(newtr);
    reader.close();
    reader = newtr;
    assertEquals(2, reader.getSize());
    assertEquals(2, reader.getParallelTaxonomyArrays().parents().length());

    reader.close();
    writer.close();
    dir.close();
  }

  @Test
  public void testOpenIfChangedReuseAfterRecreate() throws Exception {
    // tests that if the taxonomy is recreated, no data is reused from the previous taxonomy
    Directory dir = newDirectory();
    DirectoryTaxonomyWriter writer = new DirectoryTaxonomyWriter(dir);
    FacetLabel cp_a = new FacetLabel("a");
    writer.addCategory(cp_a);
    writer.close();

    DirectoryTaxonomyReader r1 = new DirectoryTaxonomyReader(dir);
    // fill r1's caches
    assertPathsAndOrdinals(r1, new int[] {1}, new FacetLabel[] {cp_a});

    // now recreate, add a different category
    writer = new DirectoryTaxonomyWriter(dir, OpenMode.CREATE);
    FacetLabel cp_b = new FacetLabel("b");
    writer.addCategory(cp_b);
    writer.close();

    DirectoryTaxonomyReader r2 = TaxonomyReader.openIfChanged(r1);
    assertNotNull(r2);

    // fill r2's caches
    assertPathsAndOrdinals(r2, new int[] {1}, new FacetLabel[] {cp_b});

    // check that r1 doesn't see cp_b
    assertGettingOrdinals(r1, new int[] {1, INVALID_ORDINAL}, new FacetLabel[] {cp_a, cp_b});
    assertGettingPaths(r1, new FacetLabel[] {cp_a, ILLEGAL_PATH}, new int[] {1, 2});

    // check that r2 doesn't see cp_a
    assertGettingOrdinals(r2, new int[] {INVALID_ORDINAL, 1}, new FacetLabel[] {cp_a, cp_b});
    assertGettingPaths(r2, new FacetLabel[] {cp_b, ILLEGAL_PATH}, new int[] {1, 2});

    r2.close();
    r1.close();
    dir.close();
  }

  @Test
  public void testOpenIfChangedReuse() throws Exception {
    // test the reuse of data from the old DTR instance
    for (boolean nrt : new boolean[] {false, true}) {
      Directory dir = newDirectory();
      DirectoryTaxonomyWriter writer = new DirectoryTaxonomyWriter(dir);

      FacetLabel cp_a = new FacetLabel("a");
      writer.addCategory(cp_a);
      if (!nrt) writer.commit();

      DirectoryTaxonomyReader r1 =
          nrt ? new DirectoryTaxonomyReader(writer) : new DirectoryTaxonomyReader(dir);
      // fill r1's caches
      assertPathsAndOrdinals(r1, new int[] {1}, new FacetLabel[] {cp_a});

      FacetLabel cp_b = new FacetLabel("b");
      writer.addCategory(cp_b);
      if (!nrt) writer.commit();

      DirectoryTaxonomyReader r2 = TaxonomyReader.openIfChanged(r1);
      assertNotNull(r2);

      // add r2's categories to the caches
      assertPathsAndOrdinals(r2, new int[] {1, 2}, new FacetLabel[] {cp_a, cp_b});

      // check that r1 doesn't see cp_b
      assertGettingOrdinals(r1, new int[] {1, INVALID_ORDINAL}, new FacetLabel[] {cp_a, cp_b});
      assertGettingPaths(r1, new FacetLabel[] {cp_a, ILLEGAL_PATH}, new int[] {1, 2});

      r1.close();
      r2.close();
      writer.close();
      dir.close();
    }
  }

  @Test
  public void testOpenIfChangedReplaceTaxonomy() throws Exception {
    // test openIfChanged when replaceTaxonomy is called, which is equivalent to recreate
    // only can work with NRT as well
    Directory src = newDirectory();
    DirectoryTaxonomyWriter w = new DirectoryTaxonomyWriter(src);
    FacetLabel cp_b = new FacetLabel("b");
    w.addCategory(cp_b);
    w.close();

    for (boolean nrt : new boolean[] {false, true}) {
      Directory dir = newDirectory();
      DirectoryTaxonomyWriter writer = new DirectoryTaxonomyWriter(dir);

      FacetLabel cp_a = new FacetLabel("a");
      writer.addCategory(cp_a);
      if (!nrt) writer.commit();

      DirectoryTaxonomyReader r1 =
          nrt ? new DirectoryTaxonomyReader(writer) : new DirectoryTaxonomyReader(dir);
      // fill r1's caches
      assertPathsAndOrdinals(r1, new int[] {1}, new FacetLabel[] {cp_a});

      // now replace taxonomy
      writer.replaceTaxonomy(src);
      if (!nrt) writer.commit();

      DirectoryTaxonomyReader r2 = TaxonomyReader.openIfChanged(r1);
      assertNotNull(r2);

      // fill r2's caches
      assertPathsAndOrdinals(r2, new int[] {1}, new FacetLabel[] {cp_b});

      // check that r1 doesn't see cp_b
      assertGettingOrdinals(r1, new int[] {1, INVALID_ORDINAL}, new FacetLabel[] {cp_a, cp_b});
      assertGettingPaths(r1, new FacetLabel[] {cp_a, ILLEGAL_PATH}, new int[] {1, 2});

      // check that r2 doesn't see cp_a
      assertGettingOrdinals(r2, new int[] {INVALID_ORDINAL, 1}, new FacetLabel[] {cp_a, cp_b});
      assertGettingPaths(r2, new FacetLabel[] {cp_b, ILLEGAL_PATH}, new int[] {1, 2});

      r2.close();
      r1.close();
      writer.close();
      dir.close();
    }

    src.close();
  }

  private void assertGettingOrdinals(
      DirectoryTaxonomyReader reader, int[] expectedOrds, FacetLabel[] sourcePaths)
      throws IOException {
    // To exercise mix of cache hit and cache misses for getOrdinal and getBulkOrdinals this method:
    // 1. Randomly gets a few ords using sequential calls.
    // 2. Call bulk get method.
    // 3. Continue sequential calls for the remaining items.
    assertEquals(expectedOrds.length, sourcePaths.length);
    int bulkOperationsIteration = random().nextInt(sourcePaths.length);
    List<Integer> indexesShuffled =
        new ArrayList<>(
            IntStream.range(0, sourcePaths.length).boxed().collect(Collectors.toList()));
    Collections.shuffle(indexesShuffled, random());

    for (int i = 0; i < bulkOperationsIteration; i++) {
      int nextIndex = indexesShuffled.get(i);
      assertEquals(expectedOrds[nextIndex], reader.getOrdinal(sourcePaths[nextIndex]));
    }

    int[] bulkOrdResult = reader.getBulkOrdinals(sourcePaths);
    assertArrayEquals(expectedOrds, bulkOrdResult);

    for (int i = bulkOperationsIteration; i < sourcePaths.length; i++) {
      int nextIndex = indexesShuffled.get(i);
      assertEquals(expectedOrds[nextIndex], reader.getOrdinal(sourcePaths[nextIndex]));
    }
  }

  private void assertGettingPaths(
      DirectoryTaxonomyReader reader, FacetLabel[] expectedPaths, int[] sourceOrds)
      throws IOException {
    // To exercise mix of cache hit and cache misses for getPath and getBulkPath this method:
    // 1. Randomly gets a few paths using sequential calls.
    // 2. Call bulk get method.
    // 3. Continue sequential calls for the remaining items.
    // Note: expectedPaths should refer to ILLEGAL_PATH for ords from sourceOrds that are expected
    // to throw IllegalArgumentException
    assertEquals(expectedPaths.length, sourceOrds.length);
    int bulkOperationsIteration = random().nextInt(sourceOrds.length);
    List<Integer> indexesShuffled =
        new ArrayList<>(IntStream.range(0, sourceOrds.length).boxed().collect(Collectors.toList()));
    Collections.shuffle(indexesShuffled, random());

    boolean illegalPathExceptionIsExpected =
        Arrays.stream(expectedPaths).anyMatch(x -> x == ILLEGAL_PATH);
    for (int i = 0; i < bulkOperationsIteration; i++) {
      int nextIndex = indexesShuffled.get(i);
      if (expectedPaths[nextIndex] == ILLEGAL_PATH) {
        expectThrows(IllegalArgumentException.class, () -> reader.getPath(sourceOrds[nextIndex]));
      } else {
        assertEquals(expectedPaths[nextIndex], reader.getPath(sourceOrds[nextIndex]));
      }
    }

    if (illegalPathExceptionIsExpected) {
      expectThrows(IllegalArgumentException.class, () -> reader.getBulkPath(sourceOrds));
    } else {
      // clone because getBulkPath changes order of param's elements
      int[] sourceOrdsCopy = sourceOrds.clone();
      FacetLabel[] bulkPathsResult = reader.getBulkPath(sourceOrdsCopy);
      assertArrayEquals(expectedPaths, bulkPathsResult);
    }

    for (int i = bulkOperationsIteration; i < sourceOrds.length; i++) {
      int nextIndex = indexesShuffled.get(i);
      if (expectedPaths[nextIndex] == ILLEGAL_PATH) {
        expectThrows(IllegalArgumentException.class, () -> reader.getPath(sourceOrds[nextIndex]));
      } else {
        assertEquals(expectedPaths[nextIndex], reader.getPath(sourceOrds[nextIndex]));
      }
    }
  }

  private void assertPathsAndOrdinals(
      DirectoryTaxonomyReader reader, int[] ords, FacetLabel[] paths) throws IOException {
    // use this method to assert "symmetric" ordinals and paths: when source ords and paths match
    // expected ords and paths. This works for valid ords and paths that exist in the index.
    assertGettingPaths(reader, paths, ords);
    assertGettingOrdinals(reader, ords, paths);
  }

  @Test
  public void testGetChildren() throws Exception {
    Directory dir = newDirectory();
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(dir);
    int numCategories = atLeast(10);
    int numA = 0, numB = 0;
    Random random = random();
    // add the two categories for which we'll also add children (so asserts are simpler)
    taxoWriter.addCategory(new FacetLabel("a"));
    taxoWriter.addCategory(new FacetLabel("b"));
    for (int i = 0; i < numCategories; i++) {
      if (random.nextBoolean()) {
        taxoWriter.addCategory(new FacetLabel("a", Integer.toString(i)));
        ++numA;
      } else {
        taxoWriter.addCategory(new FacetLabel("b", Integer.toString(i)));
        ++numB;
      }
    }
    // add category with no children
    taxoWriter.addCategory(new FacetLabel("c"));
    taxoWriter.close();

    DirectoryTaxonomyReader taxoReader = new DirectoryTaxonomyReader(dir);

    // non existing category
    ChildrenIterator it = taxoReader.getChildren(taxoReader.getOrdinal(new FacetLabel("invalid")));
    assertEquals(INVALID_ORDINAL, it.next());

    // a category with no children
    it = taxoReader.getChildren(taxoReader.getOrdinal(new FacetLabel("c")));
    assertEquals(INVALID_ORDINAL, it.next());

    // arbitrary negative ordinal
    it = taxoReader.getChildren(-2);
    assertEquals(INVALID_ORDINAL, it.next());

    // root's children
    Set<String> roots = new HashSet<>(Arrays.asList("a", "b", "c"));
    it = taxoReader.getChildren(TaxonomyReader.ROOT_ORDINAL);
    while (!roots.isEmpty()) {
      FacetLabel root = taxoReader.getPath(it.next());
      assertEquals(1, root.length);
      assertTrue(roots.remove(root.components[0]));
    }
    assertEquals(INVALID_ORDINAL, it.next());

    for (int i = 0; i < 2; i++) {
      FacetLabel cp = i == 0 ? new FacetLabel("a") : new FacetLabel("b");
      int ordinal = taxoReader.getOrdinal(cp);
      it = taxoReader.getChildren(ordinal);
      int numChildren = 0;
      int child;
      while ((child = it.next()) != INVALID_ORDINAL) {
        FacetLabel path = taxoReader.getPath(child);
        assertEquals(2, path.length);
        assertEquals(path.components[0], i == 0 ? "a" : "b");
        ++numChildren;
      }
      int expected = i == 0 ? numA : numB;
      assertEquals("invalid num children", expected, numChildren);
    }
    taxoReader.close();

    dir.close();
  }

  @Test
  public void testAccountable() throws Exception {
    Directory dir = newDirectory();
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(dir);
    int numCategories = atLeast(10);
    Random random = random();
    // add the two categories for which we'll also add children (so asserts are simpler)
    taxoWriter.addCategory(new FacetLabel("a"));
    taxoWriter.addCategory(new FacetLabel("b"));
    for (int i = 0; i < numCategories; i++) {
      if (random.nextBoolean()) {
        taxoWriter.addCategory(new FacetLabel("a", Integer.toString(i)));
      } else {
        taxoWriter.addCategory(new FacetLabel("b", Integer.toString(i)));
      }
    }
    // add category with no children
    taxoWriter.addCategory(new FacetLabel("c"));
    taxoWriter.close();

    DirectoryTaxonomyReader taxoReader = new DirectoryTaxonomyReader(dir);
    taxoReader.getParallelTaxonomyArrays(); // increases memory usage as a side-effect
    assertTrue(taxoReader.ramBytesUsed() > 0);
    assertTrue(taxoReader.getChildResources().size() > 0);
    taxoReader.close();
    dir.close();
  }

  @Test
  public void testGetPathAndOrdinalsRandomMultithreading() throws Exception {
    Directory src = newDirectory();
    DirectoryTaxonomyWriter w = new DirectoryTaxonomyWriter(src);
    final int maxNumberOfLabelsToIndex = 1000;
    final int maxNumberOfUniqueLabelsToIndex = maxNumberOfLabelsToIndex / 2;
    final int cacheSize = maxNumberOfUniqueLabelsToIndex / 2; // to cause some cache evictions
    String randomArray[] = new String[RandomizedTest.randomIntBetween(1, maxNumberOfLabelsToIndex)];
    // adding a smaller bound on ints ensures that we will have some duplicate ordinals in random
    // test cases
    Arrays.setAll(
        randomArray, i -> Integer.toString(random().nextInt(maxNumberOfUniqueLabelsToIndex)));

    FacetLabel allPaths[] = new FacetLabel[randomArray.length];

    for (int i = 0; i < randomArray.length; i++) {
      allPaths[i] = new FacetLabel(randomArray[i]);
      w.addCategory(allPaths[i]);
      // add random commits to create multiple segments in the index
      if (random().nextBoolean()) {
        w.commit();
      }
    }
    w.commit();
    w.close();

    DirectoryTaxonomyReader r1 = new DirectoryTaxonomyReader(src);
    r1.setCacheSize(cacheSize);

    int allOrdinals[] = r1.getBulkOrdinals(allPaths);

    // Assert getPath and getBulkPath first, then assert getOrdinal and getBulkOrdinals.
    // Create multiple threads to check result correctness and thread contention in the cache.
    for (boolean assertGettingOrdinals : new boolean[] {false, true}) {
      Thread[] addThreads = new Thread[RandomNumbers.randomIntBetween(random(), 1, 12)];
      for (int z = 0; z < addThreads.length; z++) {
        addThreads[z] =
            new Thread() {
              @Override
              public void run() {
                // each thread iterates for numThreadIterations times
                int numThreadIterations = random().nextInt(10);
                for (int threadIterations = 0;
                    threadIterations < numThreadIterations;
                    threadIterations++) {

                  // length of the FacetLabel array that we are going to check
                  int numOfOrdinalsToCheck = RandomizedTest.randomIntBetween(1, allOrdinals.length);
                  int[] ordinals = new int[numOfOrdinalsToCheck];
                  FacetLabel[] path = new FacetLabel[numOfOrdinalsToCheck];

                  for (int i = 0; i < numOfOrdinalsToCheck; i++) {
                    // we deliberately allow it to choose repeat indexes as this will exercise the
                    // cache
                    int ordinalIndex = random().nextInt(allOrdinals.length);
                    ordinals[i] = allOrdinals[ordinalIndex];
                    path[i] = allPaths[ordinalIndex];
                  }

                  try {
                    // main check for correctness is done here
                    if (assertGettingOrdinals) {
                      assertGettingOrdinals(r1, ordinals, path);
                    } else {
                      assertGettingPaths(r1, path, ordinals);
                    }
                  } catch (IOException e) {
                    // this should ideally never occur, but if it does just rethrow the error to the
                    // caller
                    throw new RuntimeException(e);
                  }
                }
              }
            };
      }
      for (Thread t : addThreads) t.start();
      for (Thread t : addThreads) t.join();
    }

    r1.close();
    src.close();
  }
}
