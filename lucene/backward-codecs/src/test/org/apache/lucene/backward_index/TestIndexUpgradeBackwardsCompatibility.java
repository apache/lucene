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
package org.apache.lucene.backward_index;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexUpgrader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.Version;

public class TestIndexUpgradeBackwardsCompatibility extends BackwardsCompatibilityTestBase {

  /**
   * A parameter constructor for {@link com.carrotsearch.randomizedtesting.RandomizedRunner}. See
   * {@link #testVersionsFactory()} for details on the values provided to the framework.
   */
  public TestIndexUpgradeBackwardsCompatibility(Version version, String pattern) {
    super(version, pattern);
  }

  /** Provides all current version to the test-framework for each of the index suffixes. */
  @ParametersFactory(argumentFormatting = "Lucene-Version:%1$s; Pattern: %2$s")
  public static Iterable<Object[]> testVersionsFactory() throws IllegalAccessException {
    Iterable<Object[]> allSupportedVersions =
        allVersion(
            TestBasicBackwardsCompatibility.INDEX_NAME,
            TestBasicBackwardsCompatibility.SUFFIX_CFS,
            TestBasicBackwardsCompatibility.SUFFIX_NO_CFS);
    return allSupportedVersions;
  }

  /** Randomizes the use of some of the constructor variations */
  static IndexUpgrader newIndexUpgrader(Directory dir) {
    final boolean streamType = random().nextBoolean();
    final int choice = TestUtil.nextInt(random(), 0, 2);
    switch (choice) {
      case 0:
        return new IndexUpgrader(dir);
      case 1:
        return new IndexUpgrader(dir, streamType ? null : InfoStream.NO_OUTPUT, false);
      case 2:
        return new IndexUpgrader(dir, newIndexWriterConfig(null), false);
      default:
        fail("case statement didn't get updated when random bounds changed");
    }
    return null; // never get here
  }

  public void testUpgradeOldIndex() throws Exception {
    int indexCreatedVersion =
        SegmentInfos.readLatestCommit(directory).getIndexCreatedVersionMajor();
    newIndexUpgrader(directory).upgrade();
    checkAllSegmentsUpgraded(directory, indexCreatedVersion);
  }

  @Override
  protected void createIndex(Directory directory) throws IOException {
    if (indexPattern.equals(
        createPattern(
            TestBasicBackwardsCompatibility.INDEX_NAME,
            TestBasicBackwardsCompatibility.SUFFIX_CFS))) {
      TestBasicBackwardsCompatibility.createIndex(directory, true, false);
    } else {
      TestBasicBackwardsCompatibility.createIndex(directory, false, false);
    }
  }

  public void testUpgradeOldSingleSegmentIndexWithAdditions() throws Exception {
    // TODO we use to have single segment indices but we stopped creating them at some point
    // either delete the test or recreate the indices
    assumeTrue("Original index must be single segment", 1 == getNumberOfSegments(directory));
    int indexCreatedVersion =
        SegmentInfos.readLatestCommit(directory).getIndexCreatedVersionMajor();

    // create a bunch of dummy segments
    int id = 40;
    Directory ramDir = new ByteBuffersDirectory();
    for (int i = 0; i < 3; i++) {
      // only use Log- or TieredMergePolicy, to make document addition predictable and not
      // suddenly merge:
      MergePolicy mp = random().nextBoolean() ? newLogMergePolicy() : newTieredMergePolicy();
      IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(mp);
      IndexWriter w = new IndexWriter(ramDir, iwc);
      // add few more docs:
      for (int j = 0; j < RANDOM_MULTIPLIER * random().nextInt(30); j++) {
        TestBasicBackwardsCompatibility.addDoc(w, id++);
      }
      try {
        w.commit();
      } finally {
        w.close();
      }
    }

    // add dummy segments (which are all in current
    // version) to single segment index
    MergePolicy mp = random().nextBoolean() ? newLogMergePolicy() : newTieredMergePolicy();
    IndexWriterConfig iwc = new IndexWriterConfig(null).setMergePolicy(mp);
    IndexWriter w = new IndexWriter(directory, iwc);
    w.addIndexes(ramDir);
    try (w) {
      w.commit();
    }

    // determine count of segments in modified index
    final int origSegCount = getNumberOfSegments(directory);

    // ensure there is only one commit
    assertEquals(1, DirectoryReader.listCommits(directory).size());
    newIndexUpgrader(directory).upgrade();

    final int segCount = checkAllSegmentsUpgraded(directory, indexCreatedVersion);
    assertEquals(
        "Index must still contain the same number of segments, as only one segment was upgraded and nothing else merged",
        origSegCount,
        segCount);
  }

  // LUCENE-5907
  public void testUpgradeWithNRTReader() throws Exception {
    IndexWriter writer =
        new IndexWriter(
            directory,
            newIndexWriterConfig(new MockAnalyzer(random()))
                .setOpenMode(IndexWriterConfig.OpenMode.APPEND));
    writer.addDocument(new Document());
    DirectoryReader r = DirectoryReader.open(writer);
    writer.commit();
    r.close();
    writer.forceMerge(1);
    writer.commit();
    writer.rollback();
    SegmentInfos.readLatestCommit(directory);
  }

  // LUCENE-5907
  public void testUpgradeThenMultipleCommits() throws Exception {
    IndexWriter writer =
        new IndexWriter(
            directory,
            newIndexWriterConfig(new MockAnalyzer(random()))
                .setOpenMode(IndexWriterConfig.OpenMode.APPEND));
    writer.addDocument(new Document());
    writer.commit();
    writer.addDocument(new Document());
    writer.commit();
    writer.close();
  }

  public void testIndexUpgraderCommandLineArgs() throws Exception {
    PrintStream savedSystemOut = System.out;
    System.setOut(new PrintStream(new ByteArrayOutputStream(), false, UTF_8));
    try {
      String name = indexName(this.version);
      Directory origDir = directory;
      int indexCreatedVersion =
          SegmentInfos.readLatestCommit(origDir).getIndexCreatedVersionMajor();
      Path dir = createTempDir(name);
      try (FSDirectory fsDir = FSDirectory.open(dir)) {
        // beware that ExtraFS might add extraXXX files
        Set<String> extraFiles = Set.of(fsDir.listAll());
        for (String file : origDir.listAll()) {
          if (extraFiles.contains(file) == false) {
            fsDir.copyFrom(origDir, file, file, IOContext.DEFAULT);
          }
        }
      }

      String path = dir.toAbsolutePath().toString();

      List<String> args = new ArrayList<>();
      if (random().nextBoolean()) {
        args.add("-verbose");
      }
      if (random().nextBoolean()) {
        args.add("-delete-prior-commits");
      }
      if (random().nextBoolean()) {
        // TODO: need to better randomize this, but ...
        //  - LuceneTestCase.FS_DIRECTORIES is private
        //  - newFSDirectory returns BaseDirectoryWrapper
        //  - BaseDirectoryWrapper doesn't expose delegate
        Class<? extends FSDirectory> dirImpl = NIOFSDirectory.class;

        args.add("-dir-impl");
        args.add(dirImpl.getName());
      }
      args.add(path);

      IndexUpgrader.main(args.toArray(new String[0]));

      try (Directory upgradedDir = newFSDirectory(dir)) {
        checkAllSegmentsUpgraded(upgradedDir, indexCreatedVersion);
      }

    } finally {
      System.setOut(savedSystemOut);
    }
  }

  static int checkAllSegmentsUpgraded(Directory dir, int indexCreatedVersion) throws IOException {
    final SegmentInfos infos = SegmentInfos.readLatestCommit(dir);
    if (VERBOSE) {
      System.out.println("checkAllSegmentsUpgraded: " + infos);
    }
    for (SegmentCommitInfo si : infos) {
      assertEquals(Version.LATEST, si.info.getVersion());
      assertNotNull(si.getId());
    }
    assertEquals(Version.LATEST, infos.getCommitLuceneVersion());
    assertEquals(indexCreatedVersion, infos.getIndexCreatedVersionMajor());
    return infos.size();
  }

  static int getNumberOfSegments(Directory dir) throws IOException {
    final SegmentInfos infos = SegmentInfos.readLatestCommit(dir);
    return infos.size();
  }
}
