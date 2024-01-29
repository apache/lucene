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

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.StandardDirectoryReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.store.BaseDirectoryWrapper;
import org.apache.lucene.util.Version;

public class TestBinaryBackwardsCompatibility extends BackwardsCompatibilityTestBase {

  static final int MIN_BINARY_SUPPORTED_MAJOR = Version.MIN_SUPPORTED_MAJOR - 1;
  static final String INDEX_NAME = "unsupported";
  static final String SUFFIX_CFS = "-cfs";
  static final String SUFFIX_NO_CFS = "-nocfs";

  public TestBinaryBackwardsCompatibility(Version version, String pattern) {
    super(version, pattern);
  }

  @ParametersFactory(argumentFormatting = "Lucene-Version:%1$s; Pattern: %2$s")
  public static Iterable<Object[]> testVersionsFactory() {
    List<Object[]> params = new ArrayList<>();
    for (Version version : BINARY_SUPPORTED_VERSIONS) {
      params.add(new Object[] {version, createPattern(INDEX_NAME, SUFFIX_CFS)});
      params.add(new Object[] {version, createPattern(INDEX_NAME, SUFFIX_NO_CFS)});
    }
    return params;
  }

  @Override
  void verifyUsesDefaultCodec(Directory dir, String name) throws IOException {
    // don't this will fail since the indices are not supported
  }

  @Override
  protected void createIndex(Directory directory) throws IOException {
    fail("not supported");
  }

  @Nightly
  public void testReadNMinusTwoCommit() throws IOException {

    try (BaseDirectoryWrapper dir = newDirectory(directory)) {
      IndexCommit commit = DirectoryReader.listCommits(dir).get(0);
      StandardDirectoryReader.open(commit, MIN_BINARY_SUPPORTED_MAJOR, null).close();
    }
  }

  @Nightly
  public void testReadNMinusTwoSegmentInfos() throws IOException {
    try (BaseDirectoryWrapper dir = newDirectory(directory)) {
      expectThrows(
          IndexFormatTooOldException.class,
          () -> SegmentInfos.readLatestCommit(dir, Version.MIN_SUPPORTED_MAJOR));
      SegmentInfos.readLatestCommit(dir, MIN_BINARY_SUPPORTED_MAJOR);
    }
  }

  @Nightly
  public void testSearchOldIndex() throws Exception {
    TestBasicBackwardsCompatibility.searchIndex(
        directory, indexPattern, MIN_BINARY_SUPPORTED_MAJOR, version);
  }
}
