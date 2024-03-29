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

import static org.apache.lucene.backward_index.BackwardsCompatibilityTestBase.createPattern;

import java.io.IOException;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems;
import org.apache.lucene.util.Version;

@SuppressFileSystems("ExtrasFS")
public class TestGenerateBwcIndices extends LuceneTestCase {

  // Backcompat index generation, described below, is mostly automated in:
  //
  //    dev-tools/scripts/addBackcompatIndexes.py
  //
  // For usage information, see:
  //
  //    http://wiki.apache.org/lucene-java/ReleaseTodo#Generate_Backcompat_Indexes
  //
  // -----
  //
  // To generate backcompat indexes with the current default codec, run the following gradle
  // command:
  //  gradlew test -Ptests.bwcdir=/path/to/store/indexes -Ptests.codec=default
  //               -Ptests.useSecurityManager=false --tests TestGenerateBwcIndices
  //
  // Also add testmethod with one of the index creation methods below, for example:
  //    -Ptestmethod=testCreateCFS
  //
  // Zip up the generated indexes:
  //
  //    cd /path/to/store/indexes/index.cfs   ; zip index.<VERSION>-cfs.zip *
  //    cd /path/to/store/indexes/index.nocfs ; zip index.<VERSION>-nocfs.zip *
  //
  // Then move those 2 zip files to your trunk checkout and add them
  // to the oldNames array.

  public void testCreateCFS() throws IOException {
    TestBasicBackwardsCompatibility basicTest =
        new TestBasicBackwardsCompatibility(
            Version.LATEST,
            createPattern(
                TestBasicBackwardsCompatibility.INDEX_NAME,
                TestBasicBackwardsCompatibility.SUFFIX_CFS));
    basicTest.createBWCIndex();
  }

  public void testCreateNoCFS() throws IOException {
    TestBasicBackwardsCompatibility basicTest =
        new TestBasicBackwardsCompatibility(
            Version.LATEST,
            createPattern(
                TestBasicBackwardsCompatibility.INDEX_NAME,
                TestBasicBackwardsCompatibility.SUFFIX_NO_CFS));
    basicTest.createBWCIndex();
  }

  public void testCreateSortedIndex() throws IOException {
    TestIndexSortBackwardsCompatibility sortedTest =
        new TestIndexSortBackwardsCompatibility(
            Version.LATEST,
            createPattern(
                TestIndexSortBackwardsCompatibility.INDEX_NAME,
                TestIndexSortBackwardsCompatibility.SUFFIX));
    sortedTest.createBWCIndex();
  }

  public void testCreateInt8HNSWIndices() throws IOException {
    TestInt8HnswBackwardsCompatibility int8HnswBackwardsCompatibility =
        new TestInt8HnswBackwardsCompatibility(
            Version.LATEST,
            createPattern(
                TestInt8HnswBackwardsCompatibility.INDEX_NAME,
                TestInt8HnswBackwardsCompatibility.SUFFIX));
    int8HnswBackwardsCompatibility.createBWCIndex();
  }

  private boolean isInitialMajorVersionRelease() {
    return Version.LATEST.equals(Version.fromBits(Version.LATEST.major, 0, 0));
  }

  public void testCreateMoreTermsIndex() throws IOException {
    if (isInitialMajorVersionRelease()) {
      // TODO - WHY ONLY on the first major version?
      TestMoreTermsBackwardsCompatibility moreTermsTest =
          new TestMoreTermsBackwardsCompatibility(
              Version.LATEST,
              createPattern(
                  TestMoreTermsBackwardsCompatibility.INDEX_NAME,
                  TestMoreTermsBackwardsCompatibility.SUFFIX));
      moreTermsTest.createBWCIndex();
    }
  }

  public void testCreateIndexWithDocValuesUpdates() throws IOException {
    if (isInitialMajorVersionRelease()) {
      // TODO - WHY ONLY on the first major version?
      TestDVUpdateBackwardsCompatibility dvUpdatesTest =
          new TestDVUpdateBackwardsCompatibility(
              Version.LATEST,
              createPattern(
                  TestDVUpdateBackwardsCompatibility.INDEX_NAME,
                  TestDVUpdateBackwardsCompatibility.SUFFIX));
      dvUpdatesTest.createBWCIndex();
    }
  }

  public void testCreateEmptyIndex() throws IOException {
    if (isInitialMajorVersionRelease()) {
      // TODO - WHY ONLY on the first major version?
      TestEmptyIndexBackwardsCompatibility emptyIndex =
          new TestEmptyIndexBackwardsCompatibility(
              Version.LATEST,
              createPattern(
                  TestEmptyIndexBackwardsCompatibility.INDEX_NAME,
                  TestEmptyIndexBackwardsCompatibility.SUFFIX));
      emptyIndex.createBWCIndex();
    }
  }
}
