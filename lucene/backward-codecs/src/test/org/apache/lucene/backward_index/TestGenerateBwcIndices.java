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

import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.Version;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.apache.lucene.backward_index.BackwardsCompatibilityTestBase.createPattern;

public class TestGenerateBwcIndices extends LuceneTestCase {

    public void testGenerate() throws Exception {
        TestIndexSortBackwardsCompatibility sortedTest = new TestIndexSortBackwardsCompatibility(Version.LATEST,
                createPattern(TestIndexSortBackwardsCompatibility.INDEX_NAME, TestIndexSortBackwardsCompatibility.SUFFIX));
        sortedTest.createBWCIndex();

        TestBasicBackwardsCompatibility basicTest = new TestBasicBackwardsCompatibility(Version.LATEST,
                createPattern(TestBasicBackwardsCompatibility.INDEX_NAME, TestBasicBackwardsCompatibility.SUFFIX_CFS));
        basicTest.createBWCIndex();
        basicTest = new TestBasicBackwardsCompatibility(Version.LATEST,
                createPattern(TestBasicBackwardsCompatibility.INDEX_NAME, TestBasicBackwardsCompatibility.SUFFIX_NO_CFS));
        basicTest.createBWCIndex();

        if (Version.LATEST.equals(Version.LUCENE_10_0_0)) {
            // NOCOMMIT - WHY ONLY on the first major verison?
            TestDVUpdateBackwardsCompatibility dvUpdatesTest = new TestDVUpdateBackwardsCompatibility(Version.LATEST,
                    createPattern(TestDVUpdateBackwardsCompatibility.INDEX_NAME, TestDVUpdateBackwardsCompatibility.SUFFIX));
            dvUpdatesTest.createBWCIndex();

            TestMoreTermsBackwardsCompatibility  moreTermsTest = new TestMoreTermsBackwardsCompatibility(Version.LATEST,
                    createPattern(TestMoreTermsBackwardsCompatibility.INDEX_NAME, TestMoreTermsBackwardsCompatibility.SUFFIX));
            moreTermsTest.createBWCIndex();
        }

    }
}
