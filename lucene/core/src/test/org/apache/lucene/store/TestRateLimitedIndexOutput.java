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
package org.apache.lucene.store;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeRateLimiter;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class TestRateLimitedIndexOutput  extends LuceneTestCase {

    public void testCheckInstanthighRate ()  throws Exception {
        try (Directory dir = newDirectory()) {
            try (IndexOutput in = dir.createOutput("RateLimitedIndexOutputTest", IOContext.DEFAULT)) {
                final List<SegmentCommitInfo> segments = new LinkedList<SegmentCommitInfo>();
                SegmentInfo si =
                        new SegmentInfo(
                                dir,
                                Version.LATEST,
                                Version.LATEST,
                                "test",
                                10,
                                false,
                                Codec.getDefault(),
                                Collections.emptyMap(),
                                StringHelper.randomId(),
                                new HashMap<>(),
                                null);
                segments.add(new SegmentCommitInfo(si, 0, 0, 0, 0, 0, StringHelper.randomId()));

                MergePolicy.OneMerge oneMerge = new MergePolicy.OneMerge(segments);
                oneMerge.mergeInit();

                MergeRateLimiter rateLimiter = new MergeRateLimiter(oneMerge.getMergeProgress());
                rateLimiter.setMBPerSec(0.0001);
                RateLimitedIndexOutput rateLimitedIndexOutput = new RateLimitedIndexOutput(rateLimiter, in);
                byte[] bytes = new byte[]{1,2,3};
                assertTrue(bytes.length > rateLimiter.getMinPauseCheckBytes());
                rateLimitedIndexOutput.writeBytes(bytes, 0, bytes.length);
                Thread.sleep(100);

                bytes = new byte[]{1,2,3,4};
                assertTrue(bytes.length > rateLimiter.getMinPauseCheckBytes());
                long start = System.nanoTime();
                rateLimitedIndexOutput.writeBytes(bytes, 0, bytes.length);
                long end = System.nanoTime() - start;
                double pauseTimes = bytes.length/1024./1024./rateLimiter.getMBPerSec()*1000000000;
                assertTrue(end > pauseTimes);
            }
        }
    }
}
