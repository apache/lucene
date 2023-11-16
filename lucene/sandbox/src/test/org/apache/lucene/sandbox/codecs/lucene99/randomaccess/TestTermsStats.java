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

package org.apache.lucene.sandbox.codecs.lucene99.randomaccess;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;


public class TestTermsStats extends LuceneTestCase {

    public void testRoundTrip() throws IOException {
        TermsStats expected = makeRandom();

        try (Directory dir = newDirectory()) {
            IndexOutput output = dir.createOutput("terms_stats", IOContext.DEFAULT);
            expected.serialize(output);
            output.close();

            IndexInput input = dir.openInput("terms_stats", IOContext.DEFAULT);
            TermsStats actual = TermsStats.deserialize(input);

            assertEquals(expected, actual);
            input.close();
        }
    }

    private TermsStats makeRandom() {
        byte[] minBytes = getRandomBytes();
        byte[] maxBytes = getRandomBytes();
        return new TermsStats(
                random().nextLong(1, Long.MAX_VALUE),
                random().nextLong(1, Long.MAX_VALUE),
                random().nextLong(1, Long.MAX_VALUE),
                random().nextInt(1, Integer.MAX_VALUE),
                new BytesRef(minBytes),
                new BytesRef(maxBytes)
        );
    }

    private static byte[] getRandomBytes() {
        byte[] minBytes = new byte[random().nextInt(100)];
        random().nextBytes(minBytes);
        return minBytes;
    }
}