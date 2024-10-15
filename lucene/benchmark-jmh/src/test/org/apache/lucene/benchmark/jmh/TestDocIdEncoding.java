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
package org.apache.lucene.benchmark.jmh;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.ArrayUtil;

public class TestDocIdEncoding extends LuceneTestCase {

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  public void testBPV21AndAbove() {

    List<DocIdEncodingBenchmark.DocIdEncoder> encoders =
        DocIdEncodingBenchmark.DocIdEncoder.SingletonFactory.getAllExcept(Collections.emptyList());

    final int[] scratch = new int[512];

    DocIdEncodingBenchmark.DocIdProvider docIdProvider =
        new DocIdEncodingBenchmark.FixedBPVRandomDocIdProvider();

    try {

      Path tempDir = Files.createTempDirectory("DocIdEncoding_testBPV21AndAbove_");

      for (DocIdEncodingBenchmark.DocIdEncoder encoder : encoders) {

        List<int[]> docIdSequences = docIdProvider.getDocIds(encoder.getClass(), 100, 100, 512);

        String encoderFileName = "Encoder_" + encoder.getClass().getSimpleName();

        try (Directory outDir = FSDirectory.open(tempDir);
            IndexOutput out = outDir.createOutput(encoderFileName, IOContext.DEFAULT)) {
          for (int[] sequence : docIdSequences) {
            encoder.encode(out, 0, sequence.length, sequence);
          }
        }

        try (Directory inDir = FSDirectory.open(tempDir);
            IndexInput in = inDir.openInput(encoderFileName, IOContext.DEFAULT)) {
          for (int[] sequence : docIdSequences) {
            encoder.decode(in, 0, sequence.length, scratch);
            assertArrayEquals(sequence, ArrayUtil.copyOfSubArray(scratch, 0, sequence.length));
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
