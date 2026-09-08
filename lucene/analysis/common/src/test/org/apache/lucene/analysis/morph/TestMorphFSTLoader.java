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
package org.apache.lucene.analysis.morph;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.FSTCompiler;
import org.apache.lucene.util.fst.IntsRefFSTEnum;
import org.apache.lucene.util.fst.PositiveIntOutputs;

public class TestMorphFSTLoader extends LuceneTestCase {

  private static final long MAX_SHALLOW_FST_RAM = 10_000L;

  public void testLoadFromPathMmapsWithoutHeapCopy() throws Exception {
    Path fstFile = writeSampleFstToPath();
    try (MorphFSTLoader.LoadedFST loaded = MorphFSTLoader.loadFromPath(fstFile)) {
      assertNotNull(loaded.resource());
      assertTrue(loaded.fst().ramBytesUsed() < MAX_SHALLOW_FST_RAM);
      assertEnumEquals(buildSampleFst(), loaded.fst());
    }
  }

  public void testLoadFromStreamCopiesOffHeap() throws Exception {
    FST<Long> expected = buildSampleFst();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    expected.save(new OutputStreamDataOutput(bos), new OutputStreamDataOutput(bos));
    FST<Long> loaded =
        MorphFSTLoader.loadFromStream(new java.io.ByteArrayInputStream(bos.toByteArray()));
    assertTrue(loaded.ramBytesUsed() < MAX_SHALLOW_FST_RAM);
    assertEnumEquals(expected, loaded);
  }

  public void testLoadCompiledCopiesOffHeap() throws Exception {
    PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton();
    FSTCompiler<Long> compiler =
        new FSTCompiler.Builder<>(FST.INPUT_TYPE.BYTE1, outputs).build();
    IntsRefBuilder scratch = new IntsRefBuilder();
    scratch.append(42);
    compiler.add(scratch.get(), 7L);
    FST.FSTMetadata<Long> metadata = compiler.compile();
    FST<Long> loaded = MorphFSTLoader.loadCompiled(metadata, compiler.getFSTReader());
    assertTrue(loaded.ramBytesUsed() < MAX_SHALLOW_FST_RAM);
  }

  private static Path writeSampleFstToPath() throws IOException {
    FST<Long> fst = buildSampleFst();
    Path path = createTempFile("morph-fst", ".dat");
    fst.save(path);
    return path;
  }

  private static FST<Long> buildSampleFst() throws IOException {
    PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton();
    FSTCompiler<Long> compiler =
        new FSTCompiler.Builder<>(FST.INPUT_TYPE.BYTE1, outputs).build();
    IntsRefBuilder scratch = new IntsRefBuilder();
    for (int i = 0; i < 64; i++) {
      scratch.clear();
      scratch.append(i);
      compiler.add(scratch.get(), (long) i);
    }
    return FST.fromFSTReader(compiler.compile(), compiler.getFSTReader());
  }

  private static void assertEnumEquals(FST<Long> expected, FST<Long> actual) throws IOException {
    IntsRefFSTEnum<Long> expectedEnum = new IntsRefFSTEnum<>(expected);
    IntsRefFSTEnum<Long> actualEnum = new IntsRefFSTEnum<>(actual);
    IntsRefFSTEnum.InputOutput<Long> e;
    while ((e = expectedEnum.next()) != null) {
      IntsRefFSTEnum.InputOutput<Long> a = actualEnum.next();
      assertNotNull(a);
      assertEquals(e.input, a.input);
      assertEquals(e.output, a.output);
    }
    assertNull(actualEnum.next());
  }
}
