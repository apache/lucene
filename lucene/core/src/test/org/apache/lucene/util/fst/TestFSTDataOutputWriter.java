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
package org.apache.lucene.util.fst;

import static org.apache.lucene.tests.util.fst.FSTTester.toIntsRef;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.tests.store.MockDirectoryWrapper;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.tests.util.fst.FSTTester;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;

public class TestFSTDataOutputWriter extends LuceneTestCase {

  private MockDirectoryWrapper dir;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir = newMockDirectory();
  }

  @Override
  public void tearDown() throws Exception {
    // can be null if we force simpletext (funky, some kind of bug in test runner maybe)
    if (dir != null) {
      dir.close();
    }
    super.tearDown();
  }

  public void testRandom() throws Exception {

    final int iters = atLeast(10);
    final int maxBytes = TEST_NIGHTLY ? 200000 : 20000;
    for (int iter = 0; iter < iters; iter++) {
      final int numBytes = TestUtil.nextInt(random(), 1, maxBytes);
      final byte[] expected = new byte[numBytes];
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      final DataOutput dataOutput = new OutputStreamDataOutput(baos);
      if (VERBOSE) {
        System.out.println("TEST: iter=" + iter + " numBytes=" + numBytes);
      }

      int pos = 0;
      while (pos < numBytes) {
        int op = random().nextInt(2);
        if (VERBOSE) {
          System.out.println("  cycle pos=" + pos);
        }
        switch (op) {
          case 0:
            {
              // write random byte
              byte b = (byte) random().nextInt(256);
              if (VERBOSE) {
                System.out.println("    writeByte b=" + b);
              }

              expected[pos++] = b;
              dataOutput.writeByte(b);
            }
            break;

          case 1:
            {
              // write random byte[]
              int len = random().nextInt(Math.min(numBytes - pos, 100));
              byte[] temp = new byte[len];
              random().nextBytes(temp);
              if (VERBOSE) {
                System.out.println("    writeBytes len=" + len + " bytes=" + Arrays.toString(temp));
              }
              System.arraycopy(temp, 0, expected, pos, temp.length);
              dataOutput.writeBytes(temp, 0, temp.length);
              pos += len;
            }
            break;
        }

        assertEquals(pos, baos.toByteArray().length);
      }
      for (int i = 0; i < numBytes; i++) {
        assertEquals("byte @ index=" + i, expected[i], baos.toByteArray()[i]);
      }
    }
  }

  public void testBasicFSA() throws IOException {
    String[] strings2 =
        new String[] {
          "station", "commotion", "elation", "elastic", "plastic", "stop", "ftop", "ftation"
        };
    IntsRef[] terms2 = new IntsRef[strings2.length];
    // we will also test writing multiple FST to a single byte array
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    for (int inputMode = 0; inputMode < 2; inputMode++) {
      if (VERBOSE) {
        System.out.println("TEST: inputMode=" + inputModeToString(inputMode));
      }

      for (int idx = 0; idx < strings2.length; idx++) {
        terms2[idx] = toIntsRef(strings2[idx], inputMode);
      }
      Arrays.sort(terms2);

      // Test pre-determined FST sizes to make sure we haven't lost minimality (at least on this
      // trivial set of terms):

      // FSA
      {
        final Outputs<Object> outputs = NoOutputs.getSingleton();
        final Object NO_OUTPUT = outputs.getNoOutput();
        final List<FSTTester.InputOutput<Object>> pairs = new ArrayList<>(terms2.length);
        for (IntsRef term : terms2) {
          pairs.add(new FSTTester.InputOutput<>(term, NO_OUTPUT));
        }
        FSTTester<Object> tester =
            new DataOutputFSTTester<>(random(), dir, inputMode, pairs, outputs, baos);
        FST<Object> fst = tester.doTest();
        assertNotNull(fst);
        assertEquals(22, tester.nodeCount);
        assertEquals(27, tester.arcCount);
      }

      // FST ord pos int
      {
        final PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton();
        final List<FSTTester.InputOutput<Long>> pairs = new ArrayList<>(terms2.length);
        for (int idx = 0; idx < terms2.length; idx++) {
          pairs.add(new FSTTester.InputOutput<>(terms2[idx], (long) idx));
        }
        FSTTester<Long> tester =
            new DataOutputFSTTester<>(random(), dir, inputMode, pairs, outputs, baos);
        final FST<Long> fst = tester.doTest();
        assertNotNull(fst);
        assertEquals(22, tester.nodeCount);
        assertEquals(27, tester.arcCount);
      }

      // FST byte sequence ord
      {
        final ByteSequenceOutputs outputs = ByteSequenceOutputs.getSingleton();
        final List<FSTTester.InputOutput<BytesRef>> pairs = new ArrayList<>(terms2.length);
        for (int idx = 0; idx < terms2.length; idx++) {
          final BytesRef output = newBytesRef(Integer.toString(idx));
          pairs.add(new FSTTester.InputOutput<>(terms2[idx], output));
        }
        FSTTester<BytesRef> tester =
            new DataOutputFSTTester<>(random(), dir, inputMode, pairs, outputs, baos);
        final FST<BytesRef> fst = tester.doTest();
        assertNotNull(fst);
        assertEquals(24, tester.nodeCount);
        assertEquals(30, tester.arcCount);
      }
    }
  }

  class DataOutputFSTTester<T> extends FSTTester<T> {

    private final ByteArrayOutputStream baos;
    private int previousOffset;

    public DataOutputFSTTester(
        Random random,
        Directory dir,
        int inputMode,
        List<InputOutput<T>> pairs,
        Outputs<T> outputs,
        ByteArrayOutputStream baos) {
      super(random, dir, inputMode, pairs, outputs);
      this.baos = baos;
    }

    @Override
    protected FSTCompiler.Builder<T> getFSTBuilder() {
      // as the byte array could already contain another FST bytes, we should get the current offset
      // to know where to start reading from
      this.previousOffset = baos.size();
      return super.getFSTBuilder().dataOutput(new OutputStreamDataOutput(baos));
    }

    @Override
    protected FST<T> compile(FSTCompiler<T> fstCompiler) throws IOException {
      FST<T> fst = fstCompiler.compile();
      assertFalse(fst.hasReverseBytesReader());

      // the returned FST is not readable thus we need to reconstruct one with FSTStore
      DataInput dataIn =
          new InputStreamDataInput(
              new ByteArrayInputStream(
                  baos.toByteArray(), previousOffset, baos.size() - previousOffset));
      return new FST<>(fst.getMetadata(), dataIn, outputs, new OnHeapFSTStore(5));
    }
  }

  String inputModeToString(int mode) {
    if (mode == 0) {
      return "utf8";
    } else {
      return "utf32";
    }
  }
}
