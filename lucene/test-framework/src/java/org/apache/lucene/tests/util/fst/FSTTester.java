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
package org.apache.lucene.tests.util.fst;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.FSTCompiler;
import org.apache.lucene.util.fst.IntsRefFSTEnum;
import org.apache.lucene.util.fst.Outputs;
import org.apache.lucene.util.fst.Util;

/** Helper class to test FSTs. */
public class FSTTester<T> {
  public final Random random;
  public final List<InputOutput<T>> pairs;
  public final int inputMode;
  public final Outputs<T> outputs;
  public final Directory dir;
  public long nodeCount;
  public long arcCount;

  public FSTTester(
      Random random, Directory dir, int inputMode, List<InputOutput<T>> pairs, Outputs<T> outputs) {
    this.random = random;
    this.dir = dir;
    this.inputMode = inputMode;
    this.pairs = pairs;
    this.outputs = outputs;
  }

  static String inputToString(int inputMode, IntsRef term) {
    return inputToString(inputMode, term, true);
  }

  static String inputToString(int inputMode, IntsRef term, boolean isValidUnicode) {
    if (!isValidUnicode) {
      return term.toString();
    } else if (inputMode == 0) {
      // utf8
      return toBytesRef(term).utf8ToString() + " " + term;
    } else {
      // utf32
      return UnicodeUtil.newString(term.ints, term.offset, term.length) + " " + term;
    }
  }

  private static BytesRef toBytesRef(IntsRef ir) {
    BytesRef br = new BytesRef(ir.length);
    for (int i = 0; i < ir.length; i++) {
      int x = ir.ints[ir.offset + i];
      assert x >= 0 && x <= 255;
      br.bytes[i] = (byte) x;
    }
    br.length = ir.length;
    return br;
  }

  /**
   * [LUCENE-9600] This was made public because a misc module test depends on it. It is not
   * recommended for generic usecase; consider {@link
   * com.carrotsearch.randomizedtesting.generators.RandomStrings} to generate random strings.
   */
  public static String getRandomString(Random random) {
    final String term;
    if (random.nextBoolean()) {
      term = TestUtil.randomRealisticUnicodeString(random);
    } else {
      // we want to mix in limited-alphabet symbols so
      // we get more sharing of the nodes given how few
      // terms we are testing...
      term = simpleRandomString(random);
    }
    return term;
  }

  public static String simpleRandomString(Random r) {
    final int end = r.nextInt(10);
    if (end == 0) {
      // allow 0 length
      return "";
    }
    final char[] buffer = new char[end];
    for (int i = 0; i < end; i++) {
      buffer[i] = (char) TestUtil.nextInt(r, 97, 102);
    }
    return new String(buffer, 0, end);
  }

  public static IntsRef toIntsRef(String s, int inputMode) {
    return toIntsRef(s, inputMode, new IntsRefBuilder());
  }

  public static IntsRef toIntsRef(String s, int inputMode, IntsRefBuilder ir) {
    if (inputMode == 0) {
      // utf8
      return toIntsRef(new BytesRef(s), ir);
    } else {
      // utf32
      return toIntsRefUTF32(s, ir);
    }
  }

  static IntsRef toIntsRefUTF32(String s, IntsRefBuilder ir) {
    final int charLength = s.length();
    int charIdx = 0;
    int intIdx = 0;
    ir.clear();
    while (charIdx < charLength) {
      ir.grow(intIdx + 1);
      final int utf32 = s.codePointAt(charIdx);
      ir.append(utf32);
      charIdx += Character.charCount(utf32);
      intIdx++;
    }
    return ir.get();
  }

  static IntsRef toIntsRef(BytesRef br, IntsRefBuilder ir) {
    ir.growNoCopy(br.length);
    ir.clear();
    for (int i = 0; i < br.length; i++) {
      ir.append(br.bytes[br.offset + i] & 0xFF);
    }
    return ir.get();
  }

  /** Holds one input/output pair. */
  public static class InputOutput<T> implements Comparable<InputOutput<T>> {
    public final IntsRef input;
    public final T output;

    public InputOutput(IntsRef input, T output) {
      this.input = input;
      this.output = output;
    }

    @Override
    public int compareTo(InputOutput<T> other) {
      return input.compareTo(other.input);
    }
  }

  // runs the term, returning the output, or null if term
  // isn't accepted.  if prefixLength is non-null it must be
  // length 1 int array; prefixLength[0] is set to the length
  // of the term prefix that matches
  private T run(FST<T> fst, IntsRef term, int[] prefixLength) throws IOException {
    assert prefixLength == null || prefixLength.length == 1;
    final FST.Arc<T> arc = fst.getFirstArc(new FST.Arc<>());
    T output = fst.outputs.getNoOutput();
    final FST.BytesReader fstReader = fst.getBytesReader();

    for (int i = 0; i <= term.length; i++) {
      final int label;
      if (i == term.length) {
        label = FST.END_LABEL;
      } else {
        label = term.ints[term.offset + i];
      }
      // System.out.println("   loop i=" + i + " label=" + label + " output=" +
      // fst.outputs.outputToString(output) + " curArc: target=" + arc.target + " isFinal?=" +
      // arc.isFinal());
      if (fst.findTargetArc(label, arc, arc, fstReader) == null) {
        // System.out.println("    not found");
        if (prefixLength != null) {
          prefixLength[0] = i;
          return output;
        } else {
          return null;
        }
      }
      output = fst.outputs.add(output, arc.output());
    }

    if (prefixLength != null) {
      prefixLength[0] = term.length;
    }

    return output;
  }

  private T randomAcceptedWord(FST<T> fst, IntsRefBuilder in) throws IOException {
    FST.Arc<T> arc = fst.getFirstArc(new FST.Arc<>());

    final List<FST.Arc<T>> arcs = new ArrayList<>();
    in.clear();
    T output = fst.outputs.getNoOutput();
    final FST.BytesReader fstReader = fst.getBytesReader();

    while (true) {
      // read all arcs:
      fst.readFirstTargetArc(arc, arc, fstReader);
      arcs.add(new FST.Arc<T>().copyFrom(arc));
      while (!arc.isLast()) {
        fst.readNextArc(arc, fstReader);
        arcs.add(new FST.Arc<T>().copyFrom(arc));
      }

      // pick one
      arc = arcs.get(random.nextInt(arcs.size()));
      arcs.clear();

      // accumulate output
      output = fst.outputs.add(output, arc.output());

      // append label
      if (arc.label() == FST.END_LABEL) {
        break;
      }

      in.append(arc.label());
    }

    return output;
  }

  public FST<T> doTest() throws IOException {

    final FSTCompiler.Builder<T> fstCompilerBuilder =
        new FSTCompiler.Builder<>(
            inputMode == 0 ? FST.INPUT_TYPE.BYTE1 : FST.INPUT_TYPE.BYTE4, outputs);

    IndexOutput indexOutput = null;
    boolean useOffHeap = random.nextBoolean();

    if (useOffHeap) {
      indexOutput = dir.createOutput("fstOffHeap.bin", IOContext.DEFAULT);
      fstCompilerBuilder.dataOutput(indexOutput);
    }

    final FSTCompiler<T> fstCompiler = fstCompilerBuilder.build();

    for (InputOutput<T> pair : pairs) {
      if (pair.output instanceof List) {
        @SuppressWarnings("unchecked")
        List<Long> longValues = (List<Long>) pair.output;
        @SuppressWarnings("unchecked")
        final FSTCompiler<Object> fstCompilerObject = (FSTCompiler<Object>) fstCompiler;
        for (Long value : longValues) {
          fstCompilerObject.add(pair.input, value);
        }
      } else {
        fstCompiler.add(pair.input, pair.output);
      }
    }

    FST<T> fst = null;
    FST.FSTMetadata<T> fstMetadata = fstCompiler.compile();

    if (useOffHeap) {
      indexOutput.close();
      if (fstMetadata == null) {
        dir.deleteFile("fstOffHeap.bin");
      } else {
        try (IndexInput in = dir.openInput("fstOffHeap.bin", IOContext.DEFAULT)) {
          fst = new FST<>(fstMetadata, in);
        } finally {
          dir.deleteFile("fstOffHeap.bin");
        }
      }
    } else if (fstMetadata != null) {
      fst = FST.fromFSTReader(fstMetadata, fstCompiler.getFSTReader());
      if (random.nextBoolean()) {
        IOContext context = LuceneTestCase.newIOContext(random);
        try (IndexOutput out = dir.createOutput("fst.bin", context)) {
          fst.save(out, out);
        }
        try (IndexInput in = dir.openInput("fst.bin", context)) {
          fst = new FST<>(FST.readMetadata(in, outputs), in);
        } finally {
          dir.deleteFile("fst.bin");
        }
      }
    }

    if (LuceneTestCase.VERBOSE && pairs.size() <= 20 && fst != null) {
      System.out.println("Printing FST as dot file to stdout:");
      final Writer w = new OutputStreamWriter(System.out, Charset.defaultCharset());
      Util.toDot(fst, w, false, false);
      w.flush();
      System.out.println("END dot file");
    }

    if (LuceneTestCase.VERBOSE) {
      if (fst == null) {
        System.out.println("  fst has 0 nodes (fully pruned)");
      } else {
        System.out.println(
            "  fst has "
                + fstCompiler.getNodeCount()
                + " nodes and "
                + fstCompiler.getArcCount()
                + " arcs");
      }
    }

    verifyUnPruned(inputMode, fst);

    nodeCount = fstCompiler.getNodeCount();
    arcCount = fstCompiler.getArcCount();

    return fst;
  }

  protected boolean outputsEqual(T a, T b) {
    return a.equals(b);
  }

  // FST is complete
  private void verifyUnPruned(int inputMode, FST<T> fst) throws IOException {

    if (pairs.size() == 0) {
      assertNull(fst);
      return;
    }

    if (LuceneTestCase.VERBOSE) {
      System.out.println("TEST: now verify " + pairs.size() + " terms");
      for (InputOutput<T> pair : pairs) {
        assertNotNull(pair);
        assertNotNull(pair.input);
        assertNotNull(pair.output);
        System.out.println(
            "  "
                + inputToString(inputMode, pair.input)
                + ": "
                + outputs.outputToString(pair.output));
      }
    }

    assertNotNull(fst);

    // visit valid pairs in order -- make sure all words
    // are accepted, and FSTEnum's next() steps through
    // them correctly
    if (LuceneTestCase.VERBOSE) {
      System.out.println("TEST: check valid terms/next()");
    }
    {
      IntsRefFSTEnum<T> fstEnum = new IntsRefFSTEnum<>(fst);
      for (InputOutput<T> pair : pairs) {
        IntsRef term = pair.input;
        if (LuceneTestCase.VERBOSE) {
          System.out.println(
              "TEST: check term="
                  + inputToString(inputMode, term)
                  + " output="
                  + fst.outputs.outputToString(pair.output));
        }
        T output = run(fst, term, null);
        assertNotNull("term " + inputToString(inputMode, term) + " is not accepted", output);
        assertTrue(outputsEqual(pair.output, output));

        // verify enum's next
        IntsRefFSTEnum.InputOutput<T> t = fstEnum.next();
        assertNotNull(t);
        assertEquals(
            "expected input="
                + inputToString(inputMode, term)
                + " but fstEnum returned "
                + inputToString(inputMode, t.input),
            term,
            t.input);
        assertTrue(outputsEqual(pair.output, t.output));
      }
      assertNull(fstEnum.next());
    }

    final Map<IntsRef, T> termsMap = new HashMap<>();
    for (InputOutput<T> pair : pairs) {
      termsMap.put(pair.input, pair.output);
    }

    // find random matching word and make sure it's valid
    if (LuceneTestCase.VERBOSE) {
      System.out.println("TEST: verify random accepted terms");
    }
    final IntsRefBuilder scratch = new IntsRefBuilder();
    int num = LuceneTestCase.atLeast(random, 500);
    for (int iter = 0; iter < num; iter++) {
      T output = randomAcceptedWord(fst, scratch);
      assertTrue(
          "accepted word " + inputToString(inputMode, scratch.get()) + " is not valid",
          termsMap.containsKey(scratch.get()));
      assertTrue(outputsEqual(termsMap.get(scratch.get()), output));
    }

    // test IntsRefFSTEnum.seek:
    if (LuceneTestCase.VERBOSE) {
      System.out.println("TEST: verify seek");
    }
    IntsRefFSTEnum<T> fstEnum = new IntsRefFSTEnum<>(fst);
    num = LuceneTestCase.atLeast(random, 100);
    for (int iter = 0; iter < num; iter++) {
      if (LuceneTestCase.VERBOSE) {
        System.out.println("  iter=" + iter);
      }
      if (random.nextBoolean()) {
        // seek to term that doesn't exist:
        while (true) {
          final IntsRef term = toIntsRef(getRandomString(random), inputMode);
          int pos = Collections.binarySearch(pairs, new InputOutput<>(term, null));
          if (pos < 0) {
            pos = -(pos + 1);
            // ok doesn't exist
            // System.out.println("  seek " + inputToString(inputMode, term));
            final IntsRefFSTEnum.InputOutput<T> seekResult;
            if (random.nextInt(3) == 0) {
              if (LuceneTestCase.VERBOSE) {
                System.out.println(
                    "  do non-exist seekExact term=" + inputToString(inputMode, term));
              }
              seekResult = fstEnum.seekExact(term);
              pos = -1;
            } else if (random.nextBoolean()) {
              if (LuceneTestCase.VERBOSE) {
                System.out.println(
                    "  do non-exist seekFloor term=" + inputToString(inputMode, term));
              }
              seekResult = fstEnum.seekFloor(term);
              pos--;
            } else {
              if (LuceneTestCase.VERBOSE) {
                System.out.println(
                    "  do non-exist seekCeil term=" + inputToString(inputMode, term));
              }
              seekResult = fstEnum.seekCeil(term);
            }

            if (pos != -1 && pos < pairs.size()) {
              // System.out.println("    got " + inputToString(inputMode,seekResult.input) + "
              // output=" + fst.outputs.outputToString(seekResult.output));
              assertNotNull(
                  "got null but expected term=" + inputToString(inputMode, pairs.get(pos).input),
                  seekResult);
              if (LuceneTestCase.VERBOSE) {
                System.out.println("    got " + inputToString(inputMode, seekResult.input));
              }
              assertEquals(
                  "expected "
                      + inputToString(inputMode, pairs.get(pos).input)
                      + " but got "
                      + inputToString(inputMode, seekResult.input),
                  pairs.get(pos).input,
                  seekResult.input);
              assertTrue(outputsEqual(pairs.get(pos).output, seekResult.output));
            } else {
              // seeked before start or beyond end
              // System.out.println("seek=" + seekTerm);
              assertNull(
                  "expected null but got "
                      + (seekResult == null ? "null" : inputToString(inputMode, seekResult.input)),
                  seekResult);
              if (LuceneTestCase.VERBOSE) {
                System.out.println("    got null");
              }
            }

            break;
          }
        }
      } else {
        // seek to term that does exist:
        InputOutput<T> pair = pairs.get(random.nextInt(pairs.size()));
        final IntsRefFSTEnum.InputOutput<T> seekResult;
        if (random.nextInt(3) == 2) {
          if (LuceneTestCase.VERBOSE) {
            System.out.println(
                "  do exists seekExact term=" + inputToString(inputMode, pair.input));
          }
          seekResult = fstEnum.seekExact(pair.input);
        } else if (random.nextBoolean()) {
          if (LuceneTestCase.VERBOSE) {
            System.out.println("  do exists seekFloor " + inputToString(inputMode, pair.input));
          }
          seekResult = fstEnum.seekFloor(pair.input);
        } else {
          if (LuceneTestCase.VERBOSE) {
            System.out.println("  do exists seekCeil " + inputToString(inputMode, pair.input));
          }
          seekResult = fstEnum.seekCeil(pair.input);
        }
        assertNotNull(seekResult);
        assertEquals(
            "got "
                + inputToString(inputMode, seekResult.input)
                + " but expected "
                + inputToString(inputMode, pair.input),
            pair.input,
            seekResult.input);
        assertTrue(outputsEqual(pair.output, seekResult.output));
      }
    }

    if (LuceneTestCase.VERBOSE) {
      System.out.println("TEST: mixed next/seek");
    }

    // test mixed next/seek
    num = LuceneTestCase.atLeast(random, 100);
    for (int iter = 0; iter < num; iter++) {
      if (LuceneTestCase.VERBOSE) {
        System.out.println("TEST: iter " + iter);
      }
      // reset:
      fstEnum = new IntsRefFSTEnum<>(fst);
      int upto = -1;
      while (true) {
        boolean isDone = false;
        if (upto == pairs.size() - 1 || random.nextBoolean()) {
          // next
          upto++;
          if (LuceneTestCase.VERBOSE) {
            System.out.println("  do next");
          }
          isDone = fstEnum.next() == null;
        } else if (upto != -1 && upto < 0.75 * pairs.size() && random.nextBoolean()) {
          int attempt = 0;
          for (; attempt < 10; attempt++) {
            IntsRef term = toIntsRef(getRandomString(random), inputMode);
            if (!termsMap.containsKey(term) && term.compareTo(pairs.get(upto).input) > 0) {
              int pos = Collections.binarySearch(pairs, new InputOutput<>(term, null));
              assert pos < 0;
              upto = -(pos + 1);

              if (random.nextBoolean()) {
                upto--;
                assertTrue(upto != -1);
                if (LuceneTestCase.VERBOSE) {
                  System.out.println(
                      "  do non-exist seekFloor(" + inputToString(inputMode, term) + ")");
                }
                isDone = fstEnum.seekFloor(term) == null;
              } else {
                if (LuceneTestCase.VERBOSE) {
                  System.out.println(
                      "  do non-exist seekCeil(" + inputToString(inputMode, term) + ")");
                }
                isDone = fstEnum.seekCeil(term) == null;
              }

              break;
            }
          }
          if (attempt == 10) {
            continue;
          }

        } else {
          final int inc = random.nextInt(pairs.size() - upto - 1);
          upto += inc;
          if (upto == -1) {
            upto = 0;
          }

          if (random.nextBoolean()) {
            if (LuceneTestCase.VERBOSE) {
              System.out.println(
                  "  do seekCeil(" + inputToString(inputMode, pairs.get(upto).input) + ")");
            }
            isDone = fstEnum.seekCeil(pairs.get(upto).input) == null;
          } else {
            if (LuceneTestCase.VERBOSE) {
              System.out.println(
                  "  do seekFloor(" + inputToString(inputMode, pairs.get(upto).input) + ")");
            }
            isDone = fstEnum.seekFloor(pairs.get(upto).input) == null;
          }
        }
        if (LuceneTestCase.VERBOSE) {
          if (!isDone) {
            System.out.println("    got " + inputToString(inputMode, fstEnum.current().input));
          } else {
            System.out.println("    got null");
          }
        }

        if (upto == pairs.size()) {
          assertTrue(isDone);
          break;
        } else {
          assertFalse(isDone);
          assertEquals(pairs.get(upto).input, fstEnum.current().input);
          assertTrue(outputsEqual(pairs.get(upto).output, fstEnum.current().output));

          /*
            if (upto < pairs.size()-1) {
            int tryCount = 0;
            while(tryCount < 10) {
            final IntsRef t = toIntsRef(getRandomString(), inputMode);
            if (pairs.get(upto).input.compareTo(t) < 0) {
            final boolean expected = t.compareTo(pairs.get(upto+1).input) < 0;
            if (LuceneTestCase.VERBOSE) {
            System.out.println("TEST: call beforeNext(" + inputToString(inputMode, t) + "); current=" + inputToString(inputMode, pairs.get(upto).input) + " next=" + inputToString(inputMode, pairs.get(upto+1).input) + " expected=" + expected);
            }
            assertEquals(expected, fstEnum.beforeNext(t));
            break;
            }
            tryCount++;
            }
            }
          */
        }
      }
    }
  }
}
