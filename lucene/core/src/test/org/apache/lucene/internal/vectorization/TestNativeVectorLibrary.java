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
package org.apache.lucene.internal.vectorization;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_INT;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.invoke.MethodHandle;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;
import java.util.function.Supplier;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.SuppressForbidden;
import org.junit.Assert;
import org.junit.BeforeClass;

/** Real library tests, enabled by {@code -Ptests.nativevector.run=true}. */
@LuceneTestCase.SuppressFileSystems("*") // Subprocesses access the real filesystem.
public class TestNativeVectorLibrary extends LuceneTestCase {
  @BeforeClass
  public static void requireTestLibraries() {
    assumeTrue(
        "Build native test libraries with -Ptests.nativevector.run=true",
        System.getProperty("tests.nativevector.libraryRoot") != null);
  }

  public void testNativeCallsAndFallback() throws Exception {
    run("prefixed", true, false, "native");
  }

  public void testUnprefixedSymbolsAreIgnored() throws Exception {
    run("unprefixed", true, false, "unprefixed");
  }

  public void testMissingOptionalLibrary() throws Exception {
    run("missing", false, false, "missing-optional");
  }

  public void testMissingRequiredLibrary() throws Exception {
    run("missing", true, false, "missing-required");
  }

  public void testStrictModeRejectsMissingSymbol() throws Exception {
    run("prefixed", true, true, "strict");
  }

  @SuppressForbidden(reason = "ProcessBuilder redirects output to a java.io.File.")
  private void run(String variant, boolean enabled, boolean strict, String scenario)
      throws Exception {
    Path libraryDir =
        variant.equals("missing")
            ? createTempDir("missing-native-library")
            : Path.of(System.getProperty("tests.nativevector.libraryRoot"), variant);
    if (!variant.equals("missing")) {
      assertTrue(Files.isRegularFile(libraryDir.resolve(System.mapLibraryName("dotProduct"))));
    }
    List<String> command = new ArrayList<>();
    command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
    command.addAll(getJvmForkArguments());
    command.addAll(
        List.of(
            "-Xmx256m",
            "-ea",
            "--add-modules=jdk.incubator.vector",
            "--enable-native-access=ALL-UNNAMED",
            "--illegal-native-access=deny",
            "-Djava.library.path=" + libraryDir,
            "-Dlucene.useNativeDotProduct=" + enabled,
            "-Dlucene.useNativeStrict=" + strict,
            Checks.class.getName(),
            scenario));
    // Loading, flags and initialization failures must be isolated from the test runner and from
    // other scenarios, since the native handles and configuration are static.
    Path output = createTempFile("native-vector", ".log");
    Process process =
        new ProcessBuilder(command)
            .redirectErrorStream(true)
            .redirectOutput(output.toFile())
            .start();
    try {
      boolean exited = process.waitFor(60, TimeUnit.SECONDS);
      if (!exited) process.destroyForcibly().waitFor();
      String diagnostic = scenario + ":\n" + Files.readString(output);
      assertTrue("Timed out: " + diagnostic, exited);
      assertEquals(diagnostic, 0, process.exitValue());
    } finally {
      process.destroyForcibly().waitFor();
      if (VERBOSE) System.out.println(Files.readString(output));
    }
  }

  /** Entry point of a fresh JVM; intentionally does not initialize LuceneTestCase. */
  @SuppressWarnings("restricted")
  public static class Checks extends Assert {
    // Keep these indices in sync with native_vectors.c.
    private static final int BYTE_DOT = 0;
    private static final int BYTE_COSINE = 1;
    private static final int FLOAT_DOT = 2;
    private static final int FLOAT_DISTANCE = 3;
    private static final int FLOAT_COSINE = 4;
    private static final int NORMALIZE = 5;
    private static final int COUNT = 6;

    public static void main(String[] args) throws Throwable {
      switch (args[0]) {
        case "missing-required", "strict" -> {
          ExceptionInInitializerError error =
              assertThrows(
                  ExceptionInInitializerError.class, NativeVectorUtilSupport::isLibraryLoaded);
          String message = error.getCause().getMessage();
          assertTrue(
              message,
              message.contains(
                  args[0].equals("strict")
                      ? "C code for lucene_squareDistance was not linked"
                      : "Native library dotProduct missing"));
        }
        case "missing-optional" -> {
          assertFalse(NativeVectorUtilSupport.isLibraryLoaded());
          VectorUtilSupport reference = new DefaultVectorUtilSupport();
          VectorUtilSupport support = new NativeVectorUtilSupport(reference);
          byte[] a = {1, -2, 3};
          byte[] b = {-4, 5, 6};
          assertEquals(reference.dotProduct(a, b), support.dotProduct(a, b));
        }
        case "native", "unprefixed" -> {
          assertTrue(NativeVectorUtilSupport.isLibraryLoaded());
          checkFunctions(args[0].equals("native"));
        }
        default -> throw new AssertionError("Unknown scenario: " + args[0]);
      }
    }

    private static void checkFunctions(boolean nativeCalls) throws Throwable {
      VectorUtilSupport reference = new DefaultVectorUtilSupport();
      VectorUtilSupport support = new NativeVectorUtilSupport(reference);
      Counters counters = new Counters(nativeCalls);
      for (int length : new int[] {0, 1, 3, 4, 7, 16, 31, 32, 33, 127, 128, 129, 257}) {
        byte[] a = new byte[length];
        byte[] b = new byte[length];
        float[] fa = new float[length];
        float[] fb = new float[length];
        for (int i = 0; i < length; i++) {
          a[i] = (byte) (i % 11 - 5);
          b[i] = (byte) (i % 7 - 3);
          fa[i] = a[i] * 0.25f;
          fb[i] = b[i] * 0.5f;
        }
        // Exercise both heap and off-heap segments, including the static entry points.
        try (Arena arena = Arena.ofConfined()) {
          for (boolean offHeap : new boolean[] {false, true}) {
            MemorySegment sa =
                offHeap ? arena.allocateFrom(JAVA_BYTE, a) : MemorySegment.ofArray(a);
            MemorySegment sb =
                offHeap ? arena.allocateFrom(JAVA_BYTE, b) : MemorySegment.ofArray(b);
            for (IntSupplier call :
                List.<IntSupplier>of(
                    () -> support.dotProduct(a, b),
                    () -> NativeVectorUtilSupport.dotProduct(a, sb),
                    () -> NativeVectorUtilSupport.dotProduct(sa, sb))) {
              counters.reset(length);
              int actual = call.getAsInt();
              counters.check(BYTE_DOT, length);
              assertEquals(reference.dotProduct(a, b), actual);
            }
            for (Supplier<Float> call :
                List.<Supplier<Float>>of(
                    () -> support.cosine(a, b),
                    () -> NativeVectorUtilSupport.cosine(a, sb),
                    () -> NativeVectorUtilSupport.cosine(sa, sb))) {
              counters.reset(length);
              float actual = call.get();
              counters.check(BYTE_COSINE, length);
              assertEquals(reference.cosine(a, b), actual, 1e-6f);
            }
          }
        }
        counters.reset(length);
        float dot = support.dotProduct(fa, fb);
        counters.check(FLOAT_DOT, length);
        assertEquals(reference.dotProduct(fa, fb), dot, 1e-5f);
        counters.reset(length);
        float distance = support.squareDistance(fa, fb);
        counters.check(FLOAT_DISTANCE, length);
        assertEquals(reference.squareDistance(fa, fb), distance, 1e-5f);
        counters.reset(length);
        float cosine = support.cosine(fa, fb);
        counters.check(FLOAT_COSINE, length);
        assertEquals(reference.cosine(fa, fb), cosine, 1e-6f);

        // This symbol is absent even in the prefixed fixture.
        counters.reset(length);
        assertEquals(reference.squareDistance(a, b), support.squareDistance(a, b));
        counters.check(-1, length);
      }
      counters.reset(3);
      float cosine = support.cosine(new byte[] {1, 0, 1}, new byte[] {1, 1, 0});
      counters.check(BYTE_COSINE, 3);
      assertEquals(0.5f, cosine, 1e-6f);

      for (float[] input : new float[][] {{3, 4}, {1, 0}, new float[5], new float[0]}) {
        for (boolean throwOnZero : new boolean[] {false, true}) {
          float[] actual = input.clone();
          float[] expected = input.clone();
          counters.reset(input.length);
          if (throwOnZero && reference.dotProduct(input, input) == 0) {
            assertThrows(IllegalArgumentException.class, () -> support.l2normalize(actual, true));
          } else {
            reference.l2normalize(expected, throwOnZero);
            assertSame(actual, support.l2normalize(actual, throwOnZero));
          }
          counters.check(NORMALIZE, input.length);
          counters.checkThrowOnZero(throwOnZero);
          assertArrayEquals(expected, actual, 1e-6f);
        }
      }
    }

    /** Observe native calls so a silently selected Java fallback cannot make the test pass. */
    private static class Counters {
      private final boolean nativeCalls;
      private final MethodHandle reset;
      private final MethodHandle calls;
      private final MethodHandle length;
      private final MethodHandle throwOnZero;

      Counters(boolean nativeCalls) {
        this.nativeCalls = nativeCalls;
        Linker linker = Linker.nativeLinker();
        SymbolLookup symbols = SymbolLookup.loaderLookup();
        reset =
            linker.downcallHandle(
                symbols.find("lucene_test_reset").orElseThrow(),
                FunctionDescriptor.ofVoid(JAVA_INT));
        calls =
            linker.downcallHandle(
                symbols.find("lucene_test_calls").orElseThrow(),
                FunctionDescriptor.of(JAVA_INT, JAVA_INT));
        length =
            linker.downcallHandle(
                symbols.find("lucene_test_length").orElseThrow(),
                FunctionDescriptor.of(JAVA_INT, JAVA_INT));
        throwOnZero =
            linker.downcallHandle(
                symbols.find("lucene_test_throw_on_zero").orElseThrow(),
                FunctionDescriptor.of(JAVA_INT));
      }

      void checkThrowOnZero(boolean expected) throws Throwable {
        assertEquals(nativeCalls ? (expected ? 1 : 0) : -1, (int) throwOnZero.invokeExact());
      }

      void reset(int expectedLength) throws Throwable {
        reset.invokeExact(expectedLength);
      }

      void check(int operation, int expectedLength) throws Throwable {
        for (int i = 0; i < COUNT; i++) {
          boolean called = nativeCalls && i == operation;
          assertEquals(
              "Native call count for operation " + i, called ? 1 : 0, (int) calls.invokeExact(i));
          if (called) {
            assertEquals(
                "Native length for operation " + i, expectedLength, (int) length.invokeExact(i));
          }
        }
      }
    }
  }
}
