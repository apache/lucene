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
package org.apache.lucene.index;

import com.carrotsearch.randomizedtesting.LifecycleScope;
import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.internal.tests.TestSecrets;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.lucene.util.SuppressForbidden;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

/* WARNING: This test does *not* extend LuceneTestCase to prevent static class
 * initialization when spawned as subprocess (and please let default codecs alive)! */

@RunWith(RandomizedRunner.class)
public class TestClassloadingDeadlock extends Assert {
  private static final int MAX_TIME_SECONDS = 30;

  @Test
  @SuppressForbidden(reason = "Uses Path.toFile because ProcessBuilder requires it.")
  public void testDeadlock() throws Exception {
    // pick random codec names for stress test in separate process:
    final Random rnd = RandomizedContext.current().getRandom();
    Set<String> avail;
    final String codecName =
        new ArrayList<>(avail = Codec.availableCodecs()).get(rnd.nextInt(avail.size()));
    final String pfName =
        new ArrayList<>(avail = PostingsFormat.availablePostingsFormats())
            .get(rnd.nextInt(avail.size()));
    final String dvfName =
        new ArrayList<>(avail = DocValuesFormat.availableDocValuesFormats())
            .get(rnd.nextInt(avail.size()));

    System.out.printf(Locale.ROOT, "codec: %s, pf: %s, dvf: %s%n", codecName, pfName, dvfName);

    List<String> args = new ArrayList<>();
    args.add(Paths.get(System.getProperty("java.home"), "bin", "java").toString());
    args.addAll(LuceneTestCase.getJvmForkArguments());
    args.addAll(List.of(getClass().getName(), codecName, pfName, dvfName));

    // Fork a separate JVM to reinitialize classes.
    final Path output = RandomizedTest.newTempFile(LifecycleScope.TEST);
    final Process p =
        new ProcessBuilder(args).redirectErrorStream(true).redirectOutput(output.toFile()).start();
    boolean success = false;
    try {
      if (p.waitFor(MAX_TIME_SECONDS * 2, TimeUnit.SECONDS)) {
        assertEquals("Process died abnormally?", 0, p.waitFor());
        success = true;
      } else {
        p.destroyForcibly().waitFor();
        fail("Process did not exit after 60 secs?");
      }
    } finally {
      if (!success) {
        System.out.println("Subprocess emitted the following output:");
        System.out.write(Files.readAllBytes(output));
      }
    }
  }

  // This method is called in a spawned process.
  public static void main(final String... args) throws Exception {
    final String codecName = args[0];
    final String pfName = args[1];
    final String dvfName = args[2];

    final var lookup = MethodHandles.lookup();
    final List<Callable<?>> tasks =
        List.of(
            Codec::getDefault,
            () -> Codec.forName(codecName),
            () -> PostingsFormat.forName(pfName),
            () -> DocValuesFormat.forName(dvfName),
            Codec::availableCodecs,
            PostingsFormat::availablePostingsFormats,
            DocValuesFormat::availableDocValuesFormats,
            TestSecrets::getIndexWriterAccess,
            () -> lookup.ensureInitialized(IndexWriter.class),
            () -> lookup.ensureInitialized(SegmentReader.class));

    final int numTasks = tasks.size(), numThreads = numTasks * 2;
    final CopyOnWriteArrayList<Thread> allThreads = new CopyOnWriteArrayList<>();
    final ExecutorService pool =
        Executors.newFixedThreadPool(
            numThreads,
            new NamedThreadFactory("deadlockchecker") {
              @Override
              public Thread newThread(Runnable r) {
                Thread t = super.newThread(r);
                allThreads.add(t);
                return t;
              }
            });

    final CyclicBarrier barrier = new CyclicBarrier(numThreads);
    IntStream.range(0, numThreads)
        .forEach(
            taskNo ->
                pool.execute(
                    () -> {
                      try {
                        // Await a common barrier point for all threads and then
                        // run racy code. This is intentional.
                        barrier.await();
                        tasks.get(taskNo % numTasks).call();
                      } catch (Throwable t) {
                        synchronized (args) {
                          System.err.println(
                              Thread.currentThread().getName() + " failed to call racy method:");
                          t.printStackTrace(System.err);
                        }
                        Runtime.getRuntime().halt(1); // signal failure to caller
                      }
                    }));

    pool.shutdown();

    if (!pool.awaitTermination(MAX_TIME_SECONDS, TimeUnit.SECONDS)) {
      // Try to collect stacks so that we can better diagnose the failure.
      System.err.println(
          "Pool didn't return after "
              + MAX_TIME_SECONDS
              + " seconds, classloader deadlock? Dumping stack traces.");

      for (Thread t : allThreads) {
        System.err.println(
            "# Thread: "
                + t
                + ", "
                + "state: "
                + t.getState()
                + ", "
                + "stack:\n\t"
                + Arrays.stream(t.getStackTrace())
                    .map(Object::toString)
                    .collect(Collectors.joining("\t"))
                + "\n");
      }
      Runtime.getRuntime().halt(1); // signal failure to caller
    }
  }
}
