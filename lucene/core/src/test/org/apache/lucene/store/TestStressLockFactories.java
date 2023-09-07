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

import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.SuppressForbidden;

@LuceneTestCase.SuppressFileSystems("*")
public class TestStressLockFactories extends LuceneTestCase {

  @SuppressForbidden(reason = "ProcessBuilder only allows to redirect to java.io.File")
  private static final ProcessBuilder applyRedirection(ProcessBuilder pb, int client, Path dir) {
    if (VERBOSE) {
      return pb.inheritIO();
    } else {
      return pb.redirectError(dir.resolve("err-" + client + ".txt").toFile())
          .redirectOutput(dir.resolve("out-" + client + ".txt").toFile())
          .redirectInput(Redirect.INHERIT);
    }
  }

  private String readIfExists(Path path) throws IOException {
    if (Files.exists(path)) {
      return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    } else {
      return null;
    }
  }

  private String readClientStdout(Path dir, int client) throws IOException {
    return readIfExists(dir.resolve("out-" + client + ".txt"));
  }

  private String readClientStderr(Path dir, int client) throws IOException {
    return readIfExists(dir.resolve("err-" + client + ".txt"));
  }

  private void runImpl(Class<? extends LockFactory> impl) throws Exception {
    final int clients = TEST_NIGHTLY ? 5 : 2;
    final String host = "127.0.0.1";
    final int delay = 1;
    final int rounds = (TEST_NIGHTLY ? 30000 : 500) * RANDOM_MULTIPLIER;

    final Path dir = createTempDir(impl.getSimpleName());

    final List<Process> processes = new ArrayList<>(clients);

    try {
      LockVerifyServer.run(
          host,
          clients,
          addr -> {
            // spawn clients as separate Java processes
            for (int i = 0; i < clients; i++) {
              try {
                List<String> args = new ArrayList<>();
                args.add(Paths.get(System.getProperty("java.home"), "bin", "java").toString());
                args.addAll(getJvmForkArguments());
                args.addAll(
                    List.of(
                        "-Xmx32M",
                        LockStressTest.class.getName(),
                        Integer.toString(i),
                        addr.getHostString(),
                        Integer.toString(addr.getPort()),
                        impl.getName(),
                        dir.toString(),
                        Integer.toString(delay),
                        Integer.toString(rounds)));

                processes.add(applyRedirection(new ProcessBuilder(args), i, dir).start());
              } catch (IOException ioe) {
                throw new AssertionError("Failed to start child process.", ioe);
              }
            }
          });
    } catch (Exception e) {
      System.err.println("Server failed");
      int client = 0;
      for (Process p : processes) {
        System.err.println("stderr for " + p.pid() + ":\n" + readClientStderr(dir, client));
        System.err.println("stdout for " + p.pid() + ":\n" + readClientStdout(dir, client));
        client++;
      }

      throw e;
    }

    // wait for all processes to exit...
    try {
      int client = 0;
      for (Process p : processes) {
        int clientTimeoutSeconds = 15;

        boolean doFail = false;
        String reason = null;

        if (p.waitFor(clientTimeoutSeconds, TimeUnit.SECONDS)) {
          // process finished within our 15 second wait period; get its exit code
          int exitCode = p.waitFor();
          if (exitCode != 0) {
            doFail = true;
            reason =
                "Process "
                    + p.pid()
                    + " (client "
                    + client
                    + ") exited abnormally: exit code "
                    + exitCode;
          }
        } else {
          doFail = true;
          reason =
              "Process "
                  + p.pid()
                  + " (client "
                  + client
                  + ") did not finish within "
                  + clientTimeoutSeconds
                  + " second timeout";
        }

        if (doFail) {
          System.err.println(reason);
          System.err.println("stderr for " + p.pid() + ":\n" + readClientStderr(dir, client));
          System.err.println("stdout for " + p.pid() + ":\n" + readClientStdout(dir, client));
          fail(reason);
        }

        client++;
      }
    } finally {
      // kill all processes, which are still alive.
      for (Process p : processes) {
        if (p.isAlive()) {
          p.destroyForcibly().waitFor();
        }
      }
    }
  }

  public void testNativeFSLockFactory() throws Exception {
    runImpl(NativeFSLockFactory.class);
  }

  public void testSimpleFSLockFactory() throws Exception {
    runImpl(SimpleFSLockFactory.class);
  }
}
