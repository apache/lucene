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
package org.apache.lucene.distribution;

import com.carrotsearch.procfork.ForkedProcess;
import com.carrotsearch.procfork.Launcher;
import com.carrotsearch.procfork.ProcessBuilderLauncher;
import com.carrotsearch.randomizedtesting.LifecycleScope;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ThrowingConsumer;
import org.junit.Test;

/** Verify that scripts included in the distribution work. */
public class TestScripts extends AbstractLuceneDistributionTest {
  @Test
  @RequiresGUI
  public void testLukeCanBeLaunched() throws Exception {
    Path distributionPath;
    if (randomBoolean()) {
      // Occasionally, be evil: put the distribution in a folder with a space inside. For Uwe.
      distributionPath = RandomizedTest.newTempDir(LifecycleScope.TEST).resolve("uh oh");
      Files.createDirectory(distributionPath);
      new Sync().sync(getDistributionPath(), distributionPath);
    } else {
      distributionPath = getDistributionPath();
    }

    Path currentJava =
        Paths.get(System.getProperty("java.home"), "bin", WINDOWS ? "java.exe" : "java");
    Assertions.assertThat(currentJava).exists();

    Path lukeScript = resolveScript(distributionPath.resolve("bin").resolve("luke"));

    Launcher launcher =
        new ProcessBuilderLauncher()
            .executable(lukeScript)
            // pass the same JVM which the tests are currently using; this also forces UTF-8 as
            // console
            // encoding so that we know we can safely read it.
            .envvar("LAUNCH_CMD", currentJava.toAbsolutePath().toString())
            .viaShellLauncher()
            .cwd(distributionPath)
            .args("--sanity-check");

    execute(
        launcher,
        0,
        120,
        (outputBytes) -> {
          // We know it's UTF-8 because we set file.encoding explicitly.
          var output = Files.readString(outputBytes, StandardCharsets.UTF_8);
          Assertions.assertThat(output).contains("[Vader] Hello, Luke.");
        });
  }

  /** The value of <code>System.getProperty("os.name")</code>. * */
  public static final String OS_NAME = System.getProperty("os.name");

  /** True iff running on Windows. */
  public static final boolean WINDOWS = OS_NAME.startsWith("Windows");

  protected Path resolveScript(Path scriptPath) {
    List<Path> candidates = new ArrayList<>();
    candidates.add(scriptPath);

    String fileName = scriptPath.getFileName().toString();
    if (WINDOWS) {
      candidates.add(scriptPath.resolveSibling(fileName + ".cmd"));
      candidates.add(scriptPath.resolveSibling(fileName + ".bat"));
    } else {
      candidates.add(scriptPath.resolveSibling(fileName + ".sh"));
    }

    return candidates.stream()
        .sequential()
        .filter(Files::exists)
        .findFirst()
        .orElseThrow(() -> new AssertionError("No script found for the base path: " + scriptPath));
  }

  private static Supplier<Charset> forkedProcessCharset =
      () -> {
        // The default charset for a forked java process could be computed for the current
        // platform but it adds more complexity. For now, assume it's just parseable ascii.
        return StandardCharsets.ISO_8859_1;
      };

  protected void execute(
      Launcher launcher,
      int expectedExitCode,
      long timeoutInSeconds,
      ThrowingConsumer<Path> processOutputConsumer)
      throws Exception {

    try (ForkedProcess forkedProcess = launcher.execute()) {
      String command = forkedProcess.getProcess().info().command().orElse("(unset command name)");

      Charset charset = forkedProcessCharset.get();
      try {
        Process p = forkedProcess.getProcess();
        if (!p.waitFor(timeoutInSeconds, TimeUnit.SECONDS)) {
          throw new AssertionError("Forked process did not terminate in the expected time");
        }

        int exitStatus = p.exitValue();

        Assertions.assertThat(exitStatus)
            .as("forked process exit status")
            .isEqualTo(expectedExitCode);

        processOutputConsumer.accept(forkedProcess.getProcessOutputFile());
      } catch (Throwable t) {
        logSubprocessOutput(
            command, Files.readString(forkedProcess.getProcessOutputFile(), charset));
        throw t;
      }
    }
  }

  protected void logSubprocessOutput(String command, String output) {
    System.out.printf(
        Locale.ROOT,
        "--- [forked subprocess output: %s] ---%n%s%n--- [end of subprocess output] ---%n",
        command,
        output);
  }
}
