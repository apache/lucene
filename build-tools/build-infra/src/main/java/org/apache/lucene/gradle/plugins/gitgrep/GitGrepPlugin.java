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
package org.apache.lucene.gradle.plugins.gitgrep;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.lucene.gradle.plugins.LuceneGradlePlugin;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.Exec;
import org.gradle.api.tasks.TaskContainer;
import org.gradle.api.tasks.TaskProvider;

/** Uses {@code git-grep(1)} to find text files matching a list of patterns. */
public class GitGrepPlugin extends LuceneGradlePlugin {
  @Override
  public void apply(Project project) {
    applicableToRootProjectOnly(project);

    // currently configured as basic regular expressions...
    // extended or even perl-compatible can be configured, but best avoided
    List<String> invalidPatterns =
        List.of(
            // misuse of RTL/LTR (https://trojansource.codes)
            "\u202A", // LRE
            "\u202B", // RLE
            "\u202C", // PDF
            "\u202D", // LRO
            "\u202E", // RLO
            "\u2066", // LRI
            "\u2067", // RLI
            "\u2068", // FSI
            "\u2069", // PDI
            // unwanted BOM markers
            "\u200B", // BOM
            "\uFEFF", // ZWS
            // svn properties
            "$Id\\b",
            "$Header\\b",
            "$Source\\b",
            "$HeadURL\\b",
            "$URL\\b",
            "$Author\\b",
            "$LastChangedBy\\b",
            "$Revision\\b",
            "$LastChangedRevision\\b",
            "$Rev\\b");

    TaskContainer tasks = project.getTasks();
    TaskProvider<Exec> applyGitGrepRulesTask =
        tasks.register(
            "applyGitGrepRules",
            Exec.class,
            (Exec task) -> {
              Provider<String> gitExecName =
                  getBuildOptions(project).getOption("lucene.tool.git").asStringProvider();
              if (gitExecName.isPresent()) {
                task.setExecutable(gitExecName.get());
              } else {
                task.setEnabled(false);
              }

              // we could pass each pattern via '-e', but this feels more comfortable.
              Path inputFile = task.getTemporaryDir().toPath().resolve("patterns.txt");
              task.doFirst(
                  _ -> {
                    try {
                      Files.createDirectories(inputFile.getParent());
                      Files.write(inputFile, invalidPatterns);
                    } catch (IOException e) {
                      throw new UncheckedIOException(e);
                    }
                  });
              task.setWorkingDir(project.getLayout().getProjectDirectory());
              task.setIgnoreExitValue(true);
              task.setArgs(
                  List.of(
                      "--no-pager",
                      "grep",
                      "--untracked", // also check untracked files
                      "--line-number", // add line numbers to output
                      "--column", // add col numbers to output
                      "-I", // don't search binary files
                      "-f", // read patterns from file
                      inputFile.toString(),
                      "--", // exempt ourselves from the check
                      ":^*GitGrepPlugin.java"));
              task.doLast(
                  _ -> {
                    // only exit status of 1 (no matches) is acceptable
                    int exitStatus = task.getExecutionResult().get().getExitValue();
                    if (exitStatus == 0) {
                      throw new GradleException("Illegal source code patterns were matched");
                    } else if (exitStatus != 1) {
                      throw new GradleException("Internal error searching for patterns");
                    }
                  });
            });

    tasks
        .named("check")
        .configure(
            task -> {
              task.dependsOn(applyGitGrepRulesTask);
            });
  }
}
