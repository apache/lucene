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
package org.apache.lucene.gradle.plugins.misc;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import org.apache.lucene.gradle.SuppressForbidden;
import org.apache.lucene.gradle.plugins.LuceneGradlePlugin;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.tasks.StopExecutionException;

/** Configures "helpXyz" tasks which display plain text files under 'help' folder. */
public class HelpPlugin extends LuceneGradlePlugin {
  record HelpFile(String name, String path, String description) {}

  private static final List<HelpFile> helpFiles =
      List.of(
          new HelpFile("Workflow", "help/workflow.txt", "Typical workflow commands."),
          new HelpFile("Tests", "help/tests.txt", "Tests, filtering, beasting, etc."),
          new HelpFile("Formatting", "help/formatting.txt", "Code formatting conventions."),
          new HelpFile("Jvms", "help/jvms.txt", "Using alternative or EA JVM toolchains."),
          new HelpFile(
              "Deps", "help/dependencies.txt", "Declaring, inspecting and excluding dependencies."),
          new HelpFile(
              "ForbiddenApis",
              "help/forbiddenApis.txt",
              "How to add/apply rules for forbidden APIs."),
          new HelpFile(
              "LocalSettings",
              "help/localSettings.txt",
              "Local settings, overrides and build performance tweaks."),
          new HelpFile(
              "Regeneration",
              "help/regeneration.txt",
              "How to refresh generated and derived resources."),
          new HelpFile("Jmh", "help/jmh.txt", "JMH micro-benchmarks."),
          new HelpFile("Git", "help/git.txt", "Git assistance and guides."),
          new HelpFile("IDEs", "help/IDEs.txt", "IDE support."),
          new HelpFile(
              "Publishing",
              "help/publishing.txt",
              "Maven and other artifact publishing, signing, etc."));

  @Override
  public void apply(Project rootProject) {
    applicableToRootProjectOnly(rootProject);

    // Register helpXyz task to display the given help section.
    var tasks = rootProject.getTasks();
    for (var helpEntry : helpFiles) {
      tasks.register(
          "help" + helpEntry.name,
          task -> {
            task.setGroup("Help (developer guides and hints)");
            task.setDescription(helpEntry.description);
            task.doFirst(
                _ -> {
                  try {
                    println("\n" + Files.readString(rootProject.file(helpEntry.path).toPath()));
                  } catch (IOException e) {
                    throw new UncheckedIOException(e);
                  }
                });
          });
    }

    // Add a custom section to the default 'help' task.
    tasks
        .named("help")
        .configure(
            task -> {
              task.doFirst(
                  _ -> {
                    printf("");
                    printf(
                        "This is Lucene %s build file (gradle %s). See some guidelines in files",
                        rootProject.getVersion(), rootProject.getGradle().getGradleVersion());
                    printf("found under the help/* folder or type any of these:");
                    printf("");

                    helpFiles.forEach(
                        helpFile ->
                            printf(
                                "  gradlew :help%-16s # %s", helpFile.name, helpFile.description));

                    println("");
                    println(
                        "For the impatient, build the project with 'gradlew assemble', run all tests with 'gradlew check', check "
                            + "your current build options with 'gradlew allOptions'.");
                    throw new StopExecutionException();
                  });
            });

    // ensure all help files exist.
    // Make sure all help files exist.
    var helpDir = getProjectRootPath(rootProject).resolve("help");
    var allHelpFilesExist =
        tasks.register(
            "checkAllHelpFilesExist",
            task -> {
              task.doFirst(
                  _ -> {
                    helpFiles.forEach(
                        helpFile -> {
                          if (!rootProject.file(helpFile.path).exists()) {
                            throw new GradleException("Help file missing: " + helpFile.path);
                          }
                        });

                    try {
                      var existing =
                          Files.list(helpDir)
                              .map(p -> "help/" + p.getFileName().toString())
                              .collect(Collectors.toSet());
                      var expected =
                          helpFiles.stream().map(v -> v.path).collect(Collectors.toSet());
                      if (!existing.equals(expected)) {
                        throw new GradleException(
                            "help/*.txt help files are not consistent with the descriptions"
                                + "in the "
                                + getClass().getName());
                      }
                    } catch (IOException e) {
                      throw new UncheckedIOException(e);
                    }
                  });
            });

    tasks
        .named("check")
        .configure(
            task -> {
              task.dependsOn(allHelpFilesExist);
            });
  }

  @SuppressForbidden(reason = "sysout is ok here.")
  private static void printf(String fmt, Object... args) {
    System.out.printf(Locale.ROOT, fmt + "\n", args);
  }

  @SuppressForbidden(reason = "sysout is ok here.")
  private static void println(String s) {
    System.out.println(s);
  }
}
