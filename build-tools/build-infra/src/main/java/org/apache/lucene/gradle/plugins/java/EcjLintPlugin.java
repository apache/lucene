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
package org.apache.lucene.gradle.plugins.java;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.lucene.gradle.plugins.LuceneGradlePlugin;
import org.apache.lucene.gradle.plugins.spotless.ApplyGoogleJavaFormatTask;
import org.eclipse.jdt.internal.compiler.batch.Main;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.file.FileCollection;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.process.CommandLineArgumentProvider;

/**
 * This adds additional linting of Java sources using ECJ (eclipse's compiler).
 *
 * @see
 *     "https://help.eclipse.org/latest/index.jsp?topic=%2Forg.eclipse.jdt.doc.user%2Ftasks%2Ftask-using_batch_compiler.htm"
 */
public class EcjLintPlugin extends LuceneGradlePlugin {
  public static final String TASK_PREFIX = "ecjLint";
  public static final String ECJ_LINT_PREFS_PATH = "validation/ecj-lint/ecj.javadocs.prefs";

  @Override
  public void apply(Project project) {
    requiresAppliedPlugin(project, JavaPlugin.class);

    Path javadocPrefsPath = gradlePluginResource(project, ECJ_LINT_PREFS_PATH);

    // Create a [sourceSetName]EcjLint task for each source set
    // with a non-empty java.srcDirs. These tasks are then
    // attached to project's "ecjLint" task.
    var javaExt = project.getExtensions().getByType(JavaPluginExtension.class);
    assert javaExt.getSourceCompatibility().equals(javaExt.getTargetCompatibility());

    var sourceSets = javaExt.getSourceSets();
    var tasks = project.getTasks();
    List<? extends TaskProvider<?>> lintTasks =
        sourceSets.stream()
            .map(
                sourceSet ->
                    tasks.register(
                        sourceSet.getTaskName(TASK_PREFIX, null),
                        task -> configureEcjTask(sourceSet, javaExt, javadocPrefsPath, task)))
            .toList();

    var ecjLintTask =
        tasks.register(
            TASK_PREFIX,
            task -> {
              task.setGroup("Verification");
              task.setDescription("Lint Java sources using ECJ.");
              task.dependsOn(lintTasks);
            });

    // Attach ecjLint to check.
    tasks
        .named("check")
        .configure(
            task -> {
              task.dependsOn(ecjLintTask);
            });
  }

  private void configureEcjTask(
      SourceSet sourceSet, JavaPluginExtension javaExt, Path javadocPrefsPath, Task task) {
    // This dependency is on a configuration; technically it causes
    // all dependencies to be resolved before this task executes
    // (this includes scheduling tasks that compile the
    // sources from other projects for example).
    task.dependsOn(sourceSet.getCompileClasspath());

    task.mustRunAfter(task.getProject().getTasks().withType(ApplyGoogleJavaFormatTask.class));

    // We create a task for all source sets but don't run
    // tasks for which Java source directories are empty.
    // We also ignore mr-jar source sets.
    if (sourceSet.getJava().getAsFileTree().isEmpty() || sourceSet.getName().matches("main\\d+")) {
      task.setEnabled(false);
    }

    List<String> args = new ArrayList<>();
    args.add("-d");
    args.add("none");

    // Use -source/-target as it is significantly faster than --release

    args.addAll(List.of("-source", javaExt.getSourceCompatibility().toString()));
    args.addAll(List.of("-target", javaExt.getTargetCompatibility().toString()));

    args.addAll(List.of("-encoding", "UTF-8"));
    args.add("-proc:none");
    args.add("-nowarn");
    args.add("-enableJavadoc");
    args.addAll(List.of("-properties", javadocPrefsPath.toAbsolutePath().toString()));

    // We depend on modular paths.
    ModularPathsExtension modularPaths =
        (ModularPathsExtension)
            sourceSet
                .getExtensions()
                .getByName(ModularPathsPlugin.MODULAR_PATHS_EXTENSION_ECJ_NAME);
    task.dependsOn(modularPaths);

    // Collect modular dependencies and their transitive dependencies to module path.
    CommandLineArgumentProvider compilationArguments = modularPaths.getCompilationArguments();
    // this isn't exactly right but the returned CommandLineArgumentProvider isn't serializable...
    task.getInputs().files(sourceSet.getCompileClasspath());

    // Collect classpath locations
    FileCollection classpathArguments = modularPaths.getCompilationClasspath().filter(File::exists);
    task.getInputs().files(classpathArguments);

    // Input sources.
    var inputSources = sourceSet.getJava().getAsFileTree();
    task.getInputs().files(inputSources);

    // Add all arguments so far as inputs.
    task.getInputs().property("args", args);

    var argsEchoFile =
        task.getProject()
            .getLayout()
            .getBuildDirectory()
            .file("ecj/" + task.getName() + "-args.txt");

    var ecjOutput =
        task.getProject()
            .getLayout()
            .getBuildDirectory()
            .file("ecj/" + task.getName() + "-output.txt");

    task.getOutputs().files(argsEchoFile, ecjOutput);

    task.doFirst(
        _ -> {
          List<String> allArgs = new ArrayList<>(args);

          compilationArguments.asArguments().forEach(allArgs::add);
          if (!classpathArguments.isEmpty()) {
            allArgs.add("-classpath");
            allArgs.add(
                classpathArguments.getFiles().stream()
                    .map(File::toString)
                    .collect(Collectors.joining(File.pathSeparator)));
          }

          var sources =
              inputSources.getFiles().stream()
                  .map(File::toString)
                  .filter(
                      file -> {
                        // Exclude the benchmark class with dependencies on nekohtml,
                        // which causes module-classpath conflicts and breaks ecj.
                        return !file.endsWith("DemoHTMLParser.java");
                      })
                  .sorted()
                  .toList();

          allArgs.addAll(sources);

          File ecjOutputFile = ecjOutput.get().getAsFile();
          try (var out = new PrintWriter(ecjOutputFile, StandardCharsets.UTF_8)) {
            Files.writeString(argsEchoFile.get().getAsFile().toPath(), String.join("\n", allArgs));

            var success =
                new Main(out, out, false /* systemExit */, null /* options */, null /* progress */)
                    .compile(allArgs.toArray(String[]::new));

            out.flush();
            if (!success) {
              throw new GradleException(
                  "ecj linter failed:\n" + Files.readString(ecjOutputFile.toPath()));
            }
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        });
  }
}
