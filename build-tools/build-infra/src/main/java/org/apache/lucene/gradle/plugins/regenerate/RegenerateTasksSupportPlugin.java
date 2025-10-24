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
package org.apache.lucene.gradle.plugins.regenerate;

import groovy.json.JsonOutput;
import groovy.json.JsonSlurper;
import groovy.lang.Closure;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.lucene.gradle.plugins.LuceneGradlePlugin;
import org.apache.lucene.gradle.plugins.licenses.CheckLicensesPlugin;
import org.apache.lucene.gradle.plugins.misc.QuietExec;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.file.FileCollection;
import org.gradle.api.logging.LogLevel;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.TaskContainer;
import org.gradle.api.tasks.TaskProvider;

/**
 * Adds support for tasks that regenerate (versioned) sources within Lucene.
 *
 * <p>This utility method implements the logic required for "persistent" incremental data or source
 * - generating tasks. This means that even in the absence of previous gradle runs (no incremental
 * information), these costly regeneration tasks can be skipped.
 *
 * <p>The idea is simple, the implementation quite complex.
 *
 * <p>Given a source-generating task "regenerateFoo" (note the prefix and suffix), we create a bunch
 * of other tasks that perform checksum generation, validation and source task skipping. For
 * example, let's say we have a task "regenerateFoo"; the following tasks would be created and
 * ordered for execution:
 *
 * <pre>
 * regenerateFooChecksumSave
 * regenerateFooChecksumCheck
 * </pre>
 *
 * We wire up those tasks with dependencies and ordering constraints so that they run in proper
 * sequence. Additional ordering constraints may be necessary and can be expressed using {@link
 * RegenerateTaskExtension} task extension.
 *
 * <p>Checksums are persisted and computed from the source task's inputs/outputs. If the persisted
 * checksums are identical to the now-current checksums, the task is skipped.
 */
public class RegenerateTasksSupportPlugin extends LuceneGradlePlugin {
  private static final Pattern REGEN_TASK_PATTERN = Pattern.compile("regenerate(?<name>.+)");

  private static final String REGEN_TASKS_GROUP = "Source/data regeneration";

  private static final String REGEN_TASK_NAME = "regenerate";

  @Override
  public void apply(Project rootProject) {
    applicableToRootProjectOnly(rootProject);
    rootProject.getAllprojects().forEach(RegenerateTasksSupportPlugin::applyRegeneratePlugin);
  }

  private static void applyRegeneratePlugin(Project project) {
    TaskContainer tasks = project.getTasks();

    var regenerateTasks =
        tasks.matching(task -> REGEN_TASK_PATTERN.matcher(task.getName()).matches());

    // Configure the extension for each regenerate task.
    regenerateTasks.configureEach(
        task -> {
          // Add a task extension to allow tasks to modify their behavior upon 'regenerate'.
          task.getExtensions().create("regenerate", RegenerateTaskExtension.class);
        });

    // If the source task is of type QuietExec, configure a subset of its
    // ignored properties automatically.
    regenerateTasks
        .withType(QuietExec.class)
        .configureEach(
            task -> {
              task.getExtensions()
                  .getByType(RegenerateTaskExtension.class)
                  .getIgnoredInputs()
                  .addAll(Set.of("args", "executable", "ignoreExitValue"));
            });

    // wait until after the evaluation is completed to give tasks some time to configure
    // extension properties, then process these tasks. I don't
    // think this can be done reasonably well based on lazy properties alone.
    project.afterEvaluate(
        _ -> {
          for (String name : project.getTasks().getNames()) {
            if (REGEN_TASK_PATTERN.matcher(name).matches()) {
              configureRegenerateTask(project, regenerateTasks.named(name));
            }
          }
        });

    // register a single task to invoke all regeneration tasks.
    tasks.register(
        REGEN_TASK_NAME,
        task -> {
          task.setGroup(REGEN_TASKS_GROUP);
          task.setDescription("Rerun any code or static data generation tasks.");
          task.dependsOn(regenerateTasks);
        });

    // Nearly all regeneration tasks touch input sources, which are inputs to checkLicenses
    // so schedule this globally, once.
    project
        .getRootProject()
        .getTasks()
        .matching(task -> task.getName().equals(CheckLicensesPlugin.CHECK_LICENSES_TASK))
        .configureEach(
            task -> {
              task.mustRunAfter(regenerateTasks);
            });
  }

  private static void configureRegenerateTask(
      Project project, TaskProvider<? extends Task> taskProvider) {
    var tasks = project.getTasks();
    String taskName = taskProvider.getName();
    String taskPrefix = taskProvider.getName();

    Path checksumsFile = project.file("src/generated/checksums/" + taskPrefix + ".json").toPath();

    // Configure checksum-saving task.
    var checksumSaveTask =
        tasks.register(
            taskPrefix + "ChecksumSave",
            t -> {
              var delegate = taskProvider.get();

              // Check delegate's sanity.
              if (delegate.getGroup() != null && !delegate.getGroup().isBlank()) {
                throw new GradleException(
                    "Internal regeneration tasks must not set task.group: " + delegate.getPath());
              }
              delegate.setGroup(REGEN_TASKS_GROUP);

              if (delegate.getDescription() == null || delegate.getDescription().isBlank()) {
                throw new GradleException(
                    "Internal regeneration tasks must set task.description: " + delegate.getPath());
              }

              RegenerateTaskExtension regenerateExt =
                  delegate.getExtensions().getByType(RegenerateTaskExtension.class);

              // ensure proper scheduling of any must-run-before tasks,
              // if there are any declared.
              {
                var taskNames = regenerateExt.getMustRunBefore().get();
                project
                    .getTasks()
                    .matching(task -> taskNames.contains(task.getName()))
                    .forEach(other -> other.mustRunAfter(t));
              }

              // ensure proper scheduling of any followedUpBy tasks (those happening before
              // checksums are saved but after the source task ran).
              {
                var taskNames = regenerateExt.getFollowedUpBy().get();
                t.dependsOn(taskNames);
                project
                    .getTasks()
                    .matching(task -> taskNames.contains(task.getName()))
                    .forEach(other -> other.mustRunAfter(delegate));
              }

              // configure explicit skipping of other tasks if they are listed
              {
                var shouldRerun = project.getObjects().property(Boolean.class);
                shouldRerun.finalizeValueOnRead();

                if (project.getGradle().getStartParameter().isRerunTasks()) {
                  shouldRerun.set(true);
                } else {
                  shouldRerun.set(
                      project
                          .getProviders()
                          .provider(
                              () -> {
                                var current = computeChecksummedEntries(delegate);
                                var expected = loadChecksums(checksumsFile);
                                return !current.equals(expected);
                              }));
                }

                delegate.onlyIf(_ -> shouldRerun.get());
                var taskNames = regenerateExt.getIfSkippedAlsoSkip().get();
                project
                    .getTasks()
                    .matching(task -> taskNames.contains(task.getName()))
                    .forEach(other -> other.onlyIf(_ -> shouldRerun.get()));
              }

              // only run if the delegate ran and did the job successfully.
              t.onlyIf(
                  _ -> {
                    return delegate.getState().getFailure() == null;
                  });

              t.getOutputs().file(checksumsFile);
              t.getOutputs()
                  .upToDateWhen(
                      _ -> {
                        return !delegate.getState().getDidWork();
                      });

              // compare checksums and write them back, if they have changed.
              t.doFirst(
                  _ -> {
                    var current = computeChecksummedEntries(delegate);
                    var expected = loadChecksums(checksumsFile);

                    if (!current.equals(expected)) {
                      writeChecksums(checksumsFile, computeChecksummedEntries(delegate));
                      t.getLogger()
                          .info(
                              "Updated the derived resources checksum file for task: {}", taskName);
                    }
                  });
            });

    taskProvider.configure(
        task -> {
          task.finalizedBy(checksumSaveTask);
        });

    // Configure checksum check task (verification).
    var checksumCheckTask =
        tasks.register(
            taskName + "ChecksumCheck",
            t -> {
              var delegate = taskProvider.get();

              // if we're regenerating, run any checks after the save.
              t.mustRunAfter(checksumSaveTask);

              t.doFirst(
                  _ -> {
                    var currentChecksums = computeChecksummedEntries(delegate);
                    var expectedChecksums = loadChecksums(checksumsFile);

                    if (!currentChecksums.equals(expectedChecksums)) {
                      // This can be made prettier but leave it verbose for now:
                      Map<String, String> current = new TreeMap<>(currentChecksums);
                      Map<String, String> expected = new TreeMap<>(expectedChecksums);

                      // remove the intersection of identical entries.
                      var sameEntries = new TreeMap<>(current);
                      sameEntries.entrySet().retainAll(expected.entrySet());
                      current.entrySet().removeAll(sameEntries.entrySet());
                      expected.entrySet().removeAll(sameEntries.entrySet());

                      Function<Map<String, String>, String> mapToString =
                          map ->
                              map.isEmpty()
                                  ? "(empty)"
                                  : map.entrySet().stream()
                                      .map(e -> "  - " + e.getKey() + "=" + e.getValue())
                                      .collect(Collectors.joining("\n"));

                      Function<FileCollection, String> filesToString =
                          files ->
                              files.getFiles().stream()
                                  .map(f -> "  - " + f)
                                  .collect(Collectors.joining("\n"));

                      throw new GradleException(
                          "Checksums mismatch for generated resources; you might have"
                              + " modified a generated resource "
                              + ("(regenerate task: "
                                  + taskName
                                  + ", checksum file: "
                                  + checksumsFile
                                  + ").\n\n")
                              + ("Current checksums:\n" + mapToString.apply(current) + "\n\n")
                              + ("Expected checksums:\n" + mapToString.apply(expected) + "\n\n")
                              + ("Input files for this task are:\n"
                                  + filesToString.apply(delegate.getInputs().getFiles())
                                  + "\n\n")
                              + ("Files generated by this task are:\n"
                                  + filesToString.apply(delegate.getOutputs().getFiles())
                                  + "\n"));
                    }
                  });
            });

    // Attach checksum check to the check task.
    tasks.named("check").configure(t -> t.dependsOn(checksumCheckTask));
  }

  private static void writeChecksums(Path checksumsFile, TreeMap<String, String> currentChecksums) {
    try {
      Files.createDirectories(checksumsFile.getParent());
      Files.writeString(
          checksumsFile, JsonOutput.prettyPrint(JsonOutput.toJson(currentChecksums)) + '\n');
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static TreeMap<String, String> loadChecksums(Path path) {
    try {
      var checksums = new TreeMap<String, String>();
      if (Files.exists(path)) {
        @SuppressWarnings("unchecked")
        var saved = (Map<String, String>) new JsonSlurper().parse(path);
        checksums.putAll(saved);
      }
      return checksums;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Compute all "checksummed" key-value pairs. */
  private static TreeMap<String, String> computeChecksummedEntries(Task sourceTask) {
    // A flat ordered map of key-value pairs.
    TreeMap<String, String> allEntries = new TreeMap<>();

    // Make sure all input properties are either simple strings
    // or closures returning simple strings.
    //
    // Don't overcomplicate things with other serializable types.
    Map<String, Object> props = new LinkedHashMap<>(sourceTask.getInputs().getProperties());

    var regenerateExt = sourceTask.getExtensions().getByType(RegenerateTaskExtension.class);
    props.keySet().removeAll(regenerateExt.getIgnoredInputs().get());

    props.forEach(
        (k, v) -> {
          allEntries.put("property:" + k, valueToString(sourceTask.getPath(), k, v));
        });

    // Collect all task input and output files and compute their checksums.
    FileCollection allFiles =
        sourceTask.getInputs().getFiles().plus(sourceTask.getOutputs().getFiles());

    var digestUtils = new DigestUtils(DigestUtils.getSha1Digest());
    var rootPath = sourceTask.getProject().getRootDir().toPath();
    for (var file : allFiles.getFiles()) {
      try {
        allEntries.put(
            rootPath.relativize(file.toPath()).toString(),
            file.exists() ? digestUtils.digestAsHex(file).trim() : "--");
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    if (sourceTask.getLogger().isEnabled(LogLevel.INFO)) {
      sourceTask
          .getLogger()
          .info(
              "Checksum properties:\n"
                  + allEntries.entrySet().stream()
                      .map(e -> "  " + e.getKey() + ": " + e.getValue())
                      .collect(Collectors.joining("\n")));
    }

    return allEntries;
  }

  private static String valueToString(String sourceTaskName, String key, Object val) {
    // Handle closures and other lazy providers.
    if (val instanceof Provider<?> asProvider) {
      val = asProvider.get();
    }

    if (val instanceof Closure<?> asClosure) {
      val = asClosure.call();
    }

    if (val instanceof Boolean || val instanceof Number) {
      val = val.toString();
    }

    if (val instanceof List<?> asList) {
      val =
          "["
              + asList.stream()
                  .map(e -> valueToString(sourceTaskName, key, e))
                  .collect(Collectors.joining(", "))
              + "]";
    }

    if (val instanceof String asString) {
      return asString;
    } else {
      throw new GradleException(
          String.format(
              Locale.ROOT,
              "Input properties of wrapped tasks must all be strings: %s in %s is not: %s",
              key,
              sourceTaskName,
              val.getClass().getName()));
    }
  }
}
