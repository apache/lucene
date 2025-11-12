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
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
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
import org.gradle.api.tasks.Delete;
import org.gradle.api.tasks.TaskCollection;
import org.gradle.api.tasks.TaskContainer;

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

  private static final String REGEN_TASKS_GROUP = "Regeneration";

  private static final String REGEN_TASK_NAME = "regenerate";
  private static final String REGEN_TASK_WIPE_CHECKSUMS = "removeRegenerateChecksums";

  @Override
  public void apply(Project rootProject) {
    applicableToRootProjectOnly(rootProject);

    var regenTasks =
        rootProject.getAllprojects().stream()
            .map(RegenerateTasksSupportPlugin::applyRegeneratePlugin)
            .toList();

    // wait until after the evaluation is completed to give tasks some time to configure
    // extension properties, then process these tasks that have the regenerate task extension
    // configured.
    rootProject
        .getGradle()
        .projectsEvaluated(
            _ -> {
              for (var taskCollection : regenTasks) {
                for (Task task : taskCollection.stream().toList()) {
                  configureRegenerateTask(task);
                }
              }
            });

    // Nearly all regeneration tasks touch input sources, which are inputs to checkLicenses
    // so schedule this globally, once.
    rootProject
        .getTasks()
        .matching(task -> task.getName().equals(CheckLicensesPlugin.CHECK_LICENSES_TASK))
        .configureEach(
            task -> {
              task.mustRunAfter(regenTasks);
            });
  }

  private static TaskCollection<Task> applyRegeneratePlugin(Project project) {
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

    // register a single task to invoke all regeneration tasks.
    tasks.register(
        REGEN_TASK_NAME,
        task -> {
          task.setGroup(REGEN_TASKS_GROUP);
          task.setDescription("Rerun any code or static data generation tasks.");
          task.dependsOn(regenerateTasks);
        });

    // register a task to wipe all checksums. Use only for debugging.
    tasks.register(
        REGEN_TASK_WIPE_CHECKSUMS,
        Delete.class,
        task -> {
          task.delete(project.fileTree("src/generated/checksums", cfg -> cfg.include("*.json")));
        });

    return regenerateTasks;
  }

  private static void configureRegenerateTask(Task delegate) {
    var project = delegate.getProject();
    var tasks = project.getTasks();
    String taskName = delegate.getName();
    String taskPrefix = delegate.getName();

    Path checksumsFile = project.file("src/generated/checksums/" + taskPrefix + ".json").toPath();

    // Configure checksum-saving task.
    var checksumSaveTask =
        tasks.register(
            taskPrefix + "ChecksumSave",
            t -> {
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

    // Configure task dependencies and wiring of after/before.
    {
      RegenerateTaskExtension regenerateExt =
          delegate.getExtensions().getByType(RegenerateTaskExtension.class);

      // ensure proper scheduling of any must-run-before tasks,
      // if there are any declared.
      {
        var taskNames = regenerateExt.getMustRunBefore().get();
        project
            .getTasks()
            .matching(task -> taskNames.contains(task.getName()))
            .configureEach(
                other -> {
                  other.mustRunAfter(checksumSaveTask);
                });
      }

      // ensure proper scheduling of any followedUpBy tasks (those happening before
      // checksums are saved but after the source task ran).
      {
        var taskNames = regenerateExt.getFollowedUpBy().get();
        checksumSaveTask.configure(t -> t.dependsOn(taskNames));
        project
            .getTasks()
            .matching(task -> taskNames.contains(task.getName()))
            .configureEach(
                other -> {
                  other.mustRunAfter(delegate);
                });
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
            .configureEach(other -> other.onlyIf(_ -> shouldRerun.get()));
      }
    }

    delegate.finalizedBy(checksumSaveTask);

    // Configure checksum check task (verification).
    var checksumCheckTask =
        tasks.register(
            taskName + "ChecksumCheck",
            t -> {
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
                      Map<String, String> sameEntries = new TreeMap<>(current);
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
        if (Files.isDirectory(file.toPath())) {
          throw new IOException(
              "All regenerate task checksummed inputs/outputs should be files: " + file);
        }
        allEntries.put(
            rootPath.relativize(file.toPath()).toString().replace(File.separatorChar, '/'),
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

    if (val instanceof URL asUrl) {
      val = asUrl.toString();
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
              val == null ? "null" : val.getClass().getName()));
    }
  }
}
