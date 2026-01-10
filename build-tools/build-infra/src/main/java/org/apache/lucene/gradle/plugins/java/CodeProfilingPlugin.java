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

import com.carrotsearch.gradle.buildinfra.buildoptions.BuildOptionsExtension;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.lucene.gradle.plugins.LuceneGradlePlugin;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.ConfigurableFileTree;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.Delete;
import org.gradle.api.tasks.IgnoreEmptyDirectories;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.TaskCollection;
import org.gradle.api.tasks.TaskContainer;
import org.gradle.api.tasks.testing.Test;

/** This enables and configures JFR profiling for tests. */
public class CodeProfilingPlugin extends LuceneGradlePlugin {

  public abstract static class ShowJfrProfileSummaryTask extends DefaultTask {
    static final String TASK_NAME = "showJfrProfileSummary";

    @InputFiles
    @PathSensitive(PathSensitivity.RELATIVE)
    @IgnoreEmptyDirectories
    public abstract ConfigurableFileCollection getJfrFiles();

    @Input
    public abstract Property<String> getMode();

    @Input
    public abstract Property<Integer> getStackSize();

    @Input
    public abstract Property<Integer> getCount();

    @Input
    public abstract Property<Boolean> getLineNumbers();

    @Input
    public abstract Property<Boolean> getFrametypes();

    @Input
    public abstract ListProperty<Boolean> getShouldRunConditions();

    @TaskAction
    public void run() {
      try {
        if (getShouldRunConditions().get().stream().anyMatch(value -> value == false)) {
          getLogger().lifecycle("One of the test tasks failed, skipping.");
          return;
        }

        List<String> jfrFiles = getJfrFiles().getFiles().stream().map(File::toString).toList();
        getLogger()
            .lifecycle(
                "Aggregated summary of {} jfr {}.",
                jfrFiles.size(),
                jfrFiles.size() == 1 ? "profile" : "profiles");
        ProfileResults.printReport(
            jfrFiles,
            getMode().get(),
            getStackSize().get(),
            getCount().get(),
            getLineNumbers().get(),
            getFrametypes().get());
      } catch (IOException e) {
        throw new GradleException("Error when generating jfr profile summaries.", e);
      }
    }
  }

  public static class CodeProfilingRootExtPlugin extends LuceneGradlePlugin {
    @Override
    public void apply(Project project) {
      applicableToRootProjectOnly(project);

      ProfilingOptions options = addProfilingOptions(project);
      if (options.profileOption.get()) {
        project
            .getRootProject()
            .getTasks()
            .register(
                ShowJfrProfileSummaryTask.TASK_NAME,
                ShowJfrProfileSummaryTask.class,
                task -> {
                  task.getMode().set(options.modeOption);
                  task.getStackSize().set(options.stackSizeOption);
                  task.getCount().set(options.countOption);
                  task.getLineNumbers().set(options.lineNumbersOption);
                  task.getFrametypes().set(options.frametypesOption);
                });
      }
    }
  }

  @Override
  public void apply(Project project) {
    requiresAppliedPlugin(project, JavaPlugin.class);

    if (project != project.getRootProject()) {
      project.getRootProject().getPlugins().apply(CodeProfilingRootExtPlugin.class);
    }

    ProfilingOptions options = addProfilingOptions(project);
    if (options.profileOption().get()) {
      boolean hasJavaFlightRecorder =
          ModuleLayer.boot()
              .findModule("jdk.jfr")
              .map(jfrModule -> this.getClass().getModule().canRead(jfrModule))
              .orElse(false);
      if (!hasJavaFlightRecorder) {
        throw new GradleException(
            "Module jdk.jfr is not available; Java Flight Recorder profiles cannot be enabled.");
      }

      TaskContainer tasks = project.getTasks();
      TaskCollection<Test> testTasks = tasks.withType(Test.class);

      // jfr files are generated in each Test task's working dir.
      Provider<List<ConfigurableFileTree>> jfrFiles =
          project
              .getProviders()
              .provider(
                  () -> {
                    return testTasks.stream()
                        .map(Test::getWorkingDir)
                        .map(
                            dir ->
                                project.fileTree(
                                    dir,
                                    cft -> {
                                      cft.include("*.jfr");
                                    }))
                        .toList();
                  });

      // wipe any previous jfr files.
      var cleanPreviousProfiles =
          tasks.register(
              "cleanPreviousJfrProfiles",
              Delete.class,
              task -> {
                task.delete(jfrFiles);
              });

      // One global task to display profiling summary.
      var showProfileSummary =
          project
              .getRootProject()
              .getTasks()
              .withType(ShowJfrProfileSummaryTask.class)
              .named(ShowJfrProfileSummaryTask.TASK_NAME);
      showProfileSummary.configure(
          task -> {
            task.getJfrFiles().from(jfrFiles);
          });

      // configure Test tasks to start with the jfr.
      testTasks.configureEach(
          task -> {
            task.jvmArgs(
                List.of(
                    "-XX:StartFlightRecording=dumponexit=true,maxsize=250M,settings="
                        + gradlePluginResource(project, "testing/profiling.jfc"),
                    "-XX:+UnlockDiagnosticVMOptions",
                    "-XX:+DebugNonSafepoints"));
            task.dependsOn(cleanPreviousProfiles);
            task.finalizedBy(showProfileSummary);

            showProfileSummary.configure(
                t -> {
                  t.getShouldRunConditions()
                      .add(
                          project
                              .getProviders()
                              .provider(
                                  () -> {
                                    return task.getState().getDidWork();
                                  }));
                });
          });
    }
  }

  private static ProfilingOptions addProfilingOptions(Project project) {
    BuildOptionsExtension buildOptions = getBuildOptions(project);
    Provider<Boolean> profileOption =
        buildOptions.addBooleanOption(
            "tests.profile", "Enable Java Flight Recorder profiling.", false);
    Provider<String> modeOption =
        buildOptions.addOption(
            ProfileResults.MODE_KEY, "Profiling mode.", ProfileResults.MODE_DEFAULT);
    Provider<Integer> stackSizeOption =
        buildOptions.addIntOption(
            ProfileResults.STACKSIZE_KEY,
            "Profiling stack size.",
            Integer.parseInt(ProfileResults.STACKSIZE_DEFAULT));
    Provider<Integer> countOption =
        buildOptions.addIntOption(
            ProfileResults.COUNT_KEY,
            "Profiling entry count.",
            Integer.parseInt(ProfileResults.COUNT_DEFAULT));
    Provider<Boolean> lineNumbersOption =
        buildOptions.addBooleanOption(
            ProfileResults.LINENUMBERS_KEY,
            "Profiling with line numbers.",
            Boolean.parseBoolean(ProfileResults.LINENUMBERS_KEY));
    Provider<Boolean> frametypesOption =
        buildOptions.addBooleanOption(
            ProfileResults.FRAMETYPES_KEY,
            "Profiling frame types.",
            Boolean.parseBoolean(ProfileResults.FRAMETYPES_DEFAULT));
    return new ProfilingOptions(
        profileOption,
        modeOption,
        stackSizeOption,
        countOption,
        lineNumbersOption,
        frametypesOption);
  }

  private record ProfilingOptions(
      Provider<Boolean> profileOption,
      Provider<String> modeOption,
      Provider<Integer> stackSizeOption,
      Provider<Integer> countOption,
      Provider<Boolean> lineNumbersOption,
      Provider<Boolean> frametypesOption) {}
}
