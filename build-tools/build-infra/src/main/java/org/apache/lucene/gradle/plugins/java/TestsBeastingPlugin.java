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

import com.carrotsearch.gradle.buildinfra.buildoptions.BuildOption;
import com.carrotsearch.gradle.buildinfra.buildoptions.BuildOptionValueSource;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.stream.IntStream;
import org.apache.lucene.gradle.plugins.LuceneGradlePlugin;
import org.apache.lucene.gradle.plugins.globals.LuceneBuildGlobalsExtension;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.internal.tasks.testing.filter.DefaultTestFilter;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.options.Option;
import org.gradle.api.tasks.testing.Test;

/**
 * Adds the {@code beast} task, which re-runs tests with a constant or varying root randomization
 * seed. The number of re-runs is passed with a build option {@code tests.dups}.
 *
 * <p>Normal {@code --tests} test filtering options apply to restrict retries to a single test.
 */
public class TestsBeastingPlugin extends LuceneGradlePlugin {
  /*
   * TODO: subtasks are not run in parallel (sigh, gradle removed this capability for intra-project tasks).
   * TODO: maybe it would be better to take a deeper approach and just feed the task
   * runner duplicated suite names (much like https://github.com/gradle/test-retry-gradle-plugin)
   * TODO: this is a somewhat related issue: https://github.com/gradle/test-retry-gradle-plugin/issues/29
   */

  public static class RootHooksPlugin extends LuceneGradlePlugin {
    @Override
    public void apply(Project rootProject) {
      applicableToRootProjectOnly(rootProject);

      BuildOption testsSeedOption = getBuildOptions(rootProject).getOption("tests.seed");

      rootProject
          .getTasks()
          .register(
              "warnAboutConstantSeed",
              task -> {
                task.doFirst(
                    t -> {
                      t.getLogger()
                          .warn(
                              "Root randomization seed is externally provided ({}), all duplicated runs will use the same starting seed.",
                              testsSeedOption.getSource().name());
                    });
              });
    }
  }

  @Override
  public void apply(Project project) {
    requiresAppliedPlugin(project, JavaPlugin.class);

    project.getRootProject().getPlugins().apply(RootHooksPlugin.class);

    BuildOption testsSeedOption = getBuildOptions(project).getOption("tests.seed");
    boolean rootSeedUserProvided =
        testsSeedOption.getSource() != BuildOptionValueSource.COMPUTED_VALUE;

    var buildOptions = getBuildOptions(project);
    Provider<Integer> dupsOption =
        buildOptions.addIntOption(
            "tests.dups", "Reiterate runs of entire test suites this many times ('beast' task).");

    boolean beastingMode =
        project.getGradle().getStartParameter().getTaskNames().stream()
            .anyMatch(name -> name.equals("beast") || name.endsWith(":beast"));

    var beastTask =
        project
            .getTasks()
            .register(
                "beast",
                BeastTask.class,
                task -> {
                  task.setDescription(
                      "Run a test suite (or a set of tests) many times over (duplicate 'test' task).");
                  task.setGroup("Verification");
                });

    if (!beastingMode) {
      return;
    }

    if (!dupsOption.isPresent()) {
      throw new GradleException("Specify -Ptests.dups=[count] for the beast task.");
    }

    // generate N test tasks and attach them to the beasting task for this
    // project;
    // the test filter will be applied by the beast task once it is received
    // from
    // command line.
    long rootSeed =
        project
            .getExtensions()
            .getByType(LuceneBuildGlobalsExtension.class)
            .getRootSeedAsLong()
            .get();

    var subtasks =
        IntStream.rangeClosed(1, dupsOption.get())
            .mapToObj(
                idx -> {
                  return project
                      .getTasks()
                      .register(
                          "test_" + idx,
                          Test.class,
                          test -> {
                            test.setFailFast(true);
                            // If there is a user-provided root seed, use it
                            // (all
                            // duplicated tasks will run
                            // from the same starting seed). Otherwise, pick a
                            // sequential derivative.
                            if (!rootSeedUserProvided) {
                              test.systemProperty(
                                  "tests.seed",
                                  String.format(
                                      Locale.ROOT,
                                      "%08X",
                                      new Random(rootSeed + (idx * 6364136223846793005L))
                                          .nextLong()));
                            }
                          });
                })
            .toList();

    beastTask.configure(task -> task.dependsOn(subtasks));
  }

  /**
   * We have to declare a dummy task here to be able to reuse the same syntax for 'test' task filter
   * option.
   */
  public abstract static class BeastTask extends DefaultTask {
    @Option(
        option = "tests",
        description = "Sets test class or method name to be included, '*' is supported.")
    public void setTestNamePatterns(List<String> patterns) {
      getTaskDependencies()
          .getDependencies(this)
          .forEach(
              subtask -> {
                if (subtask instanceof Test testTask) {
                  ((DefaultTestFilter) testTask.getFilter())
                      .setCommandLineIncludePatterns(patterns);
                }
              });
    }

    @TaskAction
    void run() {}
  }
}
