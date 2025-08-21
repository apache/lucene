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
import org.apache.lucene.gradle.plugins.LuceneGradlePlugin;
import org.barfuin.gradle.jacocolog.JacocoLogPlugin;
import org.gradle.api.Project;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.TaskCollection;
import org.gradle.api.tasks.TaskContainer;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.testing.Test;
import org.gradle.testing.jacoco.plugins.JacocoTaskExtension;
import org.gradle.testing.jacoco.tasks.JacocoReport;

/**
 * This adds jacoco code coverage to test tasks, along with an aggregated report of all coverage.
 */
public class CodeCoveragePlugin extends LuceneGradlePlugin {
  public static final String TESTS_COVERAGE_OPTION = "tests.coverage";

  public abstract static class RootExtPlugin extends LuceneGradlePlugin {
    @Override
    public void apply(Project project) {
      applicableToRootProjectOnly(project);

      boolean withCoverage = addCoverageOptions(project);

      if (withCoverage) {
        project.getPlugins().apply(JacocoLogPlugin.class);

        var tasks = project.getTasks();

        String jacocoTaskName = "jacocoAggregatedReport";
        var jacocoAggregatedReport =
            configureJacocoReport(tasks, jacocoTaskName, "Aggregated code coverage report at: ");

        tasks.register(
            "coverage",
            task -> {
              task.dependsOn(jacocoAggregatedReport);
            });
      }
    }
  }

  @Override
  public void apply(Project project) {
    requiresAppliedPlugin(project, JavaPlugin.class);

    if (project != project.getRootProject()) {
      project.getRootProject().getPlugins().apply(RootExtPlugin.class);
    }

    boolean withCoverage = addCoverageOptions(project);

    if (withCoverage) {
      project.getPlugins().apply(JacocoLogPlugin.class);

      var jacocoTestReport =
          configureJacocoReport(
              project.getTasks(), "jacocoTestReport", "Code coverage report at: ");

      TaskCollection<Test> testTasks = project.getTasks().withType(Test.class);

      project
          .getTasks()
          .register(
              "coverage",
              task -> {
                task.dependsOn(testTasks);
                task.dependsOn(jacocoTestReport);
              });

      testTasks.configureEach(
          task -> {
            // Configure jacoco destination file to be within the test
            // task's working directory
            var jacoco = task.getExtensions().getByType(JacocoTaskExtension.class);
            jacoco.setDestinationFile(
                project
                    .getProviders()
                    .provider(() -> new File(task.getWorkingDir(), "jacoco.exec")));

            // Test reports run after the test task, if it's run at all.
            task.finalizedBy(jacocoTestReport);
          });
    }
  }

  private static TaskProvider<JacocoReport> configureJacocoReport(
      TaskContainer tasks, String jacocoTaskName, String logLine) {
    var jacocoAggregatedReport = tasks.withType(JacocoReport.class).named(jacocoTaskName);

    jacocoAggregatedReport.configure(
        task -> {
          task.doLast(
              _ -> {
                task.getLogger()
                    .lifecycle(logLine + task.getReports().getHtml().getEntryPoint() + "\n");
              });
        });
    return jacocoAggregatedReport;
  }

  private static boolean addCoverageOptions(Project project) {
    // add a build option to run with coverage.
    Provider<Boolean> coverageOption =
        getBuildOptions(project)
            .addBooleanOption(TESTS_COVERAGE_OPTION, "Enables jacoco test coverage.", false);

    // Run with jacoco if either 'coverage' task is explicitly enabled or tests.coverage option is
    // true.
    return project.getGradle().getStartParameter().getTaskNames().contains("coverage")
        || coverageOption.get();
  }
}
