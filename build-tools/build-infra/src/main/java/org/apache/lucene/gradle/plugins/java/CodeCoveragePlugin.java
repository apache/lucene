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
import org.gradle.api.tasks.testing.Test;
import org.gradle.testing.jacoco.plugins.JacocoTaskExtension;
import org.gradle.testing.jacoco.tasks.JacocoReport;

/** This adds jacoco code coverage to test tasks. */
public class CodeCoveragePlugin extends LuceneGradlePlugin {
  public static final String TESTS_COVERAGE_OPTION = "tests.coverage";

  @Override
  public void apply(Project project) {
    applicableToRootProjectOnly(project);

    // add a build option to run with coverage.
    Provider<Boolean> coverageOption =
        getBuildOptions(project)
            .addBooleanOption(TESTS_COVERAGE_OPTION, "Enables jacoco test coverage.", false);

    // Run with jacoco if either 'coverage' task is explicitly enabled or tests.coverage option is
    // true.
    boolean withCoverage =
        project.getGradle().getStartParameter().getTaskNames().contains("coverage")
            || coverageOption.get();

    if (withCoverage) {
      project.getPlugins().apply(JacocoLogPlugin.class);

      var tasks = project.getTasks();

      var jacocoAggregatedReport =
          tasks.withType(JacocoReport.class).named("jacocoAggregatedReport");

      jacocoAggregatedReport.configure(
          task -> {
            task.doLast(
                t -> {
                  t.getLogger()
                      .lifecycle(
                          "Aggregated code coverage report at: "
                              + task.getReports().getHtml().getEntryPoint()
                              + "\n");
                });
          });

      tasks.register(
          "coverage",
          task -> {
            task.dependsOn(jacocoAggregatedReport);
          });

      project.getSubprojects().forEach(this::configureProject);
    }
  }

  private void configureProject(Project project) {
    project
        .getPlugins()
        .withType(JavaPlugin.class)
        .all(
            _ -> {
              project.getPlugins().apply(JacocoLogPlugin.class);

              // Synthetic task to enable test coverage (and reports).
              var jacocoTestReport =
                  project.getTasks().withType(JacocoReport.class).named("jacocoTestReport");

              jacocoTestReport.configure(
                  task -> {
                    task.doLast(
                        t -> {
                          t.getLogger()
                              .lifecycle(
                                  "Code coverage report at: : "
                                      + task.getReports().getHtml().getEntryPoint()
                                      + "\n");
                        });
                  });

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
            });
  }
}
