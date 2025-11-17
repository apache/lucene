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
import java.util.Locale;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.apache.lucene.gradle.plugins.LuceneGradlePlugin;
import org.gradle.api.Project;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.provider.Provider;
import org.gradle.api.services.BuildService;
import org.gradle.api.services.BuildServiceParameters;
import org.gradle.api.tasks.testing.Test;
import org.gradle.api.tasks.testing.TestDescriptor;
import org.gradle.api.tasks.testing.TestListener;
import org.gradle.api.tasks.testing.TestResult;

/** Display any failing tests, along with their reproduce-line at the end of the build. */
public class ShowFailedTestsAtEndPlugin extends LuceneGradlePlugin {
  private static final String SERVICE_NAME = "failingTestsTrackingService";

  public abstract static class RootExtPlugin extends LuceneGradlePlugin {
    @Override
    public void apply(Project project) {
      applicableToRootProjectOnly(project);

      // Register the shared build service that will do the end-of-build check
      project
          .getGradle()
          .getSharedServices()
          .registerIfAbsent(SERVICE_NAME, FailingTestsReportingService.class, _ -> {});
    }
  }

  public record Entry(
      String name,
      String randomizationParameters,
      String projectPath,
      String reproLine,
      File testOutput) {}

  @Override
  public void apply(Project project) {
    requiresAppliedPlugin(project, JavaPlugin.class);

    if (project != project.getRootProject()) {
      project.getRootProject().getPlugins().apply(RootExtPlugin.class);
    }

    project
        .getTasks()
        .withType(Test.class)
        .configureEach(
            task -> {
              @SuppressWarnings("unchecked")
              Provider<FailingTestsReportingService> service =
                  (Provider<FailingTestsReportingService>)
                      project
                          .getGradle()
                          .getSharedServices()
                          .getRegistrations()
                          .getByName(SERVICE_NAME)
                          .getService();
              task.usesService(service);

              String projectPath = project.getPath();
              String reproLine = ErrorReportingTestListener.getReproLineOptions(task);
              DirectoryProperty testOutputsDir =
                  task.getExtensions()
                      .getByType(TestsAndRandomizationPlugin.TestOutputsExtension.class)
                      .getTestOutputsDir();

              task.addTestListener(
                  new TestListener() {
                    @Override
                    public void beforeTest(TestDescriptor testDescriptor) {}

                    @Override
                    public void afterTest(TestDescriptor desc, TestResult result) {
                      if (result.getResultType() == TestResult.ResultType.FAILURE) {
                        // check if it's a constructor or a before/after class hook that failed.
                        String qTestName;
                        if (desc.getName().equals("classMethod")) {
                          qTestName = desc.getClassName();
                        } else {
                          qTestName = desc.getClassName() + "." + desc.getName();
                        }

                        var randomizationParameters = "";
                        var p = Pattern.compile(".+ (?<params>[{].*[}])$");
                        var matcher = p.matcher(qTestName);
                        if (matcher.matches()) {
                          randomizationParameters = matcher.group("params");
                          qTestName = qTestName.replace(randomizationParameters, "").trim();
                        }

                        service
                            .get()
                            .addFailedTest(
                                new Entry(
                                    qTestName,
                                    randomizationParameters,
                                    projectPath,
                                    "gradlew "
                                        + (projectPath + ":test")
                                        + (" --tests \"" + qTestName + "\" ")
                                        + reproLine,
                                    testOutputsDir
                                        .file(
                                            ErrorReportingTestListener.getOutputLogName(
                                                desc.getParent()))
                                        .get()
                                        .getAsFile()));
                      }
                    }

                    @Override
                    public void beforeSuite(TestDescriptor suite) {}

                    @Override
                    public void afterSuite(TestDescriptor desc, TestResult result) {
                      if (result.getExceptions() != null && !result.getExceptions().isEmpty()) {
                        service
                            .get()
                            .addFailedTest(
                                new Entry(
                                    desc.getName(),
                                    null,
                                    projectPath,
                                    "gradlew "
                                        + (projectPath + ":test")
                                        + (" --tests \"" + desc.getName() + "\" ")
                                        + reproLine,
                                    testOutputsDir
                                        .file(ErrorReportingTestListener.getOutputLogName(desc))
                                        .get()
                                        .getAsFile()));
                      }
                    }
                  });
            });
  }

  /** Build service that keeps track of test and suite times. */
  public abstract static class FailingTestsReportingService
      implements BuildService<BuildServiceParameters.None>, AutoCloseable {
    private static final Logger LOGGER = Logging.getLogger(FailingTestsReportingService.class);

    private final ConcurrentLinkedQueue<Entry> failedTests = new ConcurrentLinkedQueue<>();

    @Inject
    public FailingTestsReportingService() {}

    public void addFailedTest(Entry entry) {
      failedTests.add(entry);
    }

    @Override
    public void close() {
      if (!failedTests.isEmpty()) {
        var limit = 10;
        var formatted =
            failedTests.stream()
                .sorted((a, b) -> b.projectPath.compareTo(a.projectPath))
                .map(
                    e ->
                        String.format(
                            Locale.ROOT,
                            "  - %s (%s)%s\n    Test output: %s\n    Reproduce with: %s\n",
                            e.name,
                            e.projectPath,
                            e.randomizationParameters != null
                                    && !e.randomizationParameters.isBlank()
                                ? "\n    Context parameters: " + e.randomizationParameters
                                : "",
                            e.testOutput,
                            e.reproLine))
                .limit(limit)
                .collect(Collectors.joining("\n"));

        LOGGER.error(
            "\nERROR: {} {} failed{}:\n\n{}",
            failedTests.size(),
            failedTests.size() == 1 ? "test has" : failedTests.size() + " tests have",
            failedTests.size() > limit ? " (top " + limit + " shown)" : "",
            formatted);
      }
    }
  }
}
