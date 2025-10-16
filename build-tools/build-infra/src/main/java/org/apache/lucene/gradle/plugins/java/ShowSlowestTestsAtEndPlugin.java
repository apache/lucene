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

import java.util.Collection;
import java.util.Locale;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.apache.lucene.gradle.plugins.LuceneGradlePlugin;
import org.gradle.api.Project;
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
import org.gradle.build.event.BuildEventsListenerRegistry;
import org.gradle.tooling.events.FinishEvent;
import org.gradle.tooling.events.OperationCompletionListener;
import org.gradle.tooling.events.task.TaskFailureResult;

/** Display the slowest tests at the end of the build. */
public class ShowSlowestTestsAtEndPlugin extends LuceneGradlePlugin {
  private static final String SERVICE_NAME = "slowestTestsTrackingService";

  public abstract static class RootExtPlugin extends LuceneGradlePlugin {
    /**
     * @return Returns the injected build events listener registry.
     */
    @Inject
    protected abstract BuildEventsListenerRegistry getListenerRegistry();

    @Override
    public void apply(Project project) {
      applicableToRootProjectOnly(project);

      // Register the shared build service that will do the end-of-build check
      var service =
          project
              .getGradle()
              .getSharedServices()
              .registerIfAbsent(SERVICE_NAME, TestStatsService.class, _ -> {});

      getListenerRegistry().onTaskCompletion(service);
    }
  }

  public record Entry(String name, long duration) {}

  @Override
  public void apply(Project project) {
    requiresAppliedPlugin(project, JavaPlugin.class);

    var buildOptions = getBuildOptions(project);
    Provider<Boolean> slowestTestsOption =
        buildOptions.addBooleanOption(
            "tests.slowestTests", "Print the summary of the slowest tests.", true);
    int slowestTestsMinTime =
        buildOptions
            .addIntOption(
                "tests.slowestTests.minTime",
                "Minimum test time to consider a test slow (millis).",
                500)
            .get();
    Provider<Boolean> slowestSuitesOption =
        buildOptions.addBooleanOption(
            "tests.slowestSuites", "Print the summary of the slowest suites.", true);
    int slowestSuitesMinTime =
        buildOptions
            .addIntOption(
                "tests.slowestSuites.minTime",
                "Minimum suite time to consider a suite slow (millis).",
                1000)
            .get();

    boolean collectSlowestTests = slowestTestsOption.get();
    boolean collectSlowestSuites = slowestSuitesOption.get();

    if (!collectSlowestTests && !collectSlowestSuites) {
      // Nothing to do.
      return;
    }

    if (project != project.getRootProject()) {
      project.getRootProject().getPlugins().apply(RootExtPlugin.class);
    }

    project
        .getTasks()
        .withType(Test.class)
        .configureEach(
            task -> {
              @SuppressWarnings("unchecked")
              Provider<TestStatsService> service =
                  (Provider<TestStatsService>)
                      project
                          .getGradle()
                          .getSharedServices()
                          .getRegistrations()
                          .getByName(SERVICE_NAME)
                          .getService();
              task.usesService(service);

              var projectPath = project.getPath();

              task.addTestListener(
                  new TestListener() {
                    @Override
                    public void beforeSuite(TestDescriptor suite) {}

                    @Override
                    public void afterSuite(TestDescriptor suite, TestResult result) {
                      // Gradle reports runner times as well, omit anything that isn't attached to a
                      // concrete class.
                      if (collectSlowestSuites && suite.getClassName() != null) {
                        long duration = (result.getEndTime() - result.getStartTime());
                        if (duration >= slowestTestsMinTime) {
                          service
                              .get()
                              .addSuiteEntry(
                                  new Entry(
                                      lastNameComponent(suite) + (" (" + projectPath + ")"),
                                      duration));
                        }
                      }
                    }

                    @Override
                    public void beforeTest(TestDescriptor testDescriptor) {}

                    @Override
                    public void afterTest(TestDescriptor testDescriptor, TestResult result) {
                      if (collectSlowestTests) {
                        long duration = (result.getEndTime() - result.getStartTime());
                        if (duration >= slowestSuitesMinTime) {
                          service
                              .get()
                              .addTestEntry(
                                  new Entry(
                                      lastNameComponent(testDescriptor)
                                          + ("." + testDescriptor.getName())
                                          + (" (" + projectPath + ")"),
                                      duration));
                        }
                      }
                    }
                  });
            });
  }

  private static String lastNameComponent(TestDescriptor suite) {
    return suite.getClassName().replaceAll(".+\\.", "");
  }

  /** Build service that keeps track of test and suite times. */
  public abstract static class TestStatsService
      implements BuildService<BuildServiceParameters.None>,
          OperationCompletionListener,
          AutoCloseable {
    private static final Logger LOGGER = Logging.getLogger(TestStatsService.class);

    private final ConcurrentLinkedQueue<Entry> allTests = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<Entry> allSuites = new ConcurrentLinkedQueue<>();

    private volatile boolean hadFailedTask;

    @Inject
    public TestStatsService() {}

    public void addTestEntry(Entry entry) {
      allTests.add(entry);
    }

    public void addSuiteEntry(Entry entry) {
      allSuites.add(entry);
    }

    @Override
    public void onFinish(FinishEvent event) {
      if (event.getResult() instanceof TaskFailureResult) {
        hadFailedTask = true;
      }
    }

    @Override
    public void close() {
      if (hadFailedTask || (allTests.isEmpty() && allSuites.isEmpty())) {
        return;
      }

      if (!allTests.isEmpty()) {
        LOGGER.lifecycle("The slowest tests during this run:\n  " + toString(allTests));
      }

      if (!allSuites.isEmpty()) {
        LOGGER.lifecycle("The slowest suites during this run:\n  " + toString(allSuites));
      }
    }

    private static String toString(Collection<Entry> entries) {
      return entries.stream()
          .sorted((a, b) -> Long.compare(b.duration, a.duration))
          .limit(10)
          .map(e -> String.format(Locale.ROOT, "%5.2fs %s", e.duration / 1000d, e.name))
          .collect(Collectors.joining("\n  "));
    }
  }
}
