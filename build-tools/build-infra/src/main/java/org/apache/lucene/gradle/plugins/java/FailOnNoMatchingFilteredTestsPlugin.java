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

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.inject.Inject;
import org.apache.lucene.gradle.plugins.LuceneGradlePlugin;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.provider.Provider;
import org.gradle.api.services.BuildService;
import org.gradle.api.services.BuildServiceParameters;
import org.gradle.api.tasks.testing.Test;
import org.gradle.api.tasks.testing.TestDescriptor;
import org.gradle.api.tasks.testing.TestListener;
import org.gradle.api.tasks.testing.TestResult;

/**
 * This plugin implements the logic to fail the build if {@code --tests} filters have been provided
 * on command line and no matching tests have been found (in any module). A situation like this is
 * usually caused by a typo in the filter pattern.
 */
public class FailOnNoMatchingFilteredTestsPlugin extends LuceneGradlePlugin {
  @Override
  public void apply(Project project) {
    applicableToRootProjectOnly(project);

    // Detect if test filtering is in effect.
    List<String> taskNames = project.getGradle().getStartParameter().getTaskNames();
    boolean doCount = taskNames.stream().anyMatch("--tests"::equals);

    // Register the shared build service that will do the end-of-build check
    Provider<TestCountService> service =
        project
            .getGradle()
            .getSharedServices()
            .registerIfAbsent("testCountService", TestCountService.class, _ -> {});

    project.allprojects(
        p -> {
          p.getTasks()
              .withType(Test.class)
              .configureEach(
                  task -> {
                    // Always: do not fail the Test task itself on no matching tests
                    task.getFilter().setFailOnNoMatchingTests(false);

                    if (doCount) {
                      task.usesService(service);
                      task.doFirst(_ -> service.get().incrementExecutedTasks());
                      task.addTestListener(
                          new TestListener() {
                            @Override
                            public void beforeSuite(TestDescriptor suite) {}

                            @Override
                            public void afterSuite(TestDescriptor suite, TestResult result) {
                              service.get().addExecutedTests((int) result.getTestCount());
                            }

                            @Override
                            public void beforeTest(TestDescriptor testDescriptor) {}

                            @Override
                            public void afterTest(
                                TestDescriptor testDescriptor, TestResult result) {}
                          });
                    }
                  });
        });
  }

  /**
   * Build service that tracks executed test tasks and total tests, and fails the build at
   * completion if test filtering was applied and no tests ran.
   */
  public abstract static class TestCountService
      implements BuildService<BuildServiceParameters.None>, AutoCloseable {
    private final AtomicInteger executedTests = new AtomicInteger(0);
    private final AtomicInteger executedTasks = new AtomicInteger(0);

    @Inject
    public TestCountService() {}

    void incrementExecutedTasks() {
      executedTasks.incrementAndGet();
    }

    void addExecutedTests(int n) {
      executedTests.addAndGet(n);
    }

    @Override
    public void close() {
      int tasks = executedTasks.get();
      int tests = executedTests.get();
      if (tasks > 0 && tests == 0) {
        throw new GradleException("No tests matched the provided --tests filters?");
      }
    }
  }
}
