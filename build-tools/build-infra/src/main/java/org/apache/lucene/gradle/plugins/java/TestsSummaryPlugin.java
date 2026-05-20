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

import org.apache.lucene.gradle.plugins.LuceneGradlePlugin;
import org.gradle.api.Project;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.tasks.testing.Test;
import org.gradle.api.tasks.testing.TestDescriptor;
import org.gradle.api.tasks.testing.TestListener;
import org.gradle.api.tasks.testing.TestResult;

/** Prints per-project test summary after each {@link Test} task. */
public class TestsSummaryPlugin extends LuceneGradlePlugin {
  @Override
  public void apply(Project project) {
    requiresAppliedPlugin(project, JavaPlugin.class);

    project
        .getTasks()
        .withType(Test.class)
        .configureEach(
            task -> {
              if (!task.getLogger().isLifecycleEnabled()) {
                return;
              }

              task.addTestListener(
                  new TestListener() {
                    @Override
                    public void afterSuite(TestDescriptor suite, TestResult result) {
                      if (suite.getParent() == null) {
                        if (result.getTestCount() > 0) {
                          StringBuilder sb = new StringBuilder();
                          sb.append(task.getPath())
                              .append(" (")
                              .append(result.getResultType().name())
                              .append("): ");

                          sb.append(pluralize(result.getTestCount(), "test"));
                          if (result.getFailedTestCount() > 0) {
                            sb.append(", ")
                                .append(pluralize(result.getFailedTestCount(), " failure"));
                          }
                          if (result.getSkippedTestCount() > 0) {
                            sb.append(", ").append(result.getSkippedTestCount()).append(" skipped");
                          }

                          task.getLogger().lifecycle(sb.toString());
                        }
                      }
                    }

                    private static String pluralize(long count, String verb) {
                      return count + " " + verb + (count == 1 ? "" : "s");
                    }

                    @Override
                    public void beforeSuite(TestDescriptor suite) {
                      // empty.
                    }

                    @Override
                    public void beforeTest(TestDescriptor testDescriptor) {
                      // empty.
                    }

                    @Override
                    public void afterTest(TestDescriptor testDescriptor, TestResult result) {
                      // empty.
                    }
                  });
            });
  }
}
