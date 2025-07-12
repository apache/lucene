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

import java.util.Set;
import org.apache.lucene.gradle.plugins.LuceneGradlePlugin;
import org.apache.lucene.gradle.plugins.misc.QuietExec;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.tasks.TaskCollection;

/** Adds infrastructure support for tasks that regenerate (versioned) sources within Lucene. */
public class RegenerateTasksSupportPlugin extends LuceneGradlePlugin {
  @Override
  public void apply(Project rootProject) {
    applicableToRootProjectOnly(rootProject);

    rootProject
        .getAllprojects()
        .forEach(
            project -> {
              TaskCollection<Task> regenerateTasks =
                  project
                      .getTasks()
                      .matching(task -> task.getName().matches("generate(?<name>.*)Internal"));

              // Add an extension that allows tasks to customize signatures to all generate tasks.
              regenerateTasks.configureEach(
                  task -> {
                    task.getExtensions().create("regenerate", RegenerateTaskExtension.class);
                  });

              // configure only a subset of QuietExec's properties to be inputs to signatures.
              regenerateTasks
                  .withType(QuietExec.class)
                  .configureEach(
                      task -> {
                        task.getExtensions()
                            .getByType(RegenerateTaskExtension.class)
                            .getIgnoredInputs()
                            .addAll(Set.of("args", "executable", "ignoreExitValue"));
                      });
            });
  }
}
