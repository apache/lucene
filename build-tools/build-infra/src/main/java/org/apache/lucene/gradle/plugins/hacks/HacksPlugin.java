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
package org.apache.lucene.gradle.plugins.hacks;

import de.undercouch.gradle.tasks.download.Download;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import org.apache.lucene.gradle.plugins.LuceneGradlePlugin;
import org.apache.lucene.gradle.plugins.java.EcjLintPlugin;
import org.apache.lucene.gradle.plugins.misc.CheckGradlewScriptsTweakedPlugin;
import org.gradle.api.Project;

/** This applies various odd hacks that we probably should not need. */
public class HacksPlugin extends LuceneGradlePlugin {

  @Override
  public void apply(Project rootProject) {
    applicableToRootProjectOnly(rootProject);

    rootProject
        .getAllprojects()
        .forEach(
            project -> {
              applyRetryDownloads(project);
              addDummyOutputs(project);
            });
  }

  /**
   * Set up dummy outputs for certain tasks so that {@code clean[TaskName]} works, allowing re-runs
   * and incremental builds.
   *
   * @see "https://github.com/apache/lucene/issues/10544"
   */
  private void addDummyOutputs(Project project) {
    project
        .getTasks()
        .matching(
            task -> {
              var taskName = task.getName();
              return taskName.startsWith(EcjLintPlugin.TASK_PREFIX)
                  || taskName.equals(CheckGradlewScriptsTweakedPlugin.TASK_NAME);
            })
        .configureEach(
            task -> {
              File dummyOutput =
                  project
                      .getLayout()
                      .getBuildDirectory()
                      .file("tasks/${task.name}/dummy-output.txt")
                      .get()
                      .getAsFile();
              task.getOutputs().file(dummyOutput);

              task.doLast(
                  _ -> {
                    if (!dummyOutput.exists()) {
                      try {
                        Files.createFile(dummyOutput.toPath());
                      } catch (IOException e) {
                        throw new UncheckedIOException(e);
                      }
                    }
                  });
            });
  }

  private void applyRetryDownloads(Project project) {
    project
        .getTasks()
        .withType(Download.class)
        .configureEach(
            task -> {
              task.retries(3);
            });
  }
}
