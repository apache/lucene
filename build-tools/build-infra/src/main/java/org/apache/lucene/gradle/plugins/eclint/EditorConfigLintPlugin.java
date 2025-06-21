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
package org.apache.lucene.gradle.plugins.eclint;

import com.carrotsearch.gradle.buildinfra.buildoptions.BuildOptionsExtension;
import java.util.List;
import org.gradle.api.GradleException;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.tasks.Exec;

public class EditorConfigLintPlugin implements Plugin<Project> {
  @Override
  public void apply(Project project) {
    if (project != project.getRootProject()) {
      throw new GradleException("This plugin can be applied to the root project only.");
    }

    var optionName = "lucene.tool.eclint";
    var tasks = project.getTasks();

    var eclintToolOption =
        project
            .getExtensions()
            .getByType(BuildOptionsExtension.class)
            .addOption(optionName, "External eclint executable (path or name)");

    var applyEcLintTask =
        tasks.register(
            "applyEcLint",
            Exec.class,
            task -> {
              if (!eclintToolOption.isPresent()) {
                task.getLogger()
                    .warn(
                        "The eclint tool location is not set ('{}' option), will not apply eclint checks.",
                        optionName);
              }
            });

    // Common configuration.
    List.of(applyEcLintTask)
        .forEach(
            taskProv -> {
              taskProv.configure(
                  task -> {
                    if (!eclintToolOption.isPresent()) {
                      task.setEnabled(false);
                    }

                    task.setIgnoreExitValue(false);
                    if (eclintToolOption.isPresent()) {
                      task.setExecutable(eclintToolOption.get());
                    }
                    task.setWorkingDir(project.getLayout().getProjectDirectory());
                  });
            });

    tasks
        .matching(task -> task.getName().equals("check"))
        .configureEach(
            task -> {
              task.dependsOn(applyEcLintTask);
            });
  }
}
