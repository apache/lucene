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
package org.apache.lucene.gradle.plugins.misc;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.stream.Stream;
import org.apache.lucene.gradle.plugins.LuceneGradlePlugin;
import org.gradle.api.GradleException;
import org.gradle.api.Project;

/**
 * Check that gradlew scripts contain custom Lucene tweaks (memory settings, wrapper jar download
 * and verification, etc.). These can be accidentally overwritten if somebody updates the wrapper
 * automatically.
 */
public class CheckGradlewScriptsTweakedPlugin extends LuceneGradlePlugin {
  public static final String TASK_NAME = "gradlewScriptsTweaked";

  @Override
  public void apply(Project rootProject) {
    applicableToRootProjectOnly(rootProject);

    checkGradlewScriptsTweaked(rootProject);
  }

  private void checkGradlewScriptsTweaked(Project project) {
    var gradlewScriptsTweaked =
        project
            .getTasks()
            .register(
                TASK_NAME,
                task -> {
                  var scripts =
                      Stream.of("gradlew", "gradlew.bat")
                          .map(
                              name ->
                                  project.getLayout().getProjectDirectory().file(name).getAsFile())
                          .toList();

                  task.getInputs().files(scripts);
                  task.doFirst(
                      _ -> {
                        try {
                          for (var script : scripts) {
                            var content = Files.readString(script.toPath(), StandardCharsets.UTF_8);
                            if (!content.contains("START OF LUCENE CUSTOMIZATION")) {
                              throw new GradleException(
                                  "Launch script "
                                      + script
                                      + " seems to be missing Lucene-specific"
                                      + " customizations (have you upgraded gradle wrapper?)."
                                      + " Review the history of this file and reapply the required tweaks.");
                            }
                          }
                        } catch (IOException e) {
                          throw new UncheckedIOException(e);
                        }
                      });
                });

    project.getTasks().named("check").configure(t -> t.dependsOn(gradlewScriptsTweaked));
  }
}
