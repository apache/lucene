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
package org.apache.lucene.gradle.plugins.gitinfo;

import java.nio.file.Files;
import java.nio.file.Path;
import org.gradle.api.GradleException;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.file.Directory;

public class GitInfoPlugin implements Plugin<Project> {
  @Override
  public void apply(Project project) {
    if (project != project.getRootProject()) {
      throw new GradleException("This plugin is applicable to the rootProject only.");
    }

    var gitInfoProvider =
        project
            .getProviders()
            .of(
                GitInfoValueSource.class,
                spec -> {
                  spec.getParameters().getRootProjectDir().set(project.getProjectDir());
                });

    var gitInfoExtension =
        project.getExtensions().create(GitInfoExtension.NAME, GitInfoExtension.class);

    gitInfoExtension.getGitInfo().value(gitInfoProvider).finalizeValueOnRead();

    gitInfoExtension
        .getDotGitDir()
        .convention(
            project
                .getProviders()
                .provider(
                    () -> {
                      Directory projectDirectory =
                          project.getRootProject().getLayout().getProjectDirectory();
                      Path gitLocation = projectDirectory.getAsFile().toPath().resolve(".git");
                      if (!Files.exists(gitLocation)) {
                        throw new GradleException(
                            "Can't locate .git (probably not a git clone?): "
                                + gitLocation.toAbsolutePath());
                      }

                      if (Files.isDirectory(gitLocation)) {
                        return projectDirectory.dir(".git");
                      } else if (Files.isRegularFile(gitLocation)) {
                        return projectDirectory.file(".git");
                      } else {
                        throw new GradleException(
                            "Panic, .git location not a directory or file: "
                                + gitLocation.toAbsolutePath());
                      }
                    }))
        .finalizeValueOnRead();
  }
}
