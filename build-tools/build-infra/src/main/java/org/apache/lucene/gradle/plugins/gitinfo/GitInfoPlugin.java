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
import org.apache.lucene.gradle.plugins.LuceneGradlePlugin;
import org.apache.lucene.gradle.plugins.globals.LuceneBuildGlobalsExtension;
import org.gradle.api.Action;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.file.Directory;
import org.gradle.api.file.FileSystemLocation;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.ValueSourceSpec;

public class GitInfoPlugin extends LuceneGradlePlugin {
  @Override
  public void apply(Project project) {
    applicableToRootProjectOnly(project);

    var gitInfoExtension =
        project.getExtensions().create(GitInfoExtension.NAME, GitInfoExtension.class);
    var providers = project.getProviders();

    Property<FileSystemLocation> dotGitDir = gitInfoExtension.getDotGitDir();
    dotGitDir
        .convention(
            providers.provider(
                () -> {
                  Directory projectDirectory =
                      project.getRootProject().getLayout().getProjectDirectory();
                  Path gitLocation = projectDirectory.getAsFile().toPath().resolve(".git");
                  if (!Files.exists(gitLocation)) {
                    project
                        .getLogger()
                        .warn(
                            "This seems to be a source bundle of Lucene (not a git clone). Some tasks may be "
                                + "skipped as they rely on .git to be present (you can run 'git init' if you "
                                + "like or use a full git clone).");

                    // don't return anything from the provider if we can't locate the .git
                    // folder. This will result in the property returning false from isPresent.
                    return null;
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
        .finalizeValue();

    var gitExec =
        project.getExtensions().getByType(LuceneBuildGlobalsExtension.class).externalTool("git");

    Action<ValueSourceSpec<GitValueSourceParameters>> configureGitParams =
        spec -> {
          var params = spec.getParameters();
          params.getRootProjectDir().set(project.getProjectDir());
          params.getGitExec().set(gitExec);
          params.getDotDir().set(dotGitDir);
        };

    gitInfoExtension
        .getGitInfo()
        .value(providers.of(GitInfoValueSource.class, configureGitParams))
        .finalizeValueOnRead();

    gitInfoExtension
        .getAllNonIgnoredProjectFiles()
        .value(providers.of(GitFileListValueSource.class, configureGitParams));
  }
}
