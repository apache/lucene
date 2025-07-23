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
package org.apache.lucene.gradle.plugins.spotless;

import java.util.List;
import javax.inject.Inject;
import org.apache.lucene.gradle.plugins.LuceneGradlePlugin;
import org.gradle.api.DefaultTask;
import org.gradle.api.Project;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.ConfigurableFileTree;
import org.gradle.api.file.FileTree;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.IgnoreEmptyDirectories;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;
import org.gradle.internal.logging.progress.ProgressLoggerFactory;

/** Checks for invalid usage patterns in source files. */
public class ValidateSourcePatternsPlugin extends LuceneGradlePlugin {
  private static List<String> extensions = List.of("java", "xml");

  @Override
  public void apply(Project project) {
    applicableToRootProjectOnly(project);

    project.allprojects(this::configureProject);
  }

  private void configureProject(Project project) {
    project
        .getTasks()
        .register("validateSourcePatterns", ValidateSourcePatternsTask.class, task -> {});
  }

  @CacheableTask
  public abstract static class ValidateSourcePatternsTask extends DefaultTask {
    private static final Object ratBug = new Object();

    @InputFiles
    @PathSensitive(PathSensitivity.RELATIVE)
    @IgnoreEmptyDirectories
    public abstract ConfigurableFileCollection getSourceFiles();

    @Inject
    protected abstract ProgressLoggerFactory getProgressLoggerFactory();

    @TaskAction
    public void check() {}
  }
}
