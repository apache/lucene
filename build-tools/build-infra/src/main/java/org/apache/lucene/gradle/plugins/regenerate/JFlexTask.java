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

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import org.apache.lucene.gradle.plugins.globals.LuceneBuildGlobalsExtension;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;

public abstract class JFlexTask extends DefaultTask {
  @InputFile
  public abstract RegularFileProperty getJflexFile();

  @InputFile
  public abstract RegularFileProperty getSkeleton();

  @Internal
  public abstract Property<String> getHeapSize();

  @OutputFile
  public abstract RegularFileProperty getGeneratedFile();

  // This is marked internal instead of @Classpath because classpath
  // files are then added as inputs and end up in the regenerate's
  // extension checksums file.
  @Internal
  public abstract ConfigurableFileCollection getClasspath();

  public JFlexTask() {
    getGeneratedFile()
        .convention(
            getJflexFile()
                .map(
                    f ->
                        getProject()
                            .getLayout()
                            .getProjectDirectory()
                            .file(f.getAsFile().getAbsolutePath().replace(".jflex", ".java"))));
  }

  @TaskAction
  public void generate() {
    var project = getProject();
    var logger = getLogger();

    var jflexFile = getJflexFile().get().getAsFile();
    if (!jflexFile.exists()) {
      throw new GradleException("JFlex file does not exist: " + jflexFile);
    }

    final File target = getGeneratedFile().get().getAsFile();

    logger.lifecycle("Recompiling JFlex: {}", project.relativePath(jflexFile));

    LuceneBuildGlobalsExtension buildGlobals =
        project.getExtensions().getByType(LuceneBuildGlobalsExtension.class);

    buildGlobals
        .getExecOps()
        .javaexec(
            spec -> {
              spec.setIgnoreExitValue(false);

              spec.setClasspath(getClasspath());
              spec.getMainClass().set("jflex.Main");

              spec.args("-nobak", "--quiet", "--encoding", "UTF-8");

              if (getHeapSize().isPresent()) {
                spec.setMaxHeapSize(getHeapSize().get());
              }

              if (getSkeleton().isPresent()) {
                spec.args("--skel", getSkeleton().get().getAsFile().getAbsolutePath());
              }

              spec.args(
                  "-d", target.getParentFile().getAbsolutePath(), jflexFile.getAbsolutePath());
            });

    try {
      var text = Files.readString(target.toPath());

      // fix invalid SuppressWarnings from jflex, so that it works with javac.
      // we also need to suppress 'unused' because of dead code, so it passes ecjLint
      // you can't "stack" SuppressWarnings annotations, jflex adds its own, this is the only way
      // https://github.com/jflex-de/jflex/issues/762
      text =
          text.replace(
              "SuppressWarnings(\"fallthrough\")",
              "SuppressWarnings({\"fallthrough\",\"unused\"})");

      // Seems like a bug in gjf. https://github.com/google/google-java-format/issues/1260
      text = text.replace("/* Break so we don't hit fall-through warning: */", "");

      Files.writeString(target.toPath(), text);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
