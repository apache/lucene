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

import groovy.lang.Closure;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import org.apache.lucene.gradle.plugins.globals.LuceneBuildGlobalsExtension;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.CopySpec;
import org.gradle.api.file.FileTree;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.OutputFiles;
import org.gradle.api.tasks.TaskAction;
import org.gradle.process.ExecResult;
import org.gradle.process.JavaExecSpec;

public abstract class JavaCCTask extends DefaultTask {
  /** Apply closures to all generated files before they're copied back to mainline code. */
  // A subtle bug here is that this makes it not an input... should be a list of replacements
  // instead?
  private final List<Consumer<FileTree>> afterGenerate = new ArrayList<>();

  @InputFile
  public abstract RegularFileProperty getJavaccFile();

  // This is marked internal instead of @Classpath because classpath
  // files are then added as inputs and end up in the regenerate's
  // extension checksums file.
  @Internal
  public abstract ConfigurableFileCollection getClasspath();

  public void afterGenerate(Consumer<FileTree> action) {
    this.afterGenerate.add(action);
  }

  public void afterGenerate(Closure<?> action) {
    afterGenerate(action::call);
  }

  @OutputFiles
  public List<File> getGeneratedSources() {
    var javaccFile = getJavaccFile().get().getAsFile();
    File baseDir = javaccFile.getParentFile();
    String baseName = javaccFile.getName().replace(".jj", "");

    Project p = getProject();
    return Arrays.asList(
        p.file(new File(baseDir, baseName + ".java")),
        p.file(new File(baseDir, baseName + "Constants.java")),
        p.file(new File(baseDir, baseName + "TokenManager.java")),
        p.file(new File(baseDir, "ParseException.java")),
        p.file(new File(baseDir, "Token.java")),
        p.file(new File(baseDir, "TokenMgrError.java")));
  }

  @TaskAction
  public void generate() {
    var javaccFile = getJavaccFile().get().getAsFile();
    if (!javaccFile.exists()) {
      throw new GradleException("Input file does not exist: " + javaccFile);
    }

    var logger = getLogger();
    var project = getProject();

    // Run JavaCC into a temporary dir so we can post-process & know exactly what's generated
    File tempDir = getTemporaryDir();
    if (!tempDir.exists() && !tempDir.mkdirs()) {
      throw new GradleException("Unable to create temporary directory: " + tempDir);
    }

    // Run javacc generation into temporary folder so that we know all the generated files
    // and can post-process them easily.
    LuceneBuildGlobalsExtension buildGlobals =
        project.getExtensions().getByType(LuceneBuildGlobalsExtension.class);
    buildGlobals
        .getFileOps()
        .delete(spec -> spec.delete(project.fileTree(tempDir, tree -> tree.include("**/*.java"))));

    File targetDir = javaccFile.getParentFile();
    logger.lifecycle(
        "Recompiling JavaCC: {}", project.getRootDir().toPath().relativize(javaccFile.toPath()));

    ByteArrayOutputStream output = new ByteArrayOutputStream();
    ExecResult result =
        buildGlobals
            .getExecOps()
            .javaexec(
                (JavaExecSpec spec) -> {
                  spec.setClasspath(getClasspath());

                  spec.setIgnoreExitValue(true);
                  spec.setStandardOutput(output);
                  spec.setErrorOutput(output);

                  spec.getMainClass().set("org.javacc.parser.Main");
                  spec.args(
                      "-OUTPUT_DIRECTORY=" + tempDir.getAbsolutePath(),
                      javaccFile.getAbsolutePath());
                });

    if (result.getExitValue() != 0) {
      throw new GradleException(
          "JavaCC failed to compile "
              + javaccFile
              + ", here is the compilation output:\n"
              + output);
    }

    String outString = output.toString(Charset.defaultCharset());
    if (outString.contains("Warning:")) {
      throw new GradleException(
          "JavaCC emitted warnings for "
              + javaccFile
              + ", here is the compilation output:\n"
              + output);
    }

    // Apply custom modifications
    FileTree generatedFiles = project.fileTree(tempDir);
    for (Consumer<FileTree> action : afterGenerate) {
      action.accept(generatedFiles);
    }

    // Copy back to mainline sources (excluding CharStream.java)
    buildGlobals
        .getFileOps()
        .copy(
            (CopySpec spec) -> {
              spec.from(tempDir);
              spec.into(targetDir);
              spec.exclude("CharStream.java");
            });
  }
}
