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
package org.apache.lucene.gradle.plugins.documentation;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import javax.inject.Inject;
import org.apache.lucene.gradle.plugins.globals.LuceneBuildGlobalsExtension;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.taskdefs.XmlProperty;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.internal.file.FileOperations;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.TaskAction;
import org.gradle.process.ExecOperations;
import org.gradle.process.ExecResult;

/** Convert {@code CHANGES.txt} into html using plenty of perl hackery. */
public abstract class ChangesToHtmlTask extends DefaultTask {
  @Input
  public abstract Property<String> getProductName();

  @InputFile
  public abstract RegularFileProperty getChangesFile();

  @Inject
  public abstract ExecOperations getExecOps();

  @InputFile
  public abstract RegularFileProperty getChangesDoapFile();

  @InputDirectory
  public abstract DirectoryProperty getSiteDir();

  @OutputDirectory
  public abstract DirectoryProperty getTargetDir();

  @InputFile
  public abstract RegularFileProperty getConversionScript();

  @Inject
  public abstract FileOperations getFileOperations();

  @Inject
  public ChangesToHtmlTask() {}

  @TaskAction
  public void convert() throws IOException {
    Path doapFile = getChangesDoapFile().get().getAsFile().toPath();

    Path versionsFile = loadVersions(doapFile);
    toHtml(versionsFile);

    getFileOperations()
        .copy(
            spec -> {
              spec.from(getSiteDir());
              spec.into(getTargetDir());
              spec.include("*.css");
            });
  }

  private void toHtml(Path versionsFile) throws IOException {
    try (var baos = new ByteArrayOutputStream();
        var changesFileIs = Files.newInputStream(getChangesFile().get().getAsFile().toPath());
        var output =
            Files.newOutputStream(getTargetDir().get().file("Changes.html").getAsFile().toPath())) {
      ExecResult result =
          getExecOps()
              .exec(
                  spec -> {
                    spec.setExecutable(
                        getProject()
                            .getExtensions()
                            .getByType(LuceneBuildGlobalsExtension.class)
                            .externalTool("perl"));
                    spec.setStandardInput(changesFileIs);
                    spec.setStandardOutput(output);
                    spec.setErrorOutput(output);
                    spec.setIgnoreExitValue(true);

                    spec.setArgs(
                        List.of(
                            "-CSD",
                            getConversionScript().get().getAsFile().toString(),
                            getProductName().get(),
                            versionsFile.toString()));
                  });

      if (result.getExitValue() != 0) {
        throw new GradleException(
            "Changes generation failed:\n" + baos.toString(StandardCharsets.UTF_8));
      }
    }
  }

  /** load version properties from DOAP RDF */
  private Path loadVersions(Path changesDoapFile) throws IOException {
    if (!Files.isRegularFile(changesDoapFile)) {
      throw new GradleException("doapFile does not exist: " + changesDoapFile);
    }

    Path tempFile = getTemporaryDirFactory().create().toPath().resolve("versions.properties");
    try (var writer = Files.newBufferedWriter(tempFile)) {
      String prefix = "doap." + getProductName().get();

      Project antProject = getAnt().getAntProject();
      XmlProperty xmlPropertyTask = (XmlProperty) antProject.createTask("xmlproperty");
      xmlPropertyTask.setKeeproot(false);
      xmlPropertyTask.setFile(changesDoapFile.toFile());
      xmlPropertyTask.setCollapseAttributes(false);
      xmlPropertyTask.setPrefix(prefix);
      xmlPropertyTask.execute();

      writer.write(antProject.getProperty(prefix + ".Project.release.Version.revision"));
      writer.newLine();
      writer.write(antProject.getProperty(prefix + ".Project.release.Version.created"));
      writer.newLine();
    }
    return tempFile;
  }
}
