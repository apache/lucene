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

import com.google.googlejavaformat.java.FormatterException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.tasks.TaskAction;
import org.gradle.work.DisableCachingByDefault;
import org.gradle.work.InputChanges;
import org.gradle.workers.WorkAction;
import org.gradle.workers.WorkParameters;
import org.gradle.workers.WorkQueue;

@DisableCachingByDefault
public abstract class ApplyGoogleJavaFormatTask extends ParentGoogleJavaFormatTask {
  @Inject
  public ApplyGoogleJavaFormatTask(ProjectLayout layout) {
    super(layout, "apply");
  }

  @TaskAction
  public void formatSources(InputChanges inputChanges) throws IOException {
    WorkQueue workQueue = getWorkQueue();
    List<File> sourceFiles = getIncrementalBatch(inputChanges);
    if (sourceFiles.isEmpty()) {
      return;
    }

    getLogger()
        .info(
            "Will apply formatting to {} source {} in this run.",
            sourceFiles.size(),
            sourceFiles.size() == 1 ? "file" : "files");

    for (var batch : batchSourceFiles(sourceFiles)) {
      workQueue.submit(
          ApplyGoogleJavaFormatAction.class,
          params -> {
            params.getTargetFiles().setFrom(batch);
          });
    }

    Path taskOutput = getOutputChangeListFile().get().getAsFile().toPath();
    Files.writeString(
        taskOutput, sourceFiles.stream().map(File::getPath).collect(Collectors.joining("\n")));

    workQueue.await();
  }

  public abstract static class ApplyGoogleJavaFormatAction
      implements WorkAction<ApplyGoogleJavaFormatAction.Parameters> {
    public interface Parameters extends WorkParameters {
      ConfigurableFileCollection getTargetFiles();
    }

    @Override
    public void execute() {
      var formatter = getFormatter();

      for (File inputFile : getParameters().getTargetFiles().getFiles()) {
        var inputPath = inputFile.toPath();
        try {
          String input = Files.readString(inputPath);
          String output = applyFormatter(formatter, input);

          if (!input.equals(output)) {
            Files.writeString(inputPath, output);
          }
        } catch (FormatterException | IOException e) {
          throw new RuntimeException("Could not format file: " + inputPath, e);
        }
      }
    }
  }
}
