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

import com.google.common.collect.Iterables;
import com.google.googlejavaformat.java.Formatter;
import com.google.googlejavaformat.java.FormatterException;
import com.google.googlejavaformat.java.ImportOrderer;
import com.google.googlejavaformat.java.JavaFormatterOptions;
import com.google.googlejavaformat.java.RemoveUnusedImports;
import java.io.File;
import java.util.List;
import java.util.stream.StreamSupport;
import javax.inject.Inject;
import org.gradle.api.DefaultTask;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.FileType;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.work.ChangeType;
import org.gradle.work.FileChange;
import org.gradle.work.Incremental;
import org.gradle.work.InputChanges;
import org.gradle.workers.WorkQueue;
import org.gradle.workers.WorkerExecutor;

abstract class ParentGoogleJavaFormatTask extends DefaultTask {
  /***
   * The number of files to pass to a work action in a single batch.
   */
  @Input
  public abstract Property<Integer> getBatchSize();

  @Incremental
  @InputFiles
  @PathSensitive(PathSensitivity.RELATIVE)
  public abstract ConfigurableFileCollection getSourceFiles();

  @OutputFile
  public abstract RegularFileProperty getOutputChangeListFile();

  @Inject
  protected abstract WorkerExecutor getWorkerExecutor();

  public ParentGoogleJavaFormatTask(ProjectLayout layout, String gjfTask) {
    getOutputChangeListFile()
        .convention(layout.getBuildDirectory().file("gjf-" + gjfTask + ".txt"));
    getBatchSize().convention(1);
  }

  protected Iterable<List<File>> batchSourceFiles(List<File> sourceFiles) {
    return Iterables.partition(sourceFiles, getBatchSize().get());
  }

  protected static Formatter getFormatter() {
    JavaFormatterOptions options =
        JavaFormatterOptions.builder()
            .style(JavaFormatterOptions.Style.GOOGLE)
            .formatJavadoc(true)
            .reorderModifiers(true)
            .build();
    return new Formatter(options);
  }

  protected List<File> getIncrementalBatch(InputChanges inputChanges) {
    return StreamSupport.stream(inputChanges.getFileChanges(getSourceFiles()).spliterator(), false)
        .filter(
            fileChange -> {
              return fileChange.getFileType() == FileType.FILE
                  && (fileChange.getChangeType() == ChangeType.ADDED
                      || fileChange.getChangeType() == ChangeType.MODIFIED);
            })
        .map(FileChange::getFile)
        .toList();
  }

  protected static String applyFormatter(Formatter formatter, String input)
      throws FormatterException {
    // Correct line endings, if there are any oddities.
    if (input.indexOf('\r') >= 0) {
      // replace windows sequences first,
      input = input.replace("\r\n", "\n");
      // then just remove any CRs
      input = input.replaceAll("\\r", "");
    }

    // Add trailing LF if the last character isn't an LF already. We don't
    // care about blanks because we'll reformat everything anyway.
    if (!input.isEmpty() && input.charAt(input.length() - 1) != '\n') {
      input = input + "\n";
    }

    input = ImportOrderer.reorderImports(input, JavaFormatterOptions.Style.GOOGLE);
    input = RemoveUnusedImports.removeUnusedImports(input);
    input = formatter.formatSource(input);
    return input;
  }

  @Internal
  protected WorkQueue getWorkQueue() {
    // TODO: maybe fork a separate jvm so that we can pass open-module settings there and fine-tune
    // the jvm for the task?
    return getWorkerExecutor().noIsolation();
  }
}
