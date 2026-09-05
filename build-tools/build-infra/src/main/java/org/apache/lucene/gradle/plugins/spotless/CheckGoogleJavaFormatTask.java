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

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.googlejavaformat.java.FormatterException;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;
import org.eclipse.jgit.diff.DiffFormatter;
import org.eclipse.jgit.diff.EditList;
import org.eclipse.jgit.diff.MyersDiff;
import org.eclipse.jgit.diff.RawText;
import org.eclipse.jgit.diff.RawTextComparator;
import org.gradle.api.GradleException;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.FileSystemOperations;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.TaskAction;
import org.gradle.internal.logging.text.StyledTextOutput;
import org.gradle.internal.logging.text.StyledTextOutput.Style;
import org.gradle.internal.logging.text.StyledTextOutputFactory;
import org.gradle.work.DisableCachingByDefault;
import org.gradle.work.InputChanges;
import org.gradle.workers.WorkAction;
import org.gradle.workers.WorkParameters;
import org.gradle.workers.WorkQueue;

/**
 * Verifies formatting against google java format.
 *
 * <p>Diffing code is borrowed from <a
 * href="https://github.com/diffplug/spotless/blob/main/lib-extra/src/main/java/com/diffplug/spotless/extra/integration/DiffMessageFormatter.java#L46">spotless</a>.
 */
@DisableCachingByDefault
public abstract class CheckGoogleJavaFormatTask extends ParentGoogleJavaFormatTask {
  @Input
  public abstract Property<Boolean> getColorizedOutput();

  @Input
  public abstract Property<Integer> getOutputLimit();

  @Inject
  protected abstract FileSystemOperations getFilesystemOperations();

  @Inject
  protected abstract StyledTextOutputFactory getStyledOutput();

  @Inject
  public CheckGoogleJavaFormatTask(ProjectLayout layout) {
    super(layout, "check");
    getColorizedOutput().convention(true);
    getOutputLimit().convention(1024 * 1024 * 2);
  }

  @TaskAction
  public void formatSources(InputChanges inputChanges) throws IOException {
    List<File> sourceFiles = getIncrementalBatch(inputChanges);

    var fileStates = readFileStatesFrom(getFileStateCache());
    sourceFiles = maybeRefilter(fileStates, sourceFiles);

    getLogger()
        .info(
            "Will check the formatting of {} source {} in this run.",
            sourceFiles.size(),
            sourceFiles.size() == 1 ? "file" : "files");

    if (!sourceFiles.isEmpty()) {
      int outputSeq = 0;
      File tempDir = new File(getTemporaryDir(), "changes");
      if (Files.exists(tempDir.toPath())) {
        getFilesystemOperations().delete(spec -> spec.delete(tempDir));
      }
      Files.createDirectory(tempDir.toPath());

      WorkQueue workQueue = getWorkQueue();
      for (List<File> batch : batchSourceFiles(sourceFiles)) {
        final int seq = outputSeq;
        workQueue.submit(
            CheckGoogleJavaFormatAction.class,
            params -> {
              params.getTargetFiles().setFrom(batch);
              params.getOutputFile().set(new File(tempDir, "gjf-check-" + seq + ".txt"));
            });
        outputSeq++;
      }

      // Wait for all jobs.
      workQueue.await();

      // Check if there are any changes.
      try (Stream<Path> stream = Files.list(tempDir.toPath()).sorted()) {
        var allChanges = stream.toList();

        if (!allChanges.isEmpty()) {
          var errorMsg =
              "java file(s) have google-java-format violations (run './gradlew tidy' to fix).";

          if (getColorizedOutput().getOrElse(false)) {
            printChangesToColorizedTerminal(allChanges, errorMsg);
          } else {
            printChangesToLogger(allChanges, errorMsg);
          }

          throw new GradleException(errorMsg);
        }
      }

      updateFileStates(fileStates, sourceFiles);
      writeFileStates(getFileStateCache(), fileStates);
    }

    Path taskOutput = getOutputChangeListFile().get().getAsFile().toPath();
    Files.writeString(
        taskOutput, sourceFiles.stream().map(File::getPath).collect(Collectors.joining("\n")));
  }

  private void printChangesToColorizedTerminal(List<Path> allChanges, String errorMsg)
      throws IOException {
    var out = getStyledOutput().create(this.getClass());

    out.withStyle(Style.Failure).append(errorMsg).append(" An overview diff of changes:\n");

    var normalLineStyle = Style.Normal;
    var removedLineStyle = Style.Failure;
    var addedLineStyle = Style.Success;
    var fileNameLineStyle = Style.Description;

    int limit = getOutputLimit().get();
    int writtenChars = 0;
    for (Path changeset : allChanges) {
      if (writtenChars > limit) {
        out.append("\n(...and more, omitted)");
        break;
      }

      String diff = Files.readString(changeset);
      for (var line : Splitter.on('\n').split(diff)) {
        StyledTextOutput currentStyle;
        if (line.startsWith("@@") || line.startsWith("==")) {
          currentStyle = out.style(fileNameLineStyle);
        } else if (line.startsWith("-")) {
          currentStyle = out.style(removedLineStyle);
        } else if (line.startsWith("+")) {
          currentStyle = out.style(addedLineStyle);
        } else {
          currentStyle = out.style(normalLineStyle);
        }

        currentStyle.append(line).append("\n");
      }

      writtenChars += diff.length();
    }
    out.println();
  }

  private void printChangesToLogger(List<Path> allChanges, String errorMsg) throws IOException {
    StringBuilder sb = new StringBuilder();
    int limit = getOutputLimit().get();
    for (Path changeset : allChanges) {
      if (sb.length() > limit) {
        sb.append("\n(...and more, omitted)");
        break;
      }

      sb.append(Files.readString(changeset));
      sb.append("\n");
    }

    getLogger().error(errorMsg + " An overview diff of changes:\n" + sb);
  }

  public abstract static class CheckGoogleJavaFormatAction
      implements WorkAction<CheckGoogleJavaFormatAction.Parameters> {
    public interface Parameters extends WorkParameters {
      ConfigurableFileCollection getTargetFiles();

      RegularFileProperty getOutputFile();
    }

    @Override
    public void execute() {
      var formatter = getFormatter();
      var outputFile = getParameters().getOutputFile().get().getAsFile().toPath();
      Writer diffOutput = null;
      for (File inputFile : getParameters().getTargetFiles().getFiles()) {
        var inputPath = inputFile.toPath();
        try {
          String input = Files.readString(inputPath);
          String expected = applyFormatter(formatter, input);

          if (!input.equals(expected)) {
            if (diffOutput == null) {
              diffOutput = Files.newBufferedWriter(outputFile, StandardCharsets.UTF_8);
            }

            diffOutput.write("== " + inputPath + "\n");
            writeDiff(diffOutput, input, expected);
          }
        } catch (FormatterException | IOException e) {
          throw new RuntimeException("Could not format file: " + inputPath, e);
        }
      }

      try {
        if (diffOutput != null) {
          diffOutput.close();
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    /**
     * Let's try to display a short diff of what's changed. This is informational only, really -
     * nobody should care (or try to fix those manually).
     */
    private static void writeDiff(Writer diffOutput, String input, String expected)
        throws IOException {
      var difference = diffWhitespaceLineEndings(input, expected, true, true);
      diffOutput.write(difference);
      diffOutput.write("\n");
    }

    /** Returns a git-style diff between the two unix strings. */
    private static String diffWhitespaceLineEndings(
        String dirty, String clean, boolean whitespace, boolean lineEndings) throws IOException {
      dirty = visibleWhitespaceLineEndings(dirty, whitespace, lineEndings);
      clean = visibleWhitespaceLineEndings(clean, whitespace, lineEndings);

      RawText a = new RawText(dirty.getBytes(StandardCharsets.UTF_8));
      RawText b = new RawText(clean.getBytes(StandardCharsets.UTF_8));
      EditList edits = new EditList();
      edits.addAll(MyersDiff.INSTANCE.diff(RawTextComparator.DEFAULT, a, b));

      ByteArrayOutputStream out = new ByteArrayOutputStream();
      try (DiffFormatter formatter = new DiffFormatter(out)) {
        formatter.format(edits, a, b);
      }
      String formatted = out.toString(StandardCharsets.UTF_8);

      // we don't need the diff to show this, since we display newlines ourselves
      formatted = formatted.replace("\\ No newline at end of file\n", "");

      CharMatcher NEWLINE_MATCHER = CharMatcher.is('\n');
      return NEWLINE_MATCHER.trimTrailingFrom(formatted);
    }

    /**
     * Makes the whitespace and/or the lineEndings visible.
     *
     * <p>MyersDiff wants inputs with only unix line endings. So this ensures that that is the case.
     */
    private static String visibleWhitespaceLineEndings(
        String input, boolean whitespace, boolean lineEndings) {
      final char MIDDLE_DOT = '·';

      if (whitespace) {
        input = input.replace(' ', MIDDLE_DOT).replace("\t", "\\t");
      }
      if (lineEndings) {
        input = input.replace("\r", "\\r");
      } else {
        // we want only \n, so if we didn't replace them above, we'll replace them here.
        input = input.replace("\r", "");
      }
      return input;
    }
  }
}
