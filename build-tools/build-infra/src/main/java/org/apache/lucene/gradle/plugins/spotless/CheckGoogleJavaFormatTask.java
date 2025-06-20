package org.apache.lucene.gradle.plugins.spotless;

import com.github.difflib.text.DiffRow;
import com.github.difflib.text.DiffRowGenerator;
import com.google.googlejavaformat.java.FormatterException;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;
import org.gradle.api.GradleException;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.FileSystemOperations;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.tasks.TaskAction;
import org.gradle.work.DisableCachingByDefault;
import org.gradle.work.InputChanges;
import org.gradle.workers.WorkAction;
import org.gradle.workers.WorkParameters;
import org.gradle.workers.WorkQueue;

@DisableCachingByDefault
public abstract class CheckGoogleJavaFormatTask extends ParentGoogleJavaFormatTask {

  @Inject
  protected abstract FileSystemOperations getFilesystemOperations();

  @Inject
  public CheckGoogleJavaFormatTask(ProjectLayout layout) {
    super(layout, "check");
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
            "Will check the formatting of {} source {} in this run.",
            sourceFiles.size(),
            sourceFiles.size() == 1 ? "file" : "files");

    int outputSeq = 0;
    File tempDir = new File(getTemporaryDir(), "changes");
    if (Files.exists(tempDir.toPath())) {
      getFilesystemOperations().delete(spec -> spec.delete(tempDir));
    }
    Files.createDirectory(tempDir.toPath());

    for (var sourceFile : sourceFiles) {
      final int seq = outputSeq;
      workQueue.submit(
          CheckGoogleJavaFormatAction.class,
          params -> {
            params.getTargetFiles().setFrom(List.of(sourceFile));
            params.getOutputFile().set(new File(tempDir, "gjf-check-" + seq + ".txt"));
          });
      outputSeq++;
    }

    workQueue.await();

    // Check if there are any changes.
    try (Stream<Path> stream = Files.list(tempDir.toPath()).sorted()) {
      var allChanges = stream.toList();
      StringBuilder sb = new StringBuilder();
      for (Path changeset : allChanges) {
        if (sb.length() > 1024 * 1024 * 2) {
          sb.append("(...and more files, omitted)");
          break;
        }

        sb.append(Files.readString(changeset));
        sb.append("\n");
      }

      if (!allChanges.isEmpty()) {
        throw new GradleException(
            "The following file(s) have format violations (run './gradlew tidy' to fix):\n" + sb);
      }
    }

    Path taskOutput = getOutputChangeListFile().get().getAsFile().toPath();
    Files.writeString(
        taskOutput, sourceFiles.stream().map(File::getPath).collect(Collectors.joining("\n")));
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

    static final DiffRowGenerator diffGenerator =
        DiffRowGenerator.create()
            .ignoreWhiteSpaces(false)
            .reportLinesUnchanged(false)
            .showInlineDiffs(false)
            .mergeOriginalRevised(true)
            .inlineDiffByWord(true)
            .oldTag(_ -> "")
            .newTag(_ -> "")
            .build();

    // TODO: improve this or reuse spotless's diffing alg (which is from jgit).
    private void writeDiff(Writer diffOutput, String input, String expected) throws IOException {
      List<DiffRow> diffRows =
          diffGenerator.generateDiffRows(
              Arrays.asList(input.split("[\\r\\n]+")), Arrays.asList(expected.split("[\\r\\n]+")));

      StringBuilder value = new StringBuilder();
      for (var diffRow : diffRows) {
        switch (diffRow.getTag()) {
          case EQUAL:
            continue;
          case DELETE:
            value.append("- ").append(diffRow.getOldLine());
            break;
          case INSERT:
            value.append("+ ").append(diffRow.getNewLine());
            break;
          case CHANGE:
            value.append("- ").append(diffRow.getOldLine());
            value.append("\n");
            value.append("+ ").append(diffRow.getNewLine());
            break;
          default:
            throw new RuntimeException();
        }
        value.append("\n");
      }

      diffOutput.write(value.toString());
      diffOutput.write("\n");
    }
  }
}
