package org.apache.lucene.gradle.plugins.spotless;

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
