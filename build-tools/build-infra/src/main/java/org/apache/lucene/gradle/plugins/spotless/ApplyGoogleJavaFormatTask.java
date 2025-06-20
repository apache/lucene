package org.apache.lucene.gradle.plugins.spotless;

import com.google.googlejavaformat.java.FormatterException;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.tasks.TaskAction;
import org.gradle.work.DisableCachingByDefault;
import org.gradle.work.InputChanges;
import org.gradle.workers.WorkAction;
import org.gradle.workers.WorkParameters;
import org.gradle.workers.WorkQueue;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

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

        getLogger().info("Will apply formatting to {} source {} in this run.",
                sourceFiles.size(), sourceFiles.size() == 1 ? "file" : "files");

        for (var sourceFile : sourceFiles) {
            workQueue.submit(ApplyGoogleJavaFormatAction.class, params -> {
                params.getTargetFiles().setFrom(List.of(sourceFile));
            });
        }

        Path taskOutput = getOutputChangeListFile().get().getAsFile().toPath();
        Files.writeString(
                taskOutput,
                sourceFiles.stream().map(File::getPath).collect(Collectors.joining("\n")));

        workQueue.await();
    }

    public static abstract class ApplyGoogleJavaFormatAction implements WorkAction<ApplyGoogleJavaFormatAction.Parameters> {
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
                    String output = formatter.formatSourceAndFixImports(input);

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
