package org.apache.lucene.gradle.plugins.gitinfo;

import java.nio.file.Files;
import java.nio.file.Path;
import org.gradle.api.GradleException;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.file.Directory;

public class GitInfoPlugin implements Plugin<Project> {
  @Override
  public void apply(Project project) {
    if (project != project.getRootProject()) {
      throw new GradleException("This plugin is applicable to the rootProject only.");
    }

    var gitInfoProvider =
        project
            .getProviders()
            .of(
                GitInfoValueSource.class,
                spec -> {
                  spec.getParameters().getRootProjectDir().set(project.getProjectDir());
                });

    var gitInfoExtension =
        project.getExtensions().create(GitInfoExtension.NAME, GitInfoExtension.class);

    gitInfoExtension.getGitInfo().value(gitInfoProvider).finalizeValueOnRead();

    gitInfoExtension
        .getDotGitDir()
        .convention(
            project
                .getProviders()
                .provider(
                    () -> {
                      Directory projectDirectory =
                          project.getRootProject().getLayout().getProjectDirectory();
                      Path gitLocation = projectDirectory.getAsFile().toPath().resolve(".git");
                      if (!Files.exists(gitLocation)) {
                        throw new GradleException(
                            "Can't locate .git (probably not a git clone?): "
                                + gitLocation.toAbsolutePath());
                      }

                      if (Files.isDirectory(gitLocation)) {
                        return projectDirectory.dir(".git");
                      } else if (Files.isRegularFile(gitLocation)) {
                        return projectDirectory.file(".git");
                      } else {
                        throw new GradleException(
                            "Panic, .git location not a directory or file: "
                                + gitLocation.toAbsolutePath());
                      }
                    }))
        .finalizeValueOnRead();
  }
}
