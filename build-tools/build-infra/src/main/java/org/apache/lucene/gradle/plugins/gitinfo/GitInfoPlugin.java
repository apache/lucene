package org.apache.lucene.gradle.plugins.gitinfo;

import org.gradle.api.GradleException;
import org.gradle.api.Plugin;
import org.gradle.api.Project;

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
  }
}
