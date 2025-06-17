package org.apache.lucene.gradle.plugins.gitinfo;

import org.gradle.api.file.FileSystemLocation;
import org.gradle.api.provider.MapProperty;
import org.gradle.api.provider.Property;

public abstract class GitInfoExtension {
  public static final String NAME = "gitinfo";

  /**
   * @return Return a map of key-value pairs extracted from the current git status.
   */
  public abstract MapProperty<String, String> getGitInfo();

  /**
   * @return Return the location of {@code .git} directory (or file, if worktrees are used).
   */
  public abstract Property<FileSystemLocation> getDotGitDir();
}
