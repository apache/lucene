package org.apache.lucene.gradle.plugins.gitinfo;

import org.gradle.api.provider.MapProperty;

public abstract class GitInfoExtension {
  public static final String NAME = "gitinfo";

  public abstract MapProperty<String, String> getGitInfo();
}
