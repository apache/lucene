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
package org.apache.lucene.gradle.plugins.globals;

import com.carrotsearch.gradle.buildinfra.buildoptions.BuildOptionsExtension;
import java.io.File;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicReference;
import javax.inject.Inject;
import org.apache.lucene.gradle.plugins.misc.QuietExec;
import org.gradle.api.Action;
import org.gradle.api.GradleException;
import org.gradle.api.JavaVersion;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.provider.Property;
import org.gradle.process.ExecOperations;
import org.gradle.process.ExecResult;
import org.gradle.process.ExecSpec;

/** Global build constants. */
public abstract class LuceneBuildGlobalsExtension {
  public static final String NAME = "buildGlobals";

  /** Base Lucene version ({@code x.y.z}). */
  public String baseVersion;

  /** Major Lucene version ({@code x} in {@code x.y.z}). */
  public String majorVersion;

  /** {@code true} if this build is a snapshot build. */
  public boolean snapshotBuild;

  /** Build date ({@code yyyy-MM-dd}. */
  public String buildDate;

  /** Build time ({@code HH:mm:ss}. */
  public String buildTime;

  /** Build year ({@code yyyy}. */
  public String buildYear;

  /**
   * {@code true} if this build runs on a CI server. This is a heuristic looking for typical env.
   * variables:
   *
   * <ul>
   *   <li>{@code CI}: <a
   *       href="https://docs.github.com/en/actions/learn-github-actions/environment-variables">Github</a>
   *   <li>{@code JENKINS_} or {@code HUDSON_}: <a
   *       href="https://jenkins.thetaphi.de/env-vars.html/">Jenkins/Hudson</a>
   * </ul>
   */
  public boolean isCIBuild;

  /** Returns per-project seed for randomization. */
  public abstract Property<Long> getProjectSeedAsLong();

  /** Return the root randomization seed */
  public abstract Property<String> getRootSeed();

  /** Return the root randomization seed as a {@code long} value. */
  public abstract Property<Long> getRootSeedAsLong();

  /** Minimum Java version required for this version of Lucene. */
  public abstract Property<JavaVersion> getMinJavaVersion();

  /** If set, returns certain flags helpful for configuring the build for the intellij idea IDE. */
  public abstract Property<IntellijIdea> getIntellijIdea();

  /**
   * Returns the path to the provided named external tool. Developers may set up different tool
   * paths using local build options.
   */
  public String externalTool(String name) {
    var buildOptions =
        getProject().getRootProject().getExtensions().getByType(BuildOptionsExtension.class);
    var optionKey = "lucene.tool." + name;
    if (!buildOptions.hasOption(optionKey)) {
      throw new GradleException(
          "External tool of this name has no corresponding build option: " + optionKey);
    }
    return buildOptions.getOption(optionKey).asStringProvider().get();
  }

  /**
   * An immediate equivalent of the {@link org.apache.lucene.gradle.plugins.misc.QuietExec} task.
   * This should be avoided but is sometimes handy.
   */
  public void quietExec(Task owner, Action<ExecSpec> configureAction) {
    try {
      File outputFile = File.createTempFile("exec-output-", ".txt", owner.getTemporaryDir());
      AtomicReference<String> executable = new AtomicReference<>();
      ExecResult result;
      try (OutputStream os =
          new FilterOutputStream(Files.newOutputStream(outputFile.toPath())) {
            @Override
            public void close() {
              // no-op. we close this stream manually.
            }
          }) {
        result =
            getExecOps()
                .exec(
                    spec -> {
                      configureAction.execute(spec);

                      spec.setStandardInput(InputStream.nullInputStream());
                      spec.setStandardOutput(os);
                      spec.setErrorOutput(os);
                      spec.setIgnoreExitValue(true);

                      executable.set(spec.getExecutable());
                    });
      }
      QuietExec.processResult(result, outputFile, executable.get(), owner.getLogger());
    } catch (IOException e) {
      throw new GradleException("quietExec failed: " + e.getMessage(), e);
    }
  }

  /** For internal use. */
  @Inject
  protected abstract ExecOperations getExecOps();

  /** For internal use. */
  @Inject
  protected abstract Project getProject();
}
