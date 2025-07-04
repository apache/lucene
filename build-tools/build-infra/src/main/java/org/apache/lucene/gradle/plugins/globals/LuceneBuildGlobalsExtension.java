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

import org.gradle.api.provider.Property;

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
}
