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
package org.apache.lucene.gradle.plugins.mrjar;

import java.util.stream.IntStream;
import javax.inject.Inject;
import org.gradle.api.Project;

public abstract class MrJarsExtension {
  public static final String NAME = "mrJars";

  @Inject
  public abstract Project getProject();

  /**
   * Immediately configures additional tasks and infrastructure for apijar stubs for the provided
   * list of Java feature JDKs.
   */
  public void setupFor(int... jdkVersions) {
    LuceneJavaCoreMrjarPlugin.setupMrJarInfrastructure(
        getProject(), IntStream.of(jdkVersions).boxed().toList());
  }
}
