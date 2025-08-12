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
package org.apache.lucene.gradle.plugins.java;

import org.gradle.api.DefaultTask;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.Optional;
import org.gradle.internal.jvm.Jvm;

/** A temporary stub before all of RenderJavadocTask is ported from the gradle script. */
public abstract class RenderJavadocTaskBase extends DefaultTask {
  @Optional
  @Input
  public abstract Property<String> getExecutable();

  @Input
  @Optional
  public abstract ListProperty<String> getExtraOpts();

  public RenderJavadocTaskBase() {
    getExecutable()
        .convention(
            getProject()
                .getProviders()
                .provider(() -> Jvm.current().getJavadocExecutable().toString()));

    getExtraOpts().set(getProject().getObjects().listProperty(String.class));
  }
}
