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
package org.apache.lucene.gradle.plugins.regenerate;

import org.gradle.api.provider.SetProperty;

/**
 * An extension added to all tasks that regenerate some Lucene sources. The extension can be used to
 * add inputs and outputs used to compute hash signatures, which in turn control if the derived
 * resources haven't been altered (with respect to their sources).
 */
public abstract class RegenerateTaskExtension {
  public abstract SetProperty<String> getIgnoredInputs();
}
