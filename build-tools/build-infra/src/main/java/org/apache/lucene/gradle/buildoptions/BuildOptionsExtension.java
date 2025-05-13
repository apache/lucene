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
package org.apache.lucene.gradle.buildoptions;

import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.provider.Provider;

public abstract class BuildOptionsExtension {
  abstract NamedDomainObjectContainer<BuildOption> getAllOptions();

  /** Returns a lazy provider for the given option. */
  public Provider<String> optionValue(String name) {
    return getAllOptions().named(name).flatMap(BuildOption::asStringProvider);
  }

  public BuildOption getOption(String name) {
    return getAllOptions().named(name).get();
  }

  public boolean hasOption(String name) {
    return getAllOptions().findByName(name) != null;
  }

  public Provider<String> getAt(String name) {
    return optionValue(name);
  }

  public Provider<String> propertyMissing(String name) {
    return optionValue(name);
  }

  /** Build option with the default value. */
  public BuildOption addOption(String name, String description, String defaultValue) {
    return getAllOptions()
        .create(
            name,
            opt -> {
              opt.setDescription(description);
              opt.getDefaultValue()
                  .set(
                      new BuildOptionValue(
                          defaultValue, true, BuildOptionValueSource.EXPLICIT_VALUE));
            });
  }

  /** Build option with some dynamically computed value. */
  public BuildOption addOption(
      String name, String description, Provider<String> defaultValueProvider) {
    return getAllOptions()
        .create(
            name,
            opt -> {
              opt.setDescription(description);
              opt.getDefaultValue()
                  .set(
                      defaultValueProvider.map(
                          value ->
                              new BuildOptionValue(
                                  value, true, BuildOptionValueSource.COMPUTED_VALUE)));
            });
  }

  /** Build option without any default value. */
  public BuildOption addOption(String name, String description) {
    return getAllOptions().create(name, opt -> opt.setDescription(description));
  }
}
