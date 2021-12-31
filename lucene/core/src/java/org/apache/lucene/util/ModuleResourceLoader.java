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
package org.apache.lucene.util;

import java.io.IOException;
import java.io.InputStream;

/**
 * Simple {@link ResourceLoader} that uses {@link Module#getResourceAsStream(String)} and {@link
 * Class#forName(Module,String)} to open resources and classes, respectively. Resource paths must be
 * absolute to module's root.
 *
 * <p>To use this class, you must open the module to the {@code org.apache.lucene.core} module,
 * otherwise resources can't be looked up.
 */
public final class ModuleResourceLoader implements ResourceLoader {
  private final Module module;

  /** Creates an instance using the given Java Module to load resources and classes. */
  public ModuleResourceLoader(Module module) {
    this.module = module;
  }

  @Override
  public InputStream openResource(String resource) throws IOException {
    final var stream = module.getResourceAsStream(resource);
    if (stream == null) throw new IOException("Resource not found: " + resource);
    return stream;
  }

  @Override
  public <T> Class<? extends T> findClass(String cname, Class<T> expectedType) {
    try {
      final var clazz = Class.forName(module, cname);
      if (clazz == null) throw new ClassNotFoundException(cname);
      return clazz.asSubclass(expectedType);
    } catch (Exception e) {
      throw new RuntimeException("Cannot load class: " + cname, e);
    }
  }
}
