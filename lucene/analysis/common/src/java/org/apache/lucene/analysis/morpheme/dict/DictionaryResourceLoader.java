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
package org.apache.lucene.analysis.morpheme.dict;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.lucene.util.IOUtils;

/**
 * A utility to load morpheme dictionary resources from the classpath or file system.
 *
 * <p>NOTE: To make this work on JMS, you need to open the package that contains resources to
 * analysis-common module.
 */
public class DictionaryResourceLoader {

  /** Used to specify where (dictionary) resources get loaded from. */
  public enum ResourceScheme {
    CLASSPATH,
    FILE
  }

  private final ResourceScheme resourceScheme;
  private final String resourcePath;
  private final Class<?> ownerClass;

  /** Creates a dictionary resource loader. */
  public DictionaryResourceLoader(
      ResourceScheme resourceScheme, String resourcePath, Class<?> ownerClass) {
    this.resourceScheme = resourceScheme;
    this.resourcePath = resourcePath;
    // the actual owner of the resources; can resident in another module.
    this.ownerClass = ownerClass;
  }

  /** Read dictionary data file with {@code suffix} from the resource path */
  public final InputStream getResource(String suffix) throws IOException {
    switch (resourceScheme) {
      case CLASSPATH:
        return getClassResource(resourcePath + suffix, ownerClass);
      case FILE:
        return Files.newInputStream(Paths.get(resourcePath + suffix));
      default:
        throw new IllegalStateException("unknown resource scheme " + resourceScheme);
    }
  }

  private static InputStream getClassResource(String path, Class<?> clazz) throws IOException {
    return IOUtils.requireResourceNonNull(clazz.getResourceAsStream(path), path);
  }
}
