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
package org.apache.lucene;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.jar.Manifest;

/** Lucene's package information, including version. */
public final class LucenePackage {
  private static LucenePackage lazyInstance;
  private final String implementationVersion;

  private LucenePackage(String implementationVersion) {
    this.implementationVersion = implementationVersion;
  }

  /** Return Lucene's package, including version information. */
  public static synchronized LucenePackage get() {
    if (lazyInstance == null) {
      String version;

      Package p = LucenePackage.class.getPackage();
      version = p.getImplementationVersion();

      if (version == null) {
        var module = LucenePackage.class.getModule();
        if (module.isNamed()) {
          // Running as a module? Try parsing the manifest manually.
          InputStream is = null;
          try {
            is = module.getResourceAsStream("/META-INF/MANIFEST.MF");
            if (is != null) {
              Manifest m = new Manifest(is);
              version = m.getMainAttributes().getValue("Implementation-Version");
            }
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        }
      }

      lazyInstance = new LucenePackage(version);
    }

    return lazyInstance;
  }

  /** @return Return Lucene's implementation version or {@code null} if it cannot be determined. */
  public String getImplementationVersion() {
    return implementationVersion;
  }
}
