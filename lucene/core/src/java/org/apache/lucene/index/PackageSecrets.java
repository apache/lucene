/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.index;

import org.apache.lucene.internal.tests.IndexPackageSecrets;

/**
 * Exposes certain package-private secrets to the test framework.
 *
 * @lucene.internal
 */
public final class PackageSecrets {
  private PackageSecrets() {}

  /**
   * This method returns a test accessor that exposes some internals to test infrastructure.
   * Everything here is internal, subject to change without notice and not publicly accessible.
   *
   * <p>Within the test infrastructure, do not call this method directly, instead use the static
   * factory methods on the corresponding {@code TestSecrets} class.
   *
   * @param accessToken A secret token to only permit instantiation via the corresponding secrets
   *     class.
   * @return An instance of the secrets class; the return type is hidden from the public API.
   * @lucene.internal
   */
  public static Object getTestSecrets(Object accessToken) {
    return new IndexPackageSecrets(accessToken) {

      @Override
      public IndexReader.CacheKey newCacheKey() {
        return new IndexReader.CacheKey();
      }

      @Override
      public void setIndexWriterMaxDocs(int limit) {
        IndexWriter.setMaxDocs(limit);
      }

      @Override
      public FieldInfosBuilder newFieldInfosBuilder(String softDeletesFieldName) {
        return new FieldInfosBuilder() {
          private FieldInfos.Builder builder =
              new FieldInfos.Builder(new FieldInfos.FieldNumbers(softDeletesFieldName));

          @Override
          public FieldInfosBuilder add(FieldInfo fi) {
            builder.add(fi);
            return this;
          }

          @Override
          public FieldInfos finish() {
            return builder.finish();
          }
        };
      }
    };
  }
}
