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
package org.apache.lucene.internal;

import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.lucene.store.MMapDirectory;

/** Returns various diagnostic information for modular integration tests. */
public class ModuleCheck implements Supplier<Map<String, String>> {
  @Override
  public Map<String, String> get() {
    return Map.ofEntries(Map.entry("hello", "world"), mmapIsAccessible());
  }

  private Map.Entry<String, String> mmapIsAccessible() {
    return Map.entry("unmap.supported", Objects.toString(MMapDirectory.UNMAP_SUPPORTED));
  }
}
