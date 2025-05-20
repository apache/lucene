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
package org.apache.lucene.store;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

record DefaultIOContext(Set<FileOpenHint> hints) implements IOContext {

  public DefaultIOContext(Set<FileOpenHint> hints) {
    this.hints = Set.copyOf(Objects.requireNonNull(hints));

    // there should only be one hint of each type in the IOContext
    Map<Class<? extends FileOpenHint>, List<FileOpenHint>> hintClasses =
        hints.stream().collect(Collectors.groupingBy(IOContext.FileOpenHint::getClass));
    for (var hintType : hintClasses.entrySet()) {
      if (hintType.getValue().size() > 1) {
        throw new IllegalArgumentException("Multiple hints of type " + hintType + " specified");
      }
    }
  }

  public DefaultIOContext(FileOpenHint... hints) {
    this(Set.of(hints));
  }

  @Override
  public Context context() {
    return Context.DEFAULT;
  }

  @Override
  public MergeInfo mergeInfo() {
    return null;
  }

  @Override
  public FlushInfo flushInfo() {
    return null;
  }

  @Override
  public IOContext withHints(FileOpenHint... hints) {
    return new DefaultIOContext(hints);
  }
}
