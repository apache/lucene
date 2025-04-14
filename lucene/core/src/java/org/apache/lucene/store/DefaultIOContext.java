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

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

record DefaultIOContext(Optional<ReadAdvice> readAdvice, FileOpenHint... hints)
    implements IOContext {

  public DefaultIOContext {
    Objects.requireNonNull(readAdvice);
    Objects.requireNonNull(hints);
    if (readAdvice.isPresent() && hints.length > 0)
      throw new IllegalArgumentException("Either ReadAdvice or hints can be specified, not both");
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
    if (readAdvice.isPresent())
      throw new IllegalArgumentException("ReadAdvice has been specified directly");
    // TODO: see if this is needed or not
    if (hints.length > 0)
      throw new IllegalArgumentException("Hints have already been specified");
    return new DefaultIOContext(Optional.empty(), hints);
  }

  private static final DefaultIOContext[] READADVICE_TO_IOCONTEXT =
      Arrays.stream(ReadAdvice.values())
          .map(r -> new DefaultIOContext(Optional.of(r)))
          .toArray(DefaultIOContext[]::new);

  @Override
  public DefaultIOContext withReadAdvice(ReadAdvice advice) {
    return READADVICE_TO_IOCONTEXT[advice.ordinal()];
  }
}
