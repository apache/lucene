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
import org.apache.lucene.util.Constants;

record DefaultIOContext(ReadAdvice readAdvice, FileOpenHint... hints) implements IOContext {

  public DefaultIOContext {
    Objects.requireNonNull(readAdvice);
    Objects.requireNonNull(hints);

    // either hints or readadvice should be specified, not both
    if (hints.length > 0 && readAdvice != Constants.DEFAULT_READADVICE) {
      throw new IllegalArgumentException("Either readAdvice or hints should be specified");
    }
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

  private static final DefaultIOContext[] READADVICE_TO_IOCONTEXT =
      Arrays.stream(ReadAdvice.values())
          .map(DefaultIOContext::new)
          .toArray(DefaultIOContext[]::new);

  @Override
  public DefaultIOContext withReadAdvice(ReadAdvice advice) {
    return READADVICE_TO_IOCONTEXT[advice.ordinal()];
  }
}
