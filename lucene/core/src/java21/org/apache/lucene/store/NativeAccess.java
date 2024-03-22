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

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Optional;
import org.apache.lucene.util.Constants;

@SuppressWarnings("preview")
abstract class NativeAccess {

  /** Invoke the {@code madvise} call for the given {@link MemorySegment}. */
  public abstract void madvise(MemorySegment segment, IOContext context) throws IOException;

  /**
   * Return the NativeAccess instance for this platform. At moment we only support Linux and MacOS
   */
  public static Optional<NativeAccess> getImplementation() {
    if (Constants.LINUX || Constants.MAC_OS_X) {
      return PosixNativeAccess.getInstance();
    }
    return Optional.empty();
  }
}
