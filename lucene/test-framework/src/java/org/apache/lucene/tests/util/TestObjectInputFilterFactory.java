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
package org.apache.lucene.tests.util;

import java.io.ObjectInputFilter;
import java.io.ObjectInputFilter.Status;
import java.lang.StackWalker.StackFrame;
import java.util.function.BinaryOperator;

/**
 * Make sure we do not allow Java serialization without custom filters anywhere while running tests.
 * This factory returns a "deny-all" deserialization filter if none was installed by the
 * deserializer. To allow Gradle's test runner to serialize its config, all calls from Gradle are
 * allowed.
 */
public final class TestObjectInputFilterFactory implements BinaryOperator<ObjectInputFilter> {

  /** An {@link ObjectInputFilter} that rejects any deserialization. */
  public static final ObjectInputFilter DENY_ALL_FILTER = _ -> Status.REJECTED;

  @Override
  public ObjectInputFilter apply(ObjectInputFilter curr, ObjectInputFilter next) {
    // if Gradle's deserializer is on the stack, return next filter (which is obviously null):
    if (StackWalker.getInstance()
        .walk(s -> s.anyMatch(TestObjectInputFilterFactory::isGradleSerializerStackFrame))) {
      return next;
    }
    // if no filter was configured, return our filter that rejects all deserialization:
    if (curr == null && next == null) {
      return DENY_ALL_FILTER;
    }
    // if code installs its own filter return it:
    return next;
  }

  private static boolean isGradleSerializerStackFrame(StackFrame f) {
    final String methodName = f.getMethodName(), className = f.getClassName();
    return ("deserializeWorker".equals(methodName)
        && "org.gradle.process.internal.worker.messaging.WorkerConfigSerializer".equals(className));
  }
}
