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

/**
 * A hint that a file is not worth keeping in memory. It may be read often; what it lacks is reuse
 * worth caching for, because its reads repeat in no pattern a cache can exploit, or because it is
 * large enough next to the rest of the index that holding it would push out data that does benefit.
 *
 * <p>Close to {@link ReadOnceHint}, and weaker: it does not claim the file is read once, or in
 * order, or even that a read never revisits bytes another read has touched.
 */
public enum NoReuseHint implements IOContext.FileOpenHint {
  INSTANCE
}
