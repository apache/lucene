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

/**
 * Experimental {@link org.apache.lucene.store.Directory} implementations. Currently {@link
 * org.apache.lucene.sandbox.store.IoUringDirectory}, which serves the raw float32 KNN vector file
 * via io_uring with {@code O_DIRECT} for larger-than-RAM full-precision rerank (opt-in; requires a
 * recent Linux kernel, {@code liburing-ffi}, and {@code --enable-native-access}).
 */
package org.apache.lucene.sandbox.store;
