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
package org.apache.lucene.codecs.lucene103.blocktree.art;

import org.apache.lucene.util.BytesRef;

/**
 * The output describing the term block the prefix point to.
 *
 * @param fp the file pointer to the on-disk terms block which a trie node points to.
 * @param hasTerms false if this on-disk block consists entirely of pointers to child blocks.
 * @param floorData will be non-null when a large block of terms sharing a single trie prefix is
 *     split into multiple on-disk blocks.
 */
public record Output(long fp, boolean hasTerms, BytesRef floorData) {
}
