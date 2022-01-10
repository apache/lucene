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
package org.apache.lucene.index;

import java.io.IOException;

public abstract class LastingFilterDirectoryReader extends FilterDirectoryReader {
    protected final CacheHelper readerCacheHelper;

    /**
     * Create a new FilterDirectoryReader that filters a passed in DirectoryReader, using the supplied
     * SubReaderWrapper to wrap its subreader.
     *
     * @param in      the DirectoryReader to filter
     * @param wrapper the SubReaderWrapper to use to wrap subreaders
     */
    public LastingFilterDirectoryReader(DirectoryReader in, SubReaderWrapper wrapper) throws IOException {
        super(in, wrapper);
        this.readerCacheHelper = DelegatingCacheHelper.from(in.getReaderCacheHelper());
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return readerCacheHelper;
    }
}
