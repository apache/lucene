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
package org.apache.lucene.util;

import java.util.Collection;

/**
 * This class provides ability to track the reference counts of a set of index files and
 * delete them when their counts decreased to 0.
 */
public class FileDeleter {

    public synchronized void incRef(Collection<String> fileNames) {

    }

    public synchronized void decRef(Collection<String> fileNames) {

    }

    public synchronized RefCount getRefCount(String fileName) {
        return null;
    }

    public synchronized boolean exists(String fileName) {
        return false;
    }

    public synchronized void deleteIfNoRef(String fileName) {

    }

    private synchronized void delete(Collection<String> toDelete) {

    }

    private synchronized void delete(String fileName) {

    }

    /** Tracks the reference count for a single index file: */
    public static final class RefCount {

        // fileName used only for better assert error messages
        final String fileName;
        boolean initDone;

        RefCount(String fileName) {
            this.fileName = fileName;
        }

        int count;

        public int IncRef() {
            if (!initDone) {
                initDone = true;
            } else {
                assert count > 0
                        : Thread.currentThread().getName()
                        + ": RefCount is 0 pre-increment for file \""
                        + fileName
                        + "\"";
            }
            return ++count;
        }

        public int DecRef() {
            assert count > 0
                    : Thread.currentThread().getName()
                    + ": RefCount is 0 pre-decrement for file \""
                    + fileName
                    + "\"";
            return --count;
        }
    }
}
