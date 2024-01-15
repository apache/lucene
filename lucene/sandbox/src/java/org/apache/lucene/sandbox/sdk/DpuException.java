/* Copyright 2020 UPMEM. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 */

package org.apache.lucene.sandbox.sdk;

/**
 * Exception thrown by the API.
 */
public class DpuException extends Exception {
    public DpuException(String message) {
        super(message);
    }
}
