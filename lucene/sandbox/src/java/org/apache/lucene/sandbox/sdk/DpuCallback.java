/* Copyright 2020 UPMEM. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 */

package org.apache.lucene.sandbox.sdk;

/**
 * The callback used in {DpuSetAsync#call}.
 *
 * @see DpuSetAsync#call
 */
public interface DpuCallback {
    /**
     * The method called on the DPU system (or on each DPU rank, depending on the provided options).
     *
     * @param set The DPU set targeted by the callback.
     * @param idx The rank index in the DPU system. -1 if called on the whole DPU system.
     * @exception DpuException When a DPU operation fails in the callback.
     * @see DpuSet
     */
    void call(DpuSet set, int idx) throws DpuException;
}

