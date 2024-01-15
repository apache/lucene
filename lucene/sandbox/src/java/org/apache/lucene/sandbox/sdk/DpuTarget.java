/* Copyright 2020 UPMEM. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 */

package org.apache.lucene.sandbox.sdk;

/**
 * DPU backend that can be used in this API.
 *
 * @see DpuProfile
 */
public enum DpuTarget {
    /**
     * A software functional simulator.
     */
    Simulator,
    /**
     * An UPMEM DIMM.
     */
    Hardware;

    @Override
    public String toString() {
        switch (this) {
            case Simulator:
                return "simulator";
            case Hardware:
                return "hw";
            default:
                throw new IllegalArgumentException();
        }
    }
}
