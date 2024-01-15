/* Copyright 2020 UPMEM. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 */

package org.apache.lucene.sandbox.sdk;

import java.lang.annotation.Native;
import java.util.ArrayList;
import java.util.List;

/**
 * A dump of a DPU fault context.
 *
 * @see Dpu#debugInit
 */
public final class DpuFaultDump {
    @Native
    final long faultContextPointer;

    private final Integer bkpFaultThreadIndex;
    private final Integer dmaFaultThreadIndex;
    private final Integer memFaultThreadIndex;
    private final Integer bkpFaultId;
    private final List<Byte> runningThreads;
    private final short[] pcs;

    DpuFaultDump(long faultContextPointer, boolean bkpFault, boolean dmaFault, boolean memFault,
                 int bkpFaultThreadIndex, int dmaFaultThreadIndex, int memFaultThreadIndex,
                 int bkpFaultId, byte[] scheduling, short[] pcs) {
        this.faultContextPointer = faultContextPointer;
        this.bkpFaultThreadIndex = bkpFault ? bkpFaultThreadIndex : null;
        this.dmaFaultThreadIndex = dmaFault ? dmaFaultThreadIndex : null;
        this.memFaultThreadIndex = memFault ? memFaultThreadIndex : null;
        this.bkpFaultId = bkpFault ? bkpFaultId : null;
        this.pcs = pcs;

        int nrOfRunningThreads = 0;

        for (byte eachThread : scheduling) {
            if (eachThread != (byte) 0xFF) nrOfRunningThreads++;
        }

        this.runningThreads = new ArrayList<>(nrOfRunningThreads);

        for (int eachThreadIndex = 0; eachThreadIndex < scheduling.length; eachThreadIndex++) {
            byte eachThread = scheduling[eachThreadIndex];

            if (eachThread != (byte) 0xFF) {
                this.runningThreads.add((byte) eachThreadIndex);
            }
        }
    }

    /**
     * @return The DPU thread which triggered the BKP fault, if any (null otherwise).
     */
    public Integer getBkpFaultThreadIndex() {
        return bkpFaultThreadIndex;
    }

    /**
     * @return The DPU thread which triggered the DMA fault, if any (null otherwise).
     */
    public Integer getDmaFaultThreadIndex() {
        return dmaFaultThreadIndex;
    }

    /**
     * @return The DPU thread which triggered the MEM fault, if any (null otherwise).
     */
    public Integer getMemFaultThreadIndex() {
        return memFaultThreadIndex;
    }

    /**
     * @return The specific BKP fault ID, if any (null otherwise).
     */
    public Integer getBkpFaultId() {
        return bkpFaultId;
    }

    /**
     * @return The set of DPU threads running when the fault occurred.
     */
    public List<Byte> getRunningThreads() {
        return runningThreads;
    }

    /**
     * @return The PC value of each DPU thread.
     */
    public short[] getPcs() {
        return pcs.clone();
    }
}
