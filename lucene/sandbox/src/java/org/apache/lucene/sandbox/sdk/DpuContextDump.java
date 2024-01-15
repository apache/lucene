/* Copyright 2020 UPMEM. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 */

package org.apache.lucene.sandbox.sdk;

/**
 * A dump of a DPU internal context.
 *
 * @see Dpu#dump
 */
public final class DpuContextDump {
    private final boolean[] atomicBits;
    private final int[][] workRegisters;
    private final short[] pcs;
    private final boolean[] zeroFlags;
    private final boolean[] carryFlags;

    DpuContextDump(boolean[] atomicBits, int[] workRegisters, short[] pcs, boolean[] zeroFlags, boolean[] carryFlags) {
        int nrOfDpuThreads = pcs.length;
        int nrOfWorkRegistersPerThread = workRegisters.length / nrOfDpuThreads;

        this.atomicBits = atomicBits;

        this.workRegisters = new int[nrOfDpuThreads][nrOfWorkRegistersPerThread];

        for (int eachThread = 0; eachThread < nrOfDpuThreads; eachThread++) {
            System.arraycopy(workRegisters, eachThread * nrOfWorkRegistersPerThread, this.workRegisters[eachThread],
                    0, nrOfWorkRegistersPerThread);
        }

        this.pcs = pcs;
        this.zeroFlags = zeroFlags;
        this.carryFlags = carryFlags;
    }

    /**
     * @return The state of the DPU atomic bits.
     */
    public boolean[] getAtomicBits() {
        return atomicBits.clone();
    }

    /**
     * @return The work registers of each thread of the DPU.
     */
    public int[][] getWorkRegisters() {
        return workRegisters.clone();
    }

    /**
     * @return The PC value of each thread of the DPU.
     */
    public short[] getPcs() {
        return pcs.clone();
    }

    /**
     * @return The ZERO flag value of each thread of the DPU.
     */
    public boolean[] getZeroFlags() {
        return zeroFlags.clone();
    }

    /**
     * @return The CARRY flag value of each thread of the DPU.
     */
    public boolean[] getCarryFlags() {
        return carryFlags.clone();
    }
}
