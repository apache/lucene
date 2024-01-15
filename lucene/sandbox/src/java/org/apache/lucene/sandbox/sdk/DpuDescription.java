/* Copyright 2020 UPMEM. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 */

package org.apache.lucene.sandbox.sdk;

/**
 * Hardware characteristics of a DpuSystem.
 */
public final class DpuDescription {
    private final int chipId;
    private final int backendId;
    private final int nrOfControlInterfaces;
    private final int nrOfDpusPerControlInterface;
    private final int mramSizeInBytes;
    private final int wramSizeInWords;
    private final int iramSizeInInstructions;
    private final int nrOfThreadsPerDpu;
    private final int nrOfAtomicBitsPerDpu;
    private final int nrOfNotifyBitsPerDpu;
    private final int nrOfWorkRegistersPerThread;

    DpuDescription(int chipId, int backendId, int nrOfControlInterfaces, int nrOfDpusPerControlInterface,
                   int mramSizeInBytes, int wramSizeInWords, int iramSizeInInstructions,
                   int nrOfThreadsPerDpu, int nrOfAtomicBitsPerDpu, int nrOfNotifyBitsPerDpu, int nrOfWorkRegistersPerThread) {
        this.chipId = chipId;
        this.backendId = backendId;
        this.nrOfControlInterfaces = nrOfControlInterfaces;
        this.nrOfDpusPerControlInterface = nrOfDpusPerControlInterface;
        this.mramSizeInBytes = mramSizeInBytes;
        this.wramSizeInWords = wramSizeInWords;
        this.iramSizeInInstructions = iramSizeInInstructions;
        this.nrOfThreadsPerDpu = nrOfThreadsPerDpu;
        this.nrOfAtomicBitsPerDpu = nrOfAtomicBitsPerDpu;
        this.nrOfNotifyBitsPerDpu = nrOfNotifyBitsPerDpu;
        this.nrOfWorkRegistersPerThread = nrOfWorkRegistersPerThread;
    }

    /**
     * @return The chip version number.
     */
    public int getChipId() {
        return chipId;
    }

    /**
     * @return The DPU backend id.
     */
    public int getBackendId() {
        return backendId;
    }

    /**
     * @return The number of Control Interfaces in a DPU rank.
     */
    public int getNrOfControlInterfaces() {
        return nrOfControlInterfaces;
    }

    /**
     * @return The number of DPUs per Control Interface.
     */
    public int getNrOfDpusPerControlInterface() {
        return nrOfDpusPerControlInterface;
    }

    /**
     * @return The DPU MRAM size in bytes.
     */
    public int getMramSizeInBytes() {
        return mramSizeInBytes;
    }

    /**
     * @return The DPU WRAM size in 32-bit words.
     */
    public int getWramSizeInWords() {
        return wramSizeInWords;
    }

    /**
     * @return The DPU IRAM size in instructions.
     */
    public int getIramSizeInInstructions() {
        return iramSizeInInstructions;
    }

    /**
     * @return The number of threads in a DPU.
     */
    public int getNrOfThreadsPerDpu() {
        return nrOfThreadsPerDpu;
    }

    /**
     * @return The number of atomic bits in a DPU.
     */
    public int getNrOfAtomicBitsPerDpu() {
        return nrOfAtomicBitsPerDpu;
    }

    /**
     * @return The number of notify bits in a DPU.
     */
    public int getNrOfNotifyBitsPerDpu() {
        return nrOfNotifyBitsPerDpu;
    }

    /**
     * @return The number of work registers per thread in a DPU.
     */
    public int getNrOfWorkRegistersPerThread() {
        return nrOfWorkRegistersPerThread;
    }
}
