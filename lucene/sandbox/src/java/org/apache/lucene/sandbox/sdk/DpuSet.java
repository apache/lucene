/* Copyright 2020 UPMEM. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 */

package org.apache.lucene.sandbox.sdk;

import java.io.PrintStream;
import java.util.List;
import java.nio.ByteBuffer;

/**
 * Interface for one or more DPUs.
 *
 * @see DpuSystem
 * @see DpuRank
 * @see Dpu
 */
public interface DpuSet {
    /**
     * Provide the DPU devices in the set.
     *
     * @return The DPUs in the set.
     * @see Dpu
     */
    List<Dpu> dpus();

    /**
     * Provide the DPU ranks in the set.
     *
     * @return The DPU ranks in the set.
     * @see DpuRank
     */
     List<DpuRank> ranks();

    /**
     * Provide access to the subset of methods which can be used asynchronously.
     *
     * @return The asynchronous interface for the current DPU set.
     * @see DpuSetAsync
     */
    DpuSetAsync async();

    /**
     * Apply the reset process to the DPU set.
     *
     * @exception DpuException When the reset fails.
     * @see DpuException
     */
    void reset() throws DpuException;

    /**
     * Load the given DPU program in the DPU set.
     *
     * @param dpuExecutable The path to the DPU program.
     * @return The information on the loaded DPU program.
     * @exception DpuException When the program cannot be loaded.
     * @see DpuProgramInfo
     * @see DpuException
     */
    DpuProgramInfo load(String dpuExecutable) throws DpuException;

    /**
     * Copy data to the DPU set.
     *
     * @param dstSymbol The destination DPU symbol name.
     * @param srcBuffer The host data to be copied to the DPU set.
     * @exception DpuException When the data cannot be copied.
     * @see DpuException
     */
    void copy(String dstSymbol, byte[] srcBuffer) throws DpuException;

    /**
     * Copy data to the DPU set.
     *
     * @param dstSymbol The destination DPU symbol name.
     * @param srcBuffer The host data to be copied to the DPU set.
     * @param startIndex the index of the first element to transfer from srcBuffer
     * @param size the number of elements to transfer
     * @param dpuOffset the offset to the destination address of dstSymbol
     * @exception DpuException When the data cannot be copied.
     * @see DpuException
     */
    void copy(String dstSymbol, byte[] srcBuffer, int startIndex, 
        int size, int dpuOffset) throws DpuException;

    /**
     * Copy different data to each DPU of the DPU set.
     *
     * @param dstSymbol The destination DPU symbol name.
     * @param srcBuffers The host data to be copied to the DPU set.
     * @exception DpuException When the data cannot be copied.
     * @see DpuException
     */
    void copy(String dstSymbol, byte[][] srcBuffers) throws DpuException;

    /**
     * Copy different data to each DPU of the DPU set.
     *
     * @param dstSymbol The destination DPU symbol.
     * @param srcBuffers The host data to be copied to the DPU set.
     * @exception DpuException When the data cannot be copied.
     * @see DpuSymbol
     * @see DpuException
     */
    void copy(DpuSymbol dstSymbol, byte[][] srcBuffers) throws DpuException;

    /**
     * Copy different data to each DPU of the DPU set.
     *
     * @param dstSymbol The destination DPU symbol.
     * @param srcBuffers The host data to be copied to the DPU set (as a ByteBuffer).
     * @param size the size (number of bytes) to transfer for each DPU
     * @exception DpuException When the data cannot be copied.
     * @see DpuSymbol
     * @see DpuException
     */
    void copy(DpuSymbol dstSymbol, ByteBuffer[] srcBuffers, int size) throws DpuException;

    /**
     * Copy data from each DPU of the DPU set.
     *
     * @param dstBuffers The host buffers storing the DPU data.
     * @param srcSymbol The source DPU symbol name.
     * @exception DpuException When the data cannot be copied.
     * @see DpuException
     */
    void copy(byte[][] dstBuffers, String srcSymbol) throws DpuException;

    /**
     * Copy data from each DPU of the DPU set.
     *
     * @param dstBuffers The host buffers storing the DPU data.
     * @param srcSymbol The source DPU symbol name.
     * @param size the size (number of bytes) to transfer for each DPU
     * @exception DpuException When the data cannot be copied.
     * @see DpuException
     */
    void copy(byte[][] dstBuffers, String srcSymbol, int size) throws DpuException;

    /**
     * Copy data from each DPU of the DPU set.
     *
     * @param dstBuffers The host buffers storing the DPU data (as a ByteBuffer).
     * @param srcSymbol The source DPU symbol name.
     * @param size the size (number of bytes) to transfer for each DPU
     * @exception DpuException When the data cannot be copied.
     * @see DpuException
     */
    void copy(ByteBuffer[] dstBuffers, String srcSymbol, int size) throws DpuException;


    /**
     * Copy data from each DPU of the DPU set.
     *
     * @param dstBuffers The host buffers storing the DPU data.
     * @param srcSymbol The source DPU symbol.
     * @exception DpuException When the data cannot be copied.
     * @see DpuSymbol
     * @see DpuException
     */
    void copy(byte[][] dstBuffers, DpuSymbol srcSymbol) throws DpuException;

    /**
     * Execute the previously loaded DPU program on the DPU set.
     *
     * <p>If a default log stream has been provided during the DPU system allocation,
     * it will be used to print the DPU logs after the execution</p>
     *
     * @exception DpuException When the program execution triggers a DPU fault.
     * @see DpuSet#load
     * @see DpuException
     * @see DpuFaultDump
     */
    void exec() throws DpuException;

    /**
     * Execute the previously loaded DPU program on the DPU set,
     * then log the DPU logs in the given stream.
     *
     * @param logStream Where to print the DPU logs after execution.
     * @exception DpuException When the program execution triggers a DPU fault.
     * @see DpuSet#load
     * @see DpuException
     * @see DpuFaultDump
     */
    void exec(PrintStream logStream) throws DpuException;

    /**
     * Print the DPU set logs on the default log stream.
     *
     * <p>If no default stream was provided during the allocation, uses the standard output.</p>
     *
     * @exception DpuException When the DPU logs cannot be fetched or displayed.
     * @see DpuException
     */
    void log() throws DpuException;

    /**
     * Print the DPU set logs on the given log stream.
     *
     * @param logStream The stream on which the DPU logs will be printed.
     * @exception DpuException When the DPU logs cannot be fetched or displayed.
     * @see DpuException
     */
    void log(PrintStream logStream) throws DpuException;
}
