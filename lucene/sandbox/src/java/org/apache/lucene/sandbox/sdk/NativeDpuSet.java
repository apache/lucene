/* Copyright 2020 UPMEM. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 */

package org.apache.lucene.sandbox.sdk;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.nio.ByteBuffer;

/**
 * class to hold a native DPU set
 * This class contains all the native methods which call the C APIs under the hood
 */
final class NativeDpuSet {
    static {
        System.loadLibrary("dpujnilucene");
    }

    private List<NativeDpuSet> children;
    private boolean isSingleDpu;
    private int nrRanks;
    private long nativePointer;
    DpuSet wrapper;

    NativeDpuSet() {
        this.children = new ArrayList<>();
    }

    List<NativeDpuSet> getChildren() {
        return children;
    }

    static native DpuDescription descriptionFor(String profile) throws DpuException;
    static native NativeDpuSet allocate(int nrDpus, String profile) throws DpuException;
    static native NativeDpuSet allocateRanks(int nrRanks, String profile) throws DpuException;
    native DpuDescription description() throws DpuException;
    native void reset() throws DpuException;
    native DpuProgramInfo load(String programPath) throws DpuException;
    native void broadcast(String dpuSymbol, byte[] hostBuffer, boolean isAsync) throws DpuException;
    native void broadcast(DpuSymbol dpuSymbol, byte[] hostBuffer, boolean isAsync) throws DpuException;
    native void broadcast(String dpuSymbol, byte[] hostBuffer, int startIndex, int size, 
        int dpuAddressOffset, boolean isAsync) throws DpuException;
    native void copy(String dpuSymbol, byte[][] hostBuffers, int size, boolean toDpu, boolean isAsync) throws DpuException;
    native void copy(String dpuSymbol, ByteBuffer[] hostBuffers, int size, boolean toDpu, boolean isAsync) throws DpuException;
    native void copy(DpuSymbol dpuSymbol, byte[][] hostBuffers, int size, boolean toDpu, boolean isAsync) throws DpuException;
    native void copy(DpuSymbol dpuSymbol, ByteBuffer[] hostBuffers, int size, boolean toDpu, boolean isAsync) throws DpuException;
    native void launch(boolean isAsync) throws DpuException;
    native void log(PrintStream stream) throws DpuException;
    native void call(DpuCallback callback, boolean blocking, boolean singleCall) throws DpuException;
    native void sync() throws DpuException;
    native DpuFaultDump debugInit() throws DpuException;
    native DpuContextDump dump(DpuFaultDump faultDump) throws DpuException;
    native void systemReport(String reportFilename) throws DpuException;
    native void free() throws DpuException;
}
