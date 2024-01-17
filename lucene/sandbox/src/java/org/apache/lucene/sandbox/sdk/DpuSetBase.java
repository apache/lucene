/* Copyright 2020 UPMEM. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 */

package org.apache.lucene.sandbox.sdk;

import java.io.PrintStream;
import java.util.List;
import java.nio.ByteBuffer;

/**
 * base abstract class for a DPU set
 */
abstract class DpuSetBase {
    protected final NativeDpuSet set;
    protected final PrintStream logStream;
    private final DpuSetAsync async;

    DpuSetBase(NativeDpuSet set, PrintStream logStream) {
        this.set = set;
        this.logStream = logStream;
        this.async = new DpuSetAsync(this.set, this.logStream);
    }

    public abstract List<Dpu> dpus();

    public DpuSetAsync async() {
        return this.async;
    }

    public void reset() throws DpuException {
        this.set.reset();
    }

    public DpuProgramInfo load(String dpuExecutable) throws DpuException {
        return this.set.load(dpuExecutable);
    }

    public void copy(String dstSymbol, byte[] srcBuffer) throws DpuException {
        this.set.broadcast(dstSymbol, srcBuffer, false);
    }

    public void copy(String dstSymbol, byte[] srcBuffer, int startIndex, 
        int size, int dpuOffset) throws DpuException {
        this.set.broadcast(dstSymbol, srcBuffer, startIndex, size, dpuOffset, false);
    }

    public void copy(DpuSymbol dstSymbol, byte[] srcBuffer) throws DpuException {
        this.set.broadcast(dstSymbol, srcBuffer, false);
    }

    public void copy(String dstSymbol, byte[][] srcBuffers) throws DpuException {
        this.set.copy(dstSymbol, srcBuffers, -1, true, false);
    }

    public void copy(DpuSymbol dstSymbol, byte[][] srcBuffers) throws DpuException {
        this.set.copy(dstSymbol, srcBuffers, -1, true, false);
    }

    public void copy(DpuSymbol dstSymbol, ByteBuffer[] srcBuffers, int size) throws DpuException {
        this.set.copy(dstSymbol, srcBuffers, size, true, false);
    }

    public void copy(byte[][] dstBuffers, String srcSymbol) throws DpuException {
        this.set.copy(srcSymbol, dstBuffers, -1, false, false);
    }

    public void copy(byte[][] dstBuffers, String srcSymbol, int size) throws DpuException {
        this.set.copy(srcSymbol, dstBuffers, size, false, false);
    }

    public void copy(ByteBuffer[] dstBuffers, String srcSymbol, int size) throws DpuException {
        this.set.copy(srcSymbol, dstBuffers, size, false, false);
    }

    public void copy(byte[][] dstBuffers, DpuSymbol srcSymbol) throws DpuException {
        this.set.copy(srcSymbol, dstBuffers, -1, false, false);
    }

    public void exec() throws DpuException {
        this.exec(this.logStream);
    }

    public void exec(PrintStream logStream) throws DpuException {
        this.set.launch(false);

        if (logStream != null) {
            this.log(logStream);
        }
    }

    public void log() throws DpuException {
        PrintStream logStream = (this.logStream == null) ? System.out : this.logStream;
        this.log(logStream);
    }

    public void log(PrintStream logStream) throws DpuException {
        for (Dpu dpu: this.dpus()) {
            dpu.set.log(logStream);
        }
    }
}
