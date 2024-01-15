/* Copyright 2020 UPMEM. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 */

package org.apache.lucene.sandbox.sdk;

import java.io.PrintStream;
import java.util.Collections;
import java.util.List;

/**
 * Representation of a DPU device.
 *
 * @see DpuSet
 */
public final class Dpu extends DpuSetBase implements DpuSet {
    private final DpuRank rank;

    Dpu(NativeDpuSet set, DpuRank parentRank, PrintStream logStream) {
        super(set, logStream);
        this.set.wrapper = this;
        this.rank = parentRank;
    }

    @Override
    public List<Dpu> dpus() {
        return Collections.singletonList(this);
    }

    @Override
    public List<DpuRank> ranks() {
        return Collections.singletonList(this.rank);
    }

    public void copy(byte[] dstBuffer, DpuSymbol srcSymbol) throws DpuException {
        this.copy(new byte[][] { dstBuffer }, srcSymbol);
    }

    public void copy(byte[] dstBuffer, String srcSymbol) throws DpuException {
        this.copy(new byte[][] { dstBuffer }, srcSymbol);
    }

    public DpuFaultDump debugInit() throws DpuException {
        return this.set.debugInit();
    }

    public DpuContextDump dump(DpuFaultDump faultDump) throws DpuException {
        return this.set.dump(faultDump);
    }

    public DpuContextDump dump() throws DpuException {
        return this.dump(null);
    }

    public void systemReport(String reportFilename) throws DpuException {
        this.set.systemReport(reportFilename);
    }
}
