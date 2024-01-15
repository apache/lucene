/* Copyright 2020 UPMEM. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 */

package org.apache.lucene.sandbox.sdk;

import java.io.PrintStream;
import java.util.Collections;
import java.util.List;

/**
 * Representation of a DPU rank.
 *
 * @see DpuSet
 */
public final class DpuRank extends DpuSetBase implements DpuSet {
    private final List<Dpu> dpus;

    DpuRank(NativeDpuSet set, List<Dpu> dpus, PrintStream logStream) {
        super(set, logStream);
        this.set.wrapper = this;
        this.dpus = dpus;
    }

    @Override
    public List<Dpu> dpus() {
        return this.dpus;
    }

    @Override
    public List<DpuRank> ranks() {
        return Collections.singletonList(this);
    }
}
