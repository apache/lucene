/* Copyright 2020 UPMEM. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 */

package org.apache.lucene.sandbox.sdk;

import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

/**
 * A DPU profile, used to allocate a DPU system.
 *
 * <p>This class provides Builder-like methods to configure the profile.</p>
 *
 * @see DpuSystem
 */
public final class DpuProfile {
    Integer nrDpus;
    Integer nrRanks;
    DpuTarget target;
    Map<String, String> properties;

    private DpuProfile() {
        this.nrDpus = null;
        this.nrRanks = null;
        this.target = null;
        this.properties = new HashMap<>();
    }

    /**
     * @return The default DPU profile.
     */
    public static DpuProfile empty() {
        return new DpuProfile();
    }

    /**
     * @param nrDpus The number of DPUs to allocate.
     * @return The updated profile.
     */
    public DpuProfile nrDpus(int nrDpus) {
        this.nrDpus = nrDpus;
        return this;
    }

    /**
     * @param nrRanks The number of DPU ranks to allocate.
     * @return The updated profile.
     */
    public DpuProfile nrRanks(int nrRanks) {
        this.nrRanks = nrRanks;
        return this;
    }

    /**
     * @param target the DPU backend to allocate.
     * @return The updated profile.
     * @see DpuTarget
     */
    public DpuProfile target(DpuTarget target) {
        this.target = target;
        return this;
    }

    /**
     * Add a custom property to the profile.
     *
     * @param property The name of the property.
     * @param value The value of the property.
     * @return The updated profile.
     */
    public DpuProfile custom(String property, String value) {
        this.properties.put(property, value);
        return this;
    }

    @Override
    public String toString() {
        StringJoiner joiner = new StringJoiner(",");
        if (this.target != null) {
            joiner.add("backend=" + this.target);
        }

        for (Map.Entry<String, String> entry: this.properties.entrySet()) {
            joiner.add(entry.getKey() + "=" + entry.getValue());
        }

        return joiner.toString();
    }
}
