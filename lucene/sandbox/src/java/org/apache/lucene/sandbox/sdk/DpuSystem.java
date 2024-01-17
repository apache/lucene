/* Copyright 2020 UPMEM. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 */

package org.apache.lucene.sandbox.sdk;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Main class for a user to manage a set of DPUs
 */
public final class DpuSystem extends DpuSetBase implements DpuSet, AutoCloseable {
    /**
     * Constant used in the allocation methods to allocate all DPUs.
     */
    public static final int ALLOCATE_ALL = -1;

    private final List<Dpu> dpus;
    private final List<DpuRank> ranks;
    private final DpuDescription description;

    DpuSystem(NativeDpuSet set, PrintStream logStream) throws DpuException {
        super(set, logStream);
        this.set.wrapper = this;
        this.description = this.set.description();

        this.dpus = new ArrayList<>();
        this.ranks = new ArrayList<>();

        for (NativeDpuSet child : set.getChildren()) {
            List<Dpu> dpus = new ArrayList<>();
            DpuRank rank = new DpuRank(child, dpus, logStream);

            for (NativeDpuSet grandChild : child.getChildren()) {
                Dpu dpu = new Dpu(grandChild, rank, logStream);
                dpus.add(dpu);
            }

            this.dpus.addAll(dpus);
            this.ranks.add(rank);
        }
    }

    /**
     * Fetch the DPU description for the given profile.
     *
     * @param profile The DPU profile.
     * @return The DPU description.
     * @exception DpuException When the profile is incorrect.
     * @see DpuDescription
     * @see DpuException
     */
    public static DpuDescription description(String profile) throws DpuException {
        return NativeDpuSet.descriptionFor(profile);
    }

    /**
     * Allocate a number of DPUs with the given profile.
     *
     * @param nrDpus The number of DPUs to allocate. {@link DpuSystem#ALLOCATE_ALL} to allocate all available DPUs.
     * @param profile The DPU profile. Use an empty string for the default profile.
     * @return The allocated DPU system.
     * @exception DpuException When the DPUs could not be allocated
     * @see DpuSystem#ALLOCATE_ALL
     * @see DpuException
     */
    public static DpuSystem allocate(int nrDpus, String profile) throws DpuException {
        return new DpuSystem(NativeDpuSet.allocate(nrDpus, profile), null);
    }

    /**
     * Allocate a number of DPUs with the given profile and a default log stream.
     *
     * @param nrDpus The number of DPUs to allocate. {@link DpuSystem#ALLOCATE_ALL} to allocate all available DPUs.
     * @param profile The DPU profile. Use an empty string for the default profile.
     * @param logStream The default stream where the DPU logs will be printed after execution.
     * @return The allocated DPU system.
     * @exception DpuException When the DPUs could not be allocated
     * @see DpuSystem#ALLOCATE_ALL
     * @see DpuException
     */
    public static DpuSystem allocate(int nrDpus, String profile, PrintStream logStream) throws DpuException {
        return new DpuSystem(NativeDpuSet.allocate(nrDpus, profile), logStream);
    }

    /**
     * Allocate a number of DPU ranks with the given profile.
     *
     * @param nrRanks The number of DPU ranks to allocate. {@link DpuSystem#ALLOCATE_ALL} to allocate all available DPUs.
     * @param profile The DPU profile. Use an empty string for the default profile.
     * @return The allocated DPU system.
     * @exception DpuException When the DPUs could not be allocated
     * @see DpuSystem#ALLOCATE_ALL
     * @see DpuException
     */
    public static DpuSystem allocateRanks(int nrRanks, String profile) throws DpuException {
        return new DpuSystem(NativeDpuSet.allocateRanks(nrRanks, profile), null);
    }

    /**
     * Allocate a number of DPU ranks with the given profile and a default log stream.
     *
     * @param nrRanks The number of DPU ranks to allocate. {@link DpuSystem#ALLOCATE_ALL} to allocate all available DPUs.
     * @param profile The DPU profile. Use an empty string for the default profile.
     * @param logStream The default stream where the DPU logs will be printed after execution.
     * @return The allocated DPU system.
     * @exception DpuException When the DPUs could not be allocated
     * @see DpuSystem#ALLOCATE_ALL
     * @see DpuException
     */
    public static DpuSystem allocateRanks(int nrRanks, String profile, PrintStream logStream) throws DpuException {
        return new DpuSystem(NativeDpuSet.allocateRanks(nrRanks, profile), logStream);
    }

    /**
     * Allocate a DpuSystem with the given profile.
     *
     * @param profile The DPU profile.
     * @return The allocated DPU system.
     * @exception DpuException When the DPUs could not be allocated
     * @see DpuProfile
     * @see DpuException
     */
    public static DpuSystem allocate(DpuProfile profile) throws DpuException {
        String profileString = profile.toString();

        if (profile.nrDpus != null) {
            return DpuSystem.allocate(profile.nrDpus, profileString);
        }

        if (profile.nrRanks != null) {
            return DpuSystem.allocateRanks(profile.nrRanks, profileString);
        }

        return DpuSystem.allocate(ALLOCATE_ALL, profileString);
    }

    /**
     * Allocate a DpuSystem with the default profile.
     *
     * @return The allocated DPU system.
     * @exception DpuException When the DPUs could not be allocated
     * @see DpuException
     */
    public static DpuSystem allocate() throws DpuException {
        return DpuSystem.allocate(DpuProfile.empty());
    }

    /**
     * Return the description of the current DpuSystem.
     *
     * @return The DPU description.
     * @see DpuDescription
     */
    public DpuDescription description() {
        return this.description;
    }

    /**
     * Free the DPUs of the current DpuSystem.
     *
     * <p>The DpuSystem should not be used anymore after calling this method.</p>
     *
     * @exception DpuException When the DPUs could not be freed.
     * @see DpuException
     */
    public void free() throws DpuException {
        this.set.free();
    }

    @Override
    public List<Dpu> dpus() {
        return this.dpus;
    }

    @Override
    public List<DpuRank> ranks() {
        return this.ranks;
    }

    @Override
    public void close() throws DpuException {
        this.free();
    }
}
