package org.apache.lucene.index;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Simple demonstration of a MultiTenantCMSManager that divides a fixed thread budget
 * equally among all registered ConcurrentMergeSchedulers.
 */
public class MultiTenantCMSManager {

    private static final MultiTenantCMSManager INSTANCE = new MultiTenantCMSManager();
    private static final int coreCount = Runtime.getRuntime().availableProcessors();
    private static final int maxThreadCount = Math.max(1, coreCount / 2);

    private final Set<ConcurrentMergeScheduler> schedulers =
        Collections.synchronizedSet(new HashSet<>());

    private MultiTenantCMSManager() {}

    public static MultiTenantCMSManager getInstance() {
        return INSTANCE;
    }

    public void register(ConcurrentMergeScheduler cms) {
        schedulers.add(cms);
        updateBudgets();
    }

    public void unregister(ConcurrentMergeScheduler cms) {
        schedulers.remove(cms);
        updateBudgets();
    }

    private void updateBudgets() {
        int count = schedulers.size();
        if (count == 0) return;

        int share = Math.max(1, maxThreadCount / count);
        for (ConcurrentMergeScheduler cms : schedulers) {
            cms.setMaxMergesAndThreads(share + 5, share); // +5 to allow merge queuing
        }
    }

    // -----------------------------------------
    // 🧪 TESTING HOOKS
    // -----------------------------------------

    /** Used in tests to read the current registered CMS set */
    synchronized Set<ConcurrentMergeScheduler> getRegisteredSchedulersForTest() {
        return new HashSet<>(schedulers);
    }

    /** Used in tests to clear all registered CMS instances */
    synchronized void unregisterAllForTest() {
        schedulers.clear();
    }
}
