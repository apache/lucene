package org.apache.lucene.index;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Simple demonstration of a MultiTenantCMSManager that divides a fixed thread budget equally among
 * all registered CMSs.
 */
public class MultiTenantCMSManager {
  private static final MultiTenantCMSManager INSTANCE = new MultiTenantCMSManager();
  private static final int coreCount = Runtime.getRuntime().availableProcessors();
  private static final int total_threads = maxThreadCount = Math.max(1, coreCount / 2);
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
    int share = Math.max(1, total_threads / count);
    for (ConcurrentMergeScheduler cms : schedulers) {
      cms.setMaxMergesAndThreads(share + 5, share);
    }
  }
}
