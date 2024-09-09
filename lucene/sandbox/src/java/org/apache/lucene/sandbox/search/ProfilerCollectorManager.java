package org.apache.lucene.sandbox.search;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;

/** Collector manager for {@link ProfilerCollector} */
public abstract class ProfilerCollectorManager
    implements CollectorManager<ProfilerCollector, ProfilerCollectorResult> {

  private final String reason;

  /**
   * Creates a profiler collector manager provided a certain reason
   *
   * @param reason the reason for the collection
   */
  public ProfilerCollectorManager(String reason) {
    this.reason = reason;
  }

  /** Creates the collector to be wrapped with a {@link ProfilerCollector} */
  protected abstract Collector createCollector() throws IOException;

  @Override
  public final ProfilerCollector newCollector() throws IOException {
    return new ProfilerCollector(createCollector(), reason, List.of());
  }

  @Override
  public ProfilerCollectorResult reduce(Collection<ProfilerCollector> collectors)
      throws IOException {
    String name = null;
    String reason = null;
    long time = 0;

    for (ProfilerCollector collector : collectors) {
      assert name == null || name.equals(collector.getName());
      name = collector.getName();
      assert reason == null || reason.equals(collector.getReason());
      reason = collector.getReason();
      ProfilerCollectorResult profileResult = collector.getProfileResult();
      assert profileResult.getTime() == collector.getTime();
      time += profileResult.getTime();
    }

    return new ProfilerCollectorResult(name, reason, time, List.of());
  }
}
