package org.apache.lucene.facet;

import java.util.Collection;
import org.apache.lucene.search.CollectorManager;

public class AssertingSubDocsAtOnceCollectorManager
    implements CollectorManager<AssertingSubDocsAtOnceCollector, Object> {

  @Override
  public AssertingSubDocsAtOnceCollector newCollector() {
    return new AssertingSubDocsAtOnceCollector();
  }

  @Override
  public Object reduce(Collection<AssertingSubDocsAtOnceCollector> collectors) {
    return null;
  }
}
