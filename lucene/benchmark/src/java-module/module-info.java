@SuppressWarnings({"requires-automatic"})
module org.apache.lucene.benchmark {
  requires java.xml;
  requires org.apache.lucene.core;
  requires org.apache.lucene.analysis.common;
  requires org.apache.lucene.facet;
  requires org.apache.lucene.highlighter;
  requires org.apache.lucene.queries;
  requires org.apache.lucene.queryparser;
  requires org.apache.lucene.spatial_extras;
  requires spatial4j;

  exports org.apache.lucene.benchmark;
  exports org.apache.lucene.benchmark.byTask;
  exports org.apache.lucene.benchmark.byTask.feeds;
  exports org.apache.lucene.benchmark.byTask.programmatic;
  exports org.apache.lucene.benchmark.byTask.stats;
  exports org.apache.lucene.benchmark.byTask.tasks;
  exports org.apache.lucene.benchmark.byTask.utils;
  exports org.apache.lucene.benchmark.quality;
  exports org.apache.lucene.benchmark.quality.trec;
  exports org.apache.lucene.benchmark.quality.utils;
  exports org.apache.lucene.benchmark.utils;
}
