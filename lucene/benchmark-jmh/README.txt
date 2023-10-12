The :lucene:benchmark-jmh module contains can be used to compile
and execute JMH (https://github.com/openjdk/jmh) micro-benchmarks.

Look at existing classes and JMH documentation for inspiration on how
to write good micro-benchmarks.

To compile the project and prepare JMH launcher, run:

gradlew :lucene:benchmark-jmh:assemble

The above target will display exact commands to execute JMH from
command line, for example:

java --module-path lucene\benchmark-jmh\build\benchmarks --module org.apache.lucene.benchmark.jmh

You can pass any JMH options to the above command, for example:

  -h               displays verbose help for all options
  -l               list available benchmarks
  -lp              list benchmarks that pass the filter and their parameters
  -prof perfasm    use perfasm profiler to see assembly
  regexp           execute all benchmark containing regexp

Here is an example running a single benchmark:

java --module-path lucene\benchmark-jmh\build\benchmarks --module org.apache.lucene.benchmark.jmh org.apache.lucene.benchmark.jmh.VectorUtilBenchmark.binaryCosineVector

Or running all of VectorUtilBenchmark

java --module-path lucene\benchmark-jmh\build\benchmarks --module org.apache.lucene.benchmark.jmh org.apache.lucene.benchmark.jmh.VectorUtilBenchmark
