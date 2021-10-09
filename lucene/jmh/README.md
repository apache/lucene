<!--
    Licensed to the Apache Software Foundation (ASF) under one or more
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership.
    The ASF licenses this file to You under the Apache License, Version 2.0
    the "License"); you may not use this file except in compliance with
    the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
 -->

JMH Benchmarks module
=====================

This module contains benchmarks written using [JMH](https://openjdk.java.net/projects/code-tools/jmh/) from OpenJDK.
Writing correct micro-benchmarks in Java (or another JVM language) is difficult and there are many non-obvious
pitfalls (many due to compiler optimizations). JMH is a framework for running and analyzing benchmarks (micro or macro)
written in Java (or another JVM language).

* [JMH Benchmarks module](#jmh-benchmarks-module)
  * [Running benchmarks](#running-benchmarks)
    * [Using JMH with Async-Profiler](#using-jmh-with-async-profiler)
      * [OS Permissions for Async-Profiler](#os-permissions-for-async-profiler)
    * [Using JMH GC profiler](#using-jmh-gc-profiler)
    * [Using JMH Java Flight Recorder profiler](#using-jmh-java-flight-recorder-profiler)
  * [JMH Options](#jmh-options)

## Running benchmarks

If you want to set specific JMH flags or only run certain benchmarks, passing arguments via gradle tasks is cumbersome.
The process has been simplified by the provided `jmh.sh` script.

The default behavior is to run all benchmarks.

```shell
$ ./jmh.sh
```

Pass a pattern or name after the command to select the benchmarks.

```shell
$ ./jmh.sh BenchmarkClass
$ ./jmh.sh BenchmarkClass.benchmarkMethod
```

List all benchmarks.

```shell
$ ./jmh.sh -l
```

Check which benchmarks match the provided pattern.

```shell
$ ./jmh.sh -l Bench
```

Run a specific test and overrides the number of forks, iterations and sets warm-up iterations to `2`.

```shell
$ ./jmh.sh -f 2 -i 2 -wi 2 BenchmarkClass
```

Run a specific test with async and GC profilers on Linux and flame graph output.

```shell
$ ./jmh.sh -prof gc -prof async:libPath=/path/to/libasyncProfiler.so\;output=flamegraph\;dir=profile-results BenchmarkClass
```

### Using JMH with Async-Profiler

It's good practice to check profiler output for micro-benchmarks in order to verify that they represent the expected
application behavior and measure what you expect to measure. Some example pitfalls include the use of expensive mocks or
accidental inclusion of test setup code in the benchmarked code. JMH includes
[async-profiler](https://github.com/jvm-profiling-tools/async-profiler) integration that makes this easy:

```shell
$ ./jmh.sh -prof async:libPath=/path/to/libasyncProfiler.so\;dir=profile-results
```

With flame graph output:

```shell
$ ./jmh.sh -prof async:libPath=/path/to/libasyncProfiler.so\;output=flamegraph\;dir=profile-results
```

Simultaneous cpu, allocation and lock profiling with async profiler 2.0 and jfr output:

```shell
$ ./jmh.sh -prof async:libPath=/path/to/libasyncProfiler.so\;output=jfr\;alloc\;lock\;dir=profile-results BenchmarkClass
```

A number of arguments can be passed to configure async profiler, run the following for a description:

```shell
$ ./jmh.sh -prof async:help
```

You can also skip specifying libPath if you place the async profiler lib in a predefined location, such as one of the
locations in the env variable `LD_LIBRARY_PATH` if it has been set (many Linux distributions set this env variable, Arch
by default does not), or `/usr/lib` should work.

#### OS Permissions for Async-Profiler

Async Profiler uses perf to profile native code in addition to Java code. It will need the following for the necessary
access.

```shell
$ echo 0 > /proc/sys/kernel/kptr_restrict
$ echo 1 > /proc/sys/kernel/perf_event_paranoid
```

or

```shell
$ sudo sysctl -w kernel.kptr_restrict=0
$ sudo sysctl -w kernel.perf_event_paranoid=1
```

### Using JMH GC profiler

You can run a benchmark with `-prof gc` to measure its allocation rate:

```shell
$ ./jmh.sh -prof gc:dir=profile-results
```

Of particular importance is the `norm` alloc rates, which measure the allocations per operation rather than allocations
per second.

### Using JMH Java Flight Recorder profiler

JMH comes with a variety of built-in profilers. Here is an example of using JFR:

```shell
$./jmh.sh -prof jfr:dir=profile-results\;configName=jfr-profile.jfc BenchmarkClass
```

In this example we point to the included configuration file with configName, but you could also do something like
settings=default or settings=profile.

### JMH Options

Some common JMH options are:

```text

 Usage: ./jmh.sh [regexp*] [options]
 [opt] means optional argument.
 <opt> means required argument.
 "+" means comma-separated list of values.
 "time" arguments accept time suffixes, like "100ms".

Command line options usually take precedence over annotations.

  [arguments]                 Benchmarks to run (regexp+). (default: .*) 

  -bm <mode>                  Benchmark mode. Available modes are: [Throughput/thrpt, 
                              AverageTime/avgt, SampleTime/sample, SingleShotTime/ss, 
                              All/all]. (default: Throughput) 

  -bs <int>                   Batch size: number of benchmark method calls per 
                              operation. Some benchmark modes may ignore this 
                              setting, please check this separately. (default: 
                              1) 

  -e <regexp+>                Benchmarks to exclude from the run. 

  -f <int>                    How many times to fork a single benchmark. Use 0 to 
                              disable forking altogether. Warning: disabling 
                              forking may have detrimental impact on benchmark 
                              and infrastructure reliability, you might want 
                              to use different warmup mode instead. (default: 
                              5) 

  -foe <bool>                 Should JMH fail immediately if any benchmark had 
                              experienced an unrecoverable error? This helps 
                              to make quick sanity tests for benchmark suites, 
                              as well as make the automated runs with checking error 
                              codes. (default: false) 

  -gc <bool>                  Should JMH force GC between iterations? Forcing 
                              the GC may help to lower the noise in GC-heavy benchmarks, 
                              at the expense of jeopardizing GC ergonomics decisions. 
                              Use with care. (default: false) 

  -h                          Display help, and exit. 

  -i <int>                    Number of measurement iterations to do. Measurement 
                              iterations are counted towards the benchmark score. 
                              (default: 1 for SingleShotTime, and 5 for all other 
                              modes) 

  -jvm <string>               Use given JVM for runs. This option only affects forked 
                              runs. 

  -jvmArgs <string>           Use given JVM arguments. Most options are inherited 
                              from the host VM options, but in some cases you want 
                              to pass the options only to a forked VM. Either single 
                              space-separated option line, or multiple options 
                              are accepted. This option only affects forked runs. 

  -jvmArgsAppend <string>     Same as jvmArgs, but append these options after the 
                              already given JVM args. 

  -jvmArgsPrepend <string>    Same as jvmArgs, but prepend these options before 
                              the already given JVM arg. 

  -l                          List the benchmarks that match a filter, and exit. 

  -lp                         List the benchmarks that match a filter, along with 
                              parameters, and exit. 

  -lprof                      List profilers, and exit. 

  -lrf                        List machine-readable result formats, and exit. 

  -o <filename>               Redirect human-readable output to a given file. 

  -opi <int>                  Override operations per invocation, see @OperationsPerInvocation 
                              Javadoc for details. (default: 1) 

  -p <param={v,}*>            Benchmark parameters. This option is expected to 
                              be used once per parameter. Parameter name and parameter 
                              values should be separated with equals sign. Parameter 
                              values should be separated with commas. 

  -prof <profiler>            Use profilers to collect additional benchmark data. 
                              Some profilers are not available on all JVMs and/or 
                              all OSes. Please see the list of available profilers 
                              with -lprof. 

  -r <time>                   Minimum time to spend at each measurement iteration. 
                              Benchmarks may generally run longer than iteration 
                              duration. (default: 10 s) 

  -rf <type>                  Format type for machine-readable results. These 
                              results are written to a separate file (see -rff). 
                              See the list of available result formats with -lrf. 
                              (default: CSV) 

  -rff <filename>             Write machine-readable results to a given file. 
                              The file format is controlled by -rf option. Please 
                              see the list of result formats for available formats. 
                              (default: jmh-result.<result-format>) 

  -si <bool>                  Should JMH synchronize iterations? This would significantly 
                              lower the noise in multithreaded tests, by making 
                              sure the measured part happens only when all workers 
                              are running. (default: true) 

  -t <int>                    Number of worker threads to run with. 'max' means 
                              the maximum number of hardware threads available 
                              on the machine, figured out by JMH itself. (default: 
                              1) 

  -tg <int+>                  Override thread group distribution for asymmetric 
                              benchmarks. This option expects a comma-separated 
                              list of thread counts within the group. See @Group/@GroupThreads 
                              Javadoc for more information. 

  -to <time>                  Timeout for benchmark iteration. After reaching 
                              this timeout, JMH will try to interrupt the running 
                              tasks. Non-cooperating benchmarks may ignore this 
                              timeout. (default: 10 min) 

  -tu <TU>                    Override time unit in benchmark results. Available 
                              time units are: [m, s, ms, us, ns]. (default: SECONDS) 

  -v <mode>                   Verbosity mode. Available modes are: [SILENT, NORMAL, 
                              EXTRA]. (default: NORMAL) 

  -w <time>                   Minimum time to spend at each warmup iteration. Benchmarks 
                              may generally run longer than iteration duration. 
                              (default: 10 s) 

  -wbs <int>                  Warmup batch size: number of benchmark method calls 
                              per operation. Some benchmark modes may ignore this 
                              setting. (default: 1) 

  -wf <int>                   How many warmup forks to make for a single benchmark. 
                              All iterations within the warmup fork are not counted 
                              towards the benchmark score. Use 0 to disable warmup 
                              forks. (default: 0) 

  -wi <int>                   Number of warmup iterations to do. Warmup iterations 
                              are not counted towards the benchmark score. (default: 
                              0 for SingleShotTime, and 5 for all other modes) 

  -wm <mode>                  Warmup mode for warming up selected benchmarks. 
                              Warmup modes are: INDI = Warmup each benchmark individually, 
                              then measure it. BULK = Warmup all benchmarks first, 
                              then do all the measurements. BULK_INDI = Warmup 
                              all benchmarks first, then re-warmup each benchmark 
                              individually, then measure it. (default: INDI) 

  -wmb <regexp+>              Warmup benchmarks to include in the run in addition 
                              to already selected by the primary filters. Harness 
                              will not measure these benchmarks, but only use them 
                              for the warmup. 
```

## Writing benchmarks

For help in writing correct JMH tests, the best place to start is
the [sample code](https://hg.openjdk.java.net/code-tools/jmh/file/tip/jmh-samples/src/main/java/org/openjdk/jmh/samples/)
provided by the JMH project.

JMH is highly configurable and users are encouraged to look through the samples for suggestions on what options are
available. A good tutorial for using JMH can be
found [here](http://tutorials.jenkov.com/java-performance/jmh.html#return-value-from-benchmark-method)

