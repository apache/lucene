/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.gradle;

import java.io.IOException;
import java.lang.Thread.State;
import java.lang.instrument.Instrumentation;
import java.util.EnumSet;


// we could also just load asyncprofiler as an agent directly through the .so file, but I don't know we would have the option to filter threads via that method
public class AsyncProfiler {

  private static final String CONFIG = "event=cpu,interval=10000000"; // 10ms, nocommit - options, alloc, wall, some of the other config options ...
  private final AsyncProfilerInterface asyncProfilerInterface;

  public AsyncProfiler() {
    asyncProfilerInterface = AsyncProfilerInterface.getInstance();
  }


  public static void premain(String agentArgs, Instrumentation inst) {
     AsyncProfiler asyncProfiler = new AsyncProfiler();
    try {
      asyncProfiler.start();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        asyncProfiler.stop();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }));
  }

  public void start() throws IOException {
    // nocommit
    String version = asyncProfilerInterface.asyncProfilerCmd("version");
    System.err.println("version is " + version);

    asyncProfilerInterface.asyncProfilerCmd("start," + CONFIG + ",file=hotspot-pid-%p-id-0-%t.jfr");
  }

  public void stop() throws IOException {
    asyncProfilerInterface.asyncProfilerCmd("stop");

    // nocommit - make options
    asyncProfilerInterface.asyncProfilerCmd( "summary,flat=" + 200 + ",traces=" + 200 + "," + CONFIG + ",file=hotspot-pid-%p-id-0-%t.txt");
    asyncProfilerInterface.asyncProfilerCmd( "flamegraph," + CONFIG + ",file=flame-hotspot-pid-%p-id-0-%t.html");
    asyncProfilerInterface.asyncProfilerCmd( "tree," + CONFIG + ",file=tree-hotspot-pid-%p-id-0-%t.html");

    // very heavy file output it seems
    // asyncProfilerInterface.asyncProfilerCmd( "collapsed," + CONFIG + ",file=collapsed-hotspot-pid-%p-id-0-%t.csv");
  }


  public static final class AsyncProfilerInterface {
    private static final String DEFAULT_ASYNC_PROFILER_LIB = "asyncProfiler";
    private static final EnumSet<State> IGNORED = EnumSet.of(Thread.State.NEW, Thread.State.TERMINATED);
    private static AsyncProfilerInterface instance;

    private AsyncProfilerInterface(String library) {
      System.load(library);
    }

    private AsyncProfilerInterface() {
      System.loadLibrary(DEFAULT_ASYNC_PROFILER_LIB);
    }

    public static AsyncProfilerInterface getInstance(String library) {
      if (instance == null) {
        synchronized (AsyncProfiler.class) {
          instance = new AsyncProfilerInterface(library);
        }
      }
      return instance;
    }

    public static AsyncProfilerInterface getInstance() {
      if (instance == null) {
        synchronized (AsyncProfiler.class) {
          instance = new AsyncProfilerInterface();
        }
      }
      return instance;
    }

    public String asyncProfilerCmd(String command) throws IOException {
      return execute0(command);
    }


    // TODO: could allow configuring only profiling certain threads
    public void filter(Thread thread, boolean enabled) {
      if (thread == null) {
        filterThread0(null, enabled);
      } else {
        synchronized (thread) {
          Thread.State threadState = thread.getState();
          if (!IGNORED.contains(threadState)) {
            filterThread0(thread, enabled);
          }
        }
      }
    }

    private native String execute0(String command) throws IllegalArgumentException, IOException;

    private native void filterThread0(Thread thread, boolean enabled);

    private native void start0(String event, long interval, boolean reset) throws IllegalStateException;

    private native void stop0() throws IllegalStateException;

    private native long getSamples();

  }
}
