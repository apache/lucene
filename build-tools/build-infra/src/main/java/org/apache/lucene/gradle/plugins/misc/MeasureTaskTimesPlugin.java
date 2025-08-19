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
package org.apache.lucene.gradle.plugins.misc;

import java.util.Comparator;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.apache.lucene.gradle.plugins.LuceneGradlePlugin;
import org.gradle.api.Project;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.Provider;
import org.gradle.api.services.BuildService;
import org.gradle.api.services.BuildServiceParameters;
import org.gradle.build.event.BuildEventsListenerRegistry;
import org.gradle.tooling.events.FinishEvent;
import org.gradle.tooling.events.OperationCompletionListener;
import org.gradle.tooling.events.task.TaskFailureResult;
import org.gradle.tooling.events.task.TaskFinishEvent;
import org.gradle.tooling.events.task.TaskSuccessResult;

/**
 * Prints aggregate wall-clock times for all executed gradle tasks (collected across all projects).
 */
public class MeasureTaskTimesPlugin extends LuceneGradlePlugin {
  public static final String OPT_TASK_TIMES = "task.times";
  public static final String OPT_TASK_TIMES_AGGREGATE = "task.times.aggregate";
  public static final String OPT_TASK_TIMES_LIMIT = "task.times.limit";

  private final BuildEventsListenerRegistry listenerRegistry;

  @Inject
  public MeasureTaskTimesPlugin(BuildEventsListenerRegistry listenerRegistry) {
    this.listenerRegistry = listenerRegistry;
  }

  @Override
  public void apply(Project project) {
    applicableToRootProjectOnly(project);

    var buildOptions = getBuildOptions(project);
    var taskTimesOption =
        buildOptions.addBooleanOption(
            OPT_TASK_TIMES,
            "Measures wall-time task execution and provides a summary of the longest tasks at "
                + "the end of a successful build.",
            false);

    var aggregateOption =
        buildOptions.addBooleanOption(
            OPT_TASK_TIMES_AGGREGATE,
            "Aggregate task times by unique name (across all projects). If false, unique task paths are printed.",
            true);

    var taskCountLimitOption =
        buildOptions.addIntOption(OPT_TASK_TIMES_LIMIT, "Limit the list to the top-N tasks.", 20);

    if (!taskTimesOption.get()) {
      return;
    }

    Provider<TaskTimesService> taskTimesService =
        project
            .getGradle()
            .getSharedServices()
            .registerIfAbsent(
                "taskTimesService",
                TaskTimesService.class,
                service -> {
                  service.parameters(
                      params -> {
                        params.getAggregateByName().set(aggregateOption.get());
                        params.getLimit().set(taskCountLimitOption.get());
                      });
                });

    listenerRegistry.onTaskCompletion(taskTimesService);
  }

  /**
   * This service collects per-task durations and prints a summary when closed (at the end of the
   * build).
   */
  public abstract static class TaskTimesService
      implements BuildService<TaskTimesServiceParams>, OperationCompletionListener, AutoCloseable {
    private static final Logger LOGGER = Logging.getLogger(TaskTimesService.class);

    /** Execution time of all successful tasks. Keys are task paths, values are in millis. */
    private final ConcurrentHashMap<String, Long> taskTimes = new ConcurrentHashMap<>();

    private volatile boolean hadFailedTask;

    @Override
    public void onFinish(FinishEvent event) {
      if (event instanceof TaskFinishEvent tfe) {
        if (tfe.getResult() instanceof TaskSuccessResult successResult) {
          long durationMillis =
              Math.max(0, successResult.getEndTime() - successResult.getStartTime());
          String taskPath = tfe.getDescriptor().getTaskPath();
          taskTimes.compute(taskPath, (_, value) -> (value == null ? 0 : value) + durationMillis);
        }

        if (tfe.getResult() instanceof TaskFailureResult) {
          hadFailedTask = true;
        }
      }
    }

    private static String simpleTaskName(String path) {
      int idx = path.lastIndexOf(':');
      return (idx >= 0 && idx < path.length() - 1) ? path.substring(idx + 1) : path;
    }

    @Override
    public void close() {
      if (taskTimes.isEmpty() || hadFailedTask) return;

      var params = getParameters();

      Map<String, Long> localTaskTimes = taskTimes;

      if (params.getAggregateByName().get()) {
        localTaskTimes =
            localTaskTimes.entrySet().stream()
                .collect(Collectors.groupingBy(e -> simpleTaskName(e.getKey())))
                .entrySet()
                .stream()
                .collect(
                    Collectors.toMap(
                        Map.Entry::getKey,
                        e -> e.getValue().stream().mapToLong(Map.Entry::getValue).sum()));
      }

      String report =
          localTaskTimes.entrySet().stream()
              .sorted(
                  Comparator.comparingLong((Map.Entry<String, Long> e) -> e.getValue()).reversed())
              .map(
                  e ->
                      String.format(
                          Locale.ROOT, " %6.2f sec.  %s", e.getValue() / 1000.0, e.getKey()))
              .limit(params.getLimit().get())
              .collect(Collectors.joining("\n"));

      LOGGER.lifecycle(
          "\nSummary of top-{} task execution times ({}):\n{}",
          params.getLimit().get(),
          params.getAggregateByName().get()
              ? "aggregated by unique name, possibly running in parallel"
              : "unique task paths",
          report);
    }
  }

  public interface TaskTimesServiceParams extends BuildServiceParameters {
    Property<Boolean> getAggregateByName();

    Property<Integer> getLimit();
  }
}
