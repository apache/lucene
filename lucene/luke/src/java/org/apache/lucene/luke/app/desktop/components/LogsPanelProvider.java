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

package org.apache.lucene.luke.app.desktop.components;

import java.awt.*;
import java.awt.event.HierarchyEvent;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.stream.Collectors;
import javax.swing.*;
import org.apache.lucene.luke.app.desktop.util.MessageUtils;
import org.apache.lucene.luke.util.CircularLogBufferHandler;
import org.apache.lucene.luke.util.LoggerFactory;

/** Provider of the Logs panel */
public final class LogsPanelProvider {

  public LogsPanelProvider() {}

  public JPanel get() {
    JPanel panel = new JPanel(new BorderLayout());
    panel.setOpaque(false);
    panel.setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));

    JPanel header = new JPanel(new FlowLayout(FlowLayout.LEADING));
    header.setOpaque(false);
    header.add(new JLabel(MessageUtils.getLocalizedMessage("logs.label.level")));

    JComboBox<Level> logFilter =
        new JComboBox<>(
            new Level[] {
              Level.FINEST,
              Level.FINER,
              Level.FINE,
              Level.CONFIG,
              Level.INFO,
              Level.WARNING,
              Level.SEVERE,
              Level.OFF
            });
    logFilter.setEditable(false);
    logFilter.setSelectedItem(Level.INFO);
    header.add(logFilter);

    var logTextArea = createLogPanel(logFilter);

    panel.add(header, BorderLayout.PAGE_START);
    panel.add(new JScrollPane(logTextArea), BorderLayout.CENTER);
    return panel;
  }

  /** Prepare the component responsible for displaying logs. */
  private JTextArea createLogPanel(JComboBox<Level> logFilter) {
    JTextArea logTextArea = new JTextArea();
    logTextArea.setEditable(false);

    class LogRecordFormatter
        implements Function<CircularLogBufferHandler.ImmutableLogRecord, String> {
      @Override
      public String apply(CircularLogBufferHandler.ImmutableLogRecord record) {
        return String.format(
            Locale.ROOT,
            "%s [%s] %s: %s",
            DateTimeFormatter.ofPattern("HH:mm:ss", Locale.ROOT)
                .format(record.getInstant().atZone(ZoneId.systemDefault())),
            record.getLevel(),
            record.getLoggerName(),
            record.getMessage()
                + (record.getThrown() == null ? "" : "\n" + toString(record.getThrown())));
      }

      private String toString(Throwable t) {
        try (StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw)) {
          t.printStackTrace(pw);
          pw.flush();
          return sw.toString();
        } catch (IOException e) {
          return "Could not dump stack trace: " + e.getMessage();
        }
      }
    }

    // Hook into live data from the circular log buffer and update the initial state.
    Function<CircularLogBufferHandler.ImmutableLogRecord, String> formatter =
        new LogRecordFormatter();
    CircularLogBufferHandler.LogUpdateListener updater =
        records -> {
          // Create an immutable copy of the logs to display in the gui thread.
          ArrayList<CircularLogBufferHandler.ImmutableLogRecord> clonedCopy =
              new ArrayList<>(records);
          SwingUtilities.invokeLater(
              () -> {
                Level level = (Level) Objects.requireNonNull(logFilter.getSelectedItem());

                String logContent =
                    clonedCopy.stream()
                        .filter(record -> record.getLevel().intValue() > level.intValue())
                        .map(formatter::apply)
                        .collect(Collectors.joining("\n"));

                logTextArea.setText(logContent);
              });
        };

    var logBuffer = Objects.requireNonNull(LoggerFactory.circularBuffer);

    // Update state on filter change.
    logFilter.addActionListener(
        e -> {
          updater.accept(logBuffer.getLogRecords());
        });

    // Subscribe to log events and update state only when actually displayed.
    logTextArea.addHierarchyListener(
        (HierarchyEvent e) -> {
          if (e.getComponent() == logTextArea
              && (e.getChangeFlags() & HierarchyEvent.DISPLAYABILITY_CHANGED) != 0) {
            if (logTextArea.isDisplayable()) {
              logBuffer.addUpdateListener(updater);
              updater.accept(logBuffer.getLogRecords());
            } else {
              logBuffer.removeUpdateListener(updater);
            }
          }
        });

    return logTextArea;
  }
}
