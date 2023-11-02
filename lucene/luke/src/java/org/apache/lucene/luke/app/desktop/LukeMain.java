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

package org.apache.lucene.luke.app.desktop;

import static org.apache.lucene.luke.app.desktop.util.ExceptionHandler.handle;

import java.awt.GraphicsEnvironment;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.JFrame;
import javax.swing.UIManager;
import org.apache.lucene.luke.app.desktop.components.LukeWindowProvider;
import org.apache.lucene.luke.app.desktop.components.dialog.menubar.OpenIndexDialogFactory;
import org.apache.lucene.luke.app.desktop.util.DialogOpener;
import org.apache.lucene.luke.app.desktop.util.FontUtils;
import org.apache.lucene.luke.app.desktop.util.MessageUtils;
import org.apache.lucene.luke.util.LoggerFactory;

/** Entry class for desktop Luke */
public class LukeMain {

  static {
    LoggerFactory.initGuiLogging();
  }

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static JFrame frame;

  public static JFrame getOwnerFrame() {
    return frame;
  }

  /**
   * @return Returns {@code true} if GUI startup and initialization was successful.
   */
  private static boolean createGUI() {
    // uncaught error handler
    MessageBroker messageBroker = MessageBroker.getInstance();
    try {
      Thread.setDefaultUncaughtExceptionHandler((thread, cause) -> handle(cause, messageBroker));

      frame = new LukeWindowProvider().get();
      frame.setLocation(200, 100);
      frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
      frame.pack();
      frame.setVisible(true);

      return true;
    } catch (Throwable e) {
      messageBroker.showUnknownErrorMessage();
      log.log(Level.SEVERE, "Cannot initialize components.", e);
      return false;
    }
  }

  public static void main(String[] args) throws Exception {
    boolean sanityCheck = Arrays.asList(args).contains("--sanity-check");

    if (sanityCheck && GraphicsEnvironment.isHeadless()) {
      Logger.getGlobal().log(Level.SEVERE, "[Vader] Hello, Luke. Can't do much in headless mode.");
      Runtime.getRuntime().exit(0);
    }

    String lookAndFeelClassName = UIManager.getSystemLookAndFeelClassName();
    if (!lookAndFeelClassName.contains("AquaLookAndFeel")
        && !lookAndFeelClassName.contains("PlasticXPLookAndFeel")) {
      // may be running on linux platform
      lookAndFeelClassName = "javax.swing.plaf.metal.MetalLookAndFeel";
    }
    UIManager.setLookAndFeel(lookAndFeelClassName);

    GraphicsEnvironment genv = GraphicsEnvironment.getLocalGraphicsEnvironment();
    genv.registerFont(FontUtils.createElegantIconFont());

    var guiThreadResult = new SynchronousQueue<Boolean>();
    javax.swing.SwingUtilities.invokeLater(
        () -> {
          try {
            long _start = System.nanoTime();
            guiThreadResult.put(createGUI());

            // Show the initial dialog.
            OpenIndexDialogFactory openIndexDialogFactory = OpenIndexDialogFactory.getInstance();
            new DialogOpener<>(openIndexDialogFactory)
                .open(
                    MessageUtils.getLocalizedMessage("openindex.dialog.title"),
                    600,
                    420,
                    (factory) -> {});

            long _end = System.nanoTime() / 1_000_000;
            log.info(
                "Elapsed time for initializing GUI: "
                    + TimeUnit.NANOSECONDS.toMillis(_end - _start)
                    + " ms");
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });

    if (Boolean.FALSE.equals(guiThreadResult.take())) {
      Logger.getGlobal().log(Level.SEVERE, "Luke could not start.");
      Runtime.getRuntime().exit(1);
    }

    if (sanityCheck) {
      // In sanity-check mode on non-headless displays, return success.
      Logger.getGlobal().log(Level.SEVERE, "[Vader] Hello, Luke. We seem to be fine.");
      Runtime.getRuntime().exit(0);
    }
  }
}
