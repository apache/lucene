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
import java.nio.file.FileSystems;
import java.util.concurrent.SynchronousQueue;
import javax.swing.JFrame;
import javax.swing.UIManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.luke.app.desktop.components.LukeWindowProvider;
import org.apache.lucene.luke.app.desktop.components.dialog.menubar.OpenIndexDialogFactory;
import org.apache.lucene.luke.app.desktop.util.DialogOpener;
import org.apache.lucene.luke.app.desktop.util.FontUtils;
import org.apache.lucene.luke.app.desktop.util.MessageUtils;
import org.apache.lucene.luke.util.LoggerFactory;

/** Entry class for desktop Luke */
public class LukeMain {

  public static final String LOG_FILE =
      System.getProperty("user.home")
          + FileSystems.getDefault().getSeparator()
          + ".luke.d"
          + FileSystems.getDefault().getSeparator()
          + "luke.log";

  static {
    LoggerFactory.initGuiLogging(LOG_FILE);
  }

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static JFrame frame;

  public static JFrame getOwnerFrame() {
    return frame;
  }

  /** @return Returns {@code true} if GUI startup and initialization was successful. */
  private static boolean createAndShowGUI() {
    // uncaught error handler
    MessageBroker messageBroker = MessageBroker.getInstance();
    try {
      Thread.setDefaultUncaughtExceptionHandler((thread, cause) -> handle(cause, messageBroker));

      frame = new LukeWindowProvider().get();
      frame.setLocation(200, 100);
      frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
      frame.pack();
      frame.setVisible(true);

      OpenIndexDialogFactory openIndexDialogFactory = OpenIndexDialogFactory.getInstance();
      new DialogOpener<>(openIndexDialogFactory)
          .open(
              MessageUtils.getLocalizedMessage("openindex.dialog.title"),
              600,
              420,
              (factory) -> {});

      return true;
    } catch (Throwable e) {
      messageBroker.showUnknownErrorMessage();
      log.fatal("Cannot initialize components.", e);
      return false;
    }
  }

  public static void main(String[] args) throws Exception {
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
            guiThreadResult.put(createAndShowGUI());
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        });

    if (Boolean.FALSE.equals(guiThreadResult.take())) {
      // Use java logging just in case log4j didn't start up properly.
      java.util.logging.Logger.getGlobal()
          .severe("Luke could not start because of errors, see the log file: " + LOG_FILE);
      Runtime.getRuntime().exit(1);
    }
  }
}
