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

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.FlowLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.IOException;
import java.util.Objects;
import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTextArea;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.luke.app.desktop.MessageBroker;
import org.apache.lucene.luke.app.desktop.components.dialog.analysis.AnalysisChainDialogFactory;
import org.apache.lucene.luke.app.desktop.components.dialog.analysis.TokenAttributeDialogFactory;
import org.apache.lucene.luke.app.desktop.components.dialog.documents.AddDocumentDialogOperator;
import org.apache.lucene.luke.app.desktop.components.fragments.analysis.CustomAnalyzerPanelOperator;
import org.apache.lucene.luke.app.desktop.components.fragments.analysis.CustomAnalyzerPanelProvider;
import org.apache.lucene.luke.app.desktop.components.fragments.analysis.SimpleAnalyzeResultPanelOperator;
import org.apache.lucene.luke.app.desktop.components.fragments.analysis.SimpleAnalyzeResultPanelProvider;
import org.apache.lucene.luke.app.desktop.components.fragments.analysis.StepByStepAnalyzeResultPanelOperator;
import org.apache.lucene.luke.app.desktop.components.fragments.analysis.StepByStepAnalyzeResultPanelProvider;
import org.apache.lucene.luke.app.desktop.components.fragments.search.AnalyzerTabOperator;
import org.apache.lucene.luke.app.desktop.components.fragments.search.MLTTabOperator;
import org.apache.lucene.luke.app.desktop.util.DialogOpener;
import org.apache.lucene.luke.app.desktop.util.FontUtils;
import org.apache.lucene.luke.app.desktop.util.MessageUtils;
import org.apache.lucene.luke.app.desktop.util.StyleConstants;
import org.apache.lucene.luke.models.analysis.Analysis;
import org.apache.lucene.luke.models.analysis.AnalysisFactory;
import org.apache.lucene.luke.models.analysis.CustomAnalyzerConfig;

/** Provider of the Analysis panel */
public final class AnalysisPanelProvider implements AnalysisTabOperator {

  private final ComponentOperatorRegistry operatorRegistry;

  private final AnalysisChainDialogFactory analysisChainDialogFactory;

  private final TokenAttributeDialogFactory tokenAttrDialogFactory;

  private final MessageBroker messageBroker;

  private final JPanel mainPanel = new JPanel();

  private final JPanel custom;

  private final JLabel analyzerNameLbl = new JLabel();

  private final JLabel showChainLbl = new JLabel();

  private final JTextArea inputArea = new JTextArea();

  private final JPanel lowerPanel = new JPanel(new BorderLayout());

  private final JPanel simpleResult;

  private final JPanel stepByStepResult;

  private final JCheckBox stepByStepCB = new JCheckBox();

  private final ListenerFunctions listeners = new ListenerFunctions();

  private Analysis analysisModel;

  public AnalysisPanelProvider() throws IOException {
    this.custom = new CustomAnalyzerPanelProvider().get();

    this.operatorRegistry = ComponentOperatorRegistry.getInstance();
    this.analysisChainDialogFactory = AnalysisChainDialogFactory.getInstance();
    this.tokenAttrDialogFactory = TokenAttributeDialogFactory.getInstance();
    this.messageBroker = MessageBroker.getInstance();

    this.analysisModel = new AnalysisFactory().newInstance();

    this.simpleResult = new SimpleAnalyzeResultPanelProvider(tokenAttrDialogFactory).get();
    this.stepByStepResult = new StepByStepAnalyzeResultPanelProvider(tokenAttrDialogFactory).get();

    operatorRegistry.register(AnalysisTabOperator.class, this);

    operatorRegistry
        .get(CustomAnalyzerPanelOperator.class)
        .ifPresent(
            operator -> {
              operator.setAnalysisModel(analysisModel);
              operator.resetAnalysisComponents();
            });
    stepByStepCB.setVisible(true);
  }

  public JPanel get() {
    JPanel panel = new JPanel(new GridLayout(1, 1));
    panel.setOpaque(false);
    panel.setBorder(BorderFactory.createLineBorder(Color.gray));

    JSplitPane splitPane =
        new JSplitPane(JSplitPane.VERTICAL_SPLIT, initUpperPanel(), initLowerPanel());
    splitPane.setOpaque(false);
    splitPane.setDividerLocation(320);
    panel.add(splitPane);

    return panel;
  }

  private JPanel initUpperPanel() {
    mainPanel.setOpaque(false);
    mainPanel.setLayout(new BorderLayout());
    mainPanel.setBorder(BorderFactory.createEmptyBorder(3, 3, 3, 3));

    mainPanel.add(custom, BorderLayout.CENTER);

    return mainPanel;
  }

  private JPanel initLowerPanel() {
    JPanel inner1 = new JPanel(new BorderLayout());
    inner1.setOpaque(false);

    JPanel analyzerName = new JPanel(new FlowLayout(FlowLayout.LEADING, 10, 2));
    analyzerName.setOpaque(false);
    analyzerName.add(
        new JLabel(MessageUtils.getLocalizedMessage("analysis.label.selected_analyzer")));
    analyzerNameLbl.setText(analysisModel.currentAnalyzer().getClass().getName());
    analyzerName.add(analyzerNameLbl);
    showChainLbl.setText(MessageUtils.getLocalizedMessage("analysis.label.show_chain"));
    showChainLbl.addMouseListener(
        new MouseAdapter() {
          @Override
          public void mouseClicked(MouseEvent e) {
            listeners.showAnalysisChain(e);
          }
        });
    showChainLbl.setVisible(analysisModel.currentAnalyzer() instanceof CustomAnalyzer);
    analyzerName.add(FontUtils.toLinkText(showChainLbl));
    inner1.add(analyzerName, BorderLayout.PAGE_START);

    JPanel input = new JPanel(new FlowLayout(FlowLayout.LEADING, 5, 2));
    input.setOpaque(false);
    inputArea.setRows(3);
    inputArea.setColumns(50);
    inputArea.setLineWrap(true);
    inputArea.setWrapStyleWord(true);
    inputArea.setText(MessageUtils.getLocalizedMessage("analysis.textarea.prompt"));
    input.add(new JScrollPane(inputArea));

    JButton executeBtn =
        new JButton(
            FontUtils.elegantIconHtml(
                "&#xe007;", MessageUtils.getLocalizedMessage("analysis.button.test")));
    executeBtn.setFont(StyleConstants.FONT_BUTTON_LARGE);
    executeBtn.setMargin(new Insets(3, 3, 3, 3));
    executeBtn.addActionListener(listeners::executeAnalysis);
    input.add(executeBtn);

    stepByStepCB.setText(MessageUtils.getLocalizedMessage("analysis.checkbox.step_by_step"));
    stepByStepCB.setSelected(false);
    stepByStepCB.setOpaque(false);
    stepByStepCB.setVisible(true);
    input.add(stepByStepCB);

    JButton clearBtn = new JButton(MessageUtils.getLocalizedMessage("button.clear"));
    clearBtn.setFont(StyleConstants.FONT_BUTTON_LARGE);
    clearBtn.setMargin(new Insets(5, 5, 5, 5));
    clearBtn.addActionListener(
        e -> {
          inputArea.setText("");
          operatorRegistry
              .get(SimpleAnalyzeResultPanelOperator.class)
              .ifPresent(SimpleAnalyzeResultPanelOperator::clearTable);
          operatorRegistry
              .get(StepByStepAnalyzeResultPanelOperator.class)
              .ifPresent(StepByStepAnalyzeResultPanelOperator::clearTable);
        });
    input.add(clearBtn);

    inner1.add(input, BorderLayout.CENTER);

    lowerPanel.setOpaque(false);
    lowerPanel.setBorder(BorderFactory.createEmptyBorder(3, 3, 3, 3));
    lowerPanel.add(inner1, BorderLayout.PAGE_START);
    lowerPanel.add(this.simpleResult, BorderLayout.CENTER);

    return lowerPanel;
  }

  // control methods
  void executeAnalysis() {
    String text = inputArea.getText();
    if (Objects.isNull(text) || text.isEmpty()) {
      messageBroker.showStatusMessage(
          MessageUtils.getLocalizedMessage("analysis.message.empry_input"));
    }

    lowerPanel.remove(stepByStepResult);
    lowerPanel.add(simpleResult, BorderLayout.CENTER);

    operatorRegistry
        .get(SimpleAnalyzeResultPanelOperator.class)
        .ifPresent(
            operator -> {
              operator.setAnalysisModel(analysisModel);
              operator.executeAnalysis(text);
            });

    lowerPanel.setVisible(false);
    lowerPanel.setVisible(true);
  }

  void executeAnalysisStepByStep() {
    String text = inputArea.getText();
    if (Objects.isNull(text) || text.isEmpty()) {
      messageBroker.showStatusMessage(
          MessageUtils.getLocalizedMessage("analysis.message.empry_input"));
    }
    lowerPanel.remove(simpleResult);
    lowerPanel.add(stepByStepResult, BorderLayout.CENTER);
    operatorRegistry
        .get(StepByStepAnalyzeResultPanelOperator.class)
        .ifPresent(
            operator -> {
              operator.setAnalysisModel(analysisModel);
              operator.executeAnalysisStepByStep(text);
            });

    lowerPanel.setVisible(false);
    lowerPanel.setVisible(true);
  }

  void showAnalysisChainDialog() {
    if (getCurrentAnalyzer() instanceof CustomAnalyzer) {
      CustomAnalyzer analyzer = (CustomAnalyzer) getCurrentAnalyzer();
      new DialogOpener<>(analysisChainDialogFactory)
          .open(
              "Analysis chain",
              600,
              320,
              (factory) -> {
                factory.setAnalyzer(analyzer);
              });
    }
  }

  @Override
  public void setAnalyzerByType(String analyzerType) {
    analysisModel.createAnalyzerFromClassName(analyzerType);
    analyzerNameLbl.setText(analysisModel.currentAnalyzer().getClass().getName());
    showChainLbl.setVisible(false);
    operatorRegistry
        .get(AnalyzerTabOperator.class)
        .ifPresent(operator -> operator.setAnalyzer(analysisModel.currentAnalyzer()));
    operatorRegistry
        .get(MLTTabOperator.class)
        .ifPresent(operator -> operator.setAnalyzer(analysisModel.currentAnalyzer()));
    operatorRegistry
        .get(AddDocumentDialogOperator.class)
        .ifPresent(operator -> operator.setAnalyzer(analysisModel.currentAnalyzer()));
  }

  @Override
  public void setAnalyzerByCustomConfiguration(CustomAnalyzerConfig config) {
    analysisModel.buildCustomAnalyzer(config);
    analyzerNameLbl.setText(analysisModel.currentAnalyzer().getClass().getName());
    showChainLbl.setVisible(true);
    operatorRegistry
        .get(AnalyzerTabOperator.class)
        .ifPresent(operator -> operator.setAnalyzer(analysisModel.currentAnalyzer()));
    operatorRegistry
        .get(MLTTabOperator.class)
        .ifPresent(operator -> operator.setAnalyzer(analysisModel.currentAnalyzer()));
    operatorRegistry
        .get(AddDocumentDialogOperator.class)
        .ifPresent(operator -> operator.setAnalyzer(analysisModel.currentAnalyzer()));
  }

  @Override
  public Analyzer getCurrentAnalyzer() {
    return analysisModel.currentAnalyzer();
  }

  private class ListenerFunctions {

    void showAnalysisChain(MouseEvent e) {
      AnalysisPanelProvider.this.showAnalysisChainDialog();
    }

    void executeAnalysis(ActionEvent e) {
      if (AnalysisPanelProvider.this.stepByStepCB.isSelected()) {
        AnalysisPanelProvider.this.executeAnalysisStepByStep();
      } else {
        AnalysisPanelProvider.this.executeAnalysis();
      }
    }
  }
}
