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
package org.apache.lucene.tests.util;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.support.AnnotationSupport;

/**
 * Control the default state (enabled/ disabled) of a test tag meta-annotation. The state can be
 * switched using a system property.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD})
@ExtendWith(TagState.Condition.class)
public @interface TagState {
  boolean enabled();

  String sysProperty();

  /** Execution condition to ignore a tagged test depending on the tag's system property state. */
  final class Condition implements ExecutionCondition {
    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
      var opt = context.getElement();
      if (opt.isPresent()) {
        var element = opt.get();
        var stateAnn_ = AnnotationSupport.findAnnotation(element, TagState.class);
        if (stateAnn_.isPresent()) {
          var stateAnn = stateAnn_.get();
          var tagAnns = AnnotationSupport.findRepeatableAnnotations(element, Tag.class);
          if (tagAnns.size() != 1) {
            throw new RuntimeException("Expected exactly one @Tag meta-annotation on: " + element);
          }

          String tag = tagAnns.getFirst().value();
          var enabled =
              context
                  .getConfigurationParameter(stateAnn.sysProperty(), Boolean::parseBoolean)
                  .orElse(stateAnn.enabled());
          return enabled
              ? ConditionEvaluationResult.enabled("@" + tag + " enabled")
              : ConditionEvaluationResult.disabled(
                  "@" + tag + " disabled; use -D" + stateAnn.sysProperty() + "=true to enable.");
        }
      }

      return ConditionEvaluationResult.enabled("no @TestGroupTag found");
    }
  }
}
