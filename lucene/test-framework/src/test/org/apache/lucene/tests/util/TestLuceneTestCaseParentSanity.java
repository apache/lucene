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

import java.io.IOException;
import java.io.InputStream;
import java.lang.classfile.Attributes;
import java.lang.classfile.ClassFile;
import java.lang.classfile.ClassModel;
import java.lang.classfile.constantpool.ClassEntry;
import java.lang.classfile.constantpool.Utf8Entry;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Ensures {@link LuceneTestCaseParent} remains (mostly) independent of the test framework used: any
 * junit4 or junit-jupiter specific code should live in {@link LuceneTestCase} or {@link
 * LuceneTestCaseJupiter}.
 */
public class TestLuceneTestCaseParentSanity extends LuceneTestCase {
  /** The only classes {@link LuceneTestCaseParent} is allowed to reference (as bytecode refs). */
  private static final Set<String> ALLOWED =
      Set.of("org/junit/Assert", "org/junit/internal/AssumptionViolatedException");

  private static final String JUNIT_PREFIX = "org/junit/";

  public void testNoJUnitSpecificReferences() throws IOException {
    Set<String> referenced = new TreeSet<>();

    // Parent test class and its nested classes.
    List<String> classFiles = new ArrayList<>();
    ClassModel host = parse(LuceneTestCaseParent.class.getSimpleName() + ".class");
    classFiles.add(host.thisClass().asInternalName());
    host.findAttribute(Attributes.nestMembers())
        .ifPresent(
            attr ->
                attr.nestMembers().stream()
                    .map(ClassEntry::asInternalName)
                    .forEach(classFiles::add));

    for (String internalName : classFiles) {
      ClassModel model =
          parse(internalName.substring(internalName.lastIndexOf('/') + 1) + ".class");
      for (var entry : model.constantPool()) {
        if (entry instanceof Utf8Entry utf8) {
          var internalClassRef = utf8.stringValue();
          if (internalClassRef.startsWith(JUNIT_PREFIX) && !ALLOWED.contains(internalClassRef)) {
            referenced.add(utf8.stringValue());
          }
        }
      }
    }

    assertEquals(
        "LuceneTestCaseParent must not reference test framework-specific classes, move such code to"
            + " LuceneTestCase or LuceneTestCaseJupiter.",
        Set.of(),
        referenced);
  }

  private static ClassModel parse(String resource) throws IOException {
    try (InputStream is = LuceneTestCaseParent.class.getResourceAsStream(resource)) {
      assertNotNull("Class file not found: " + resource, is);
      return ClassFile.of().parse(is.readAllBytes());
    }
  }
}
