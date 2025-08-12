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
package org.apache.lucene.gradle.plugins.mrjar;

import java.io.IOException;
import java.lang.classfile.AccessFlags;
import java.lang.classfile.Attributes;
import java.lang.classfile.ClassFile;
import java.lang.classfile.ClassFileBuilder;
import java.lang.classfile.ClassFileElement;
import java.lang.classfile.ClassFileVersion;
import java.lang.classfile.ClassModel;
import java.lang.classfile.ClassTransform;
import java.lang.classfile.CodeModel;
import java.lang.classfile.FieldModel;
import java.lang.classfile.MethodModel;
import java.lang.classfile.MethodTransform;
import java.lang.classfile.attribute.InnerClassesAttribute;
import java.lang.classfile.attribute.RuntimeInvisibleAnnotationsAttribute;
import java.lang.classfile.attribute.SourceFileAttribute;
import java.lang.classfile.constantpool.ClassEntry;
import java.lang.constant.ClassDesc;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.AccessFlag;
import java.lang.reflect.ClassFileFormatVersion;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Extract API stubs from future JDK versions. This class must not have any dependencies outside of
 * the standard library.
 */
public final class ExtractJdkApis {
  private static final FileTime FIXED_FILEDATE =
      FileTime.from(Instant.parse("2025-05-05T00:00:00Z"));

  private static final String PATTERN_VECTOR_INCUBATOR =
      "jdk.incubator.vector/jdk/incubator/vector/*";
  private static final String PATTERN_VECTOR_VM_INTERNALS =
      "java.base/jdk/internal/vm/vector/VectorSupport{,$Vector,$VectorMask,$VectorPayload,$VectorShuffle}";

  static final Map<Integer, List<String>> CLASSFILE_PATTERNS =
      Map.of(24, List.of(PATTERN_VECTOR_VM_INTERNALS, PATTERN_VECTOR_INCUBATOR));

  private static final ClassDesc CD_PreviewFeature =
      ClassDesc.ofInternalName("jdk/internal/javac/PreviewFeature");

  public static void main(String... args) throws IOException {
    if (args.length != 3) {
      throw new IllegalArgumentException(
          "Required parameters: [target java version] [extract java version] [output apijar file], received: "
              + List.of(args));
    }
    int targetJdk = Integer.parseInt(args[0]);
    Integer extractJdk = Integer.valueOf(args[1]);
    int runtimeJdk = Runtime.version().feature();
    if (extractJdk.intValue() != runtimeJdk) {
      throw new IllegalStateException("Incorrect runtime java version: " + runtimeJdk);
    }
    if (extractJdk.intValue() < targetJdk) {
      throw new IllegalStateException(
          "extract java version " + extractJdk + " < target java version " + targetJdk);
    }
    if (!CLASSFILE_PATTERNS.containsKey(extractJdk)) {
      throw new IllegalArgumentException(
          "No support to extract stubs from java version: " + extractJdk);
    }
    var outputPath = Paths.get(args[2]);

    // the output class files need to be compatible with the targetJdk of our compilation, so we
    // need to adapt them:
    var classFileVersion =
        ClassFileVersion.of(ClassFileFormatVersion.valueOf("RELEASE_" + targetJdk).major(), 0);

    // create JRT filesystem and build a combined FileMatcher:
    var jrtPath = Paths.get(URI.create("jrt:/")).toRealPath();
    var patterns =
        CLASSFILE_PATTERNS.get(extractJdk).stream()
            .map(pattern -> jrtPath.getFileSystem().getPathMatcher("glob:" + pattern + ".class"))
            .toArray(PathMatcher[]::new);
    PathMatcher pattern = p -> Arrays.stream(patterns).anyMatch(matcher -> matcher.matches(p));

    // Collect all files to process:
    final List<Path> filesToExtract;
    try (var stream = Files.walk(jrtPath)) {
      filesToExtract = stream.filter(p -> pattern.matches(jrtPath.relativize(p))).toList();
    }

    // Process all class files:
    try (var out = new ZipOutputStream(Files.newOutputStream(outputPath))) {
      process(filesToExtract, out, classFileVersion);
    }
  }

  private static void process(
      List<Path> filesToExtract, ZipOutputStream out, ClassFileVersion classFileVersion)
      throws IOException {
    System.out.println("Loading and analyzing " + filesToExtract.size() + " class files...");
    var classesToInclude = new HashSet<String>();
    var toProcess = new TreeMap<String, ClassModel>();
    var cc =
        ClassFile.of(
            ClassFile.ConstantPoolSharingOption.NEW_POOL,
            ClassFile.DebugElementsOption.DROP_DEBUG,
            ClassFile.LineNumbersOption.DROP_LINE_NUMBERS,
            ClassFile.StackMapsOption.DROP_STACK_MAPS);
    for (Path p : filesToExtract) {
      ClassModel parsed = cc.parse(p);
      String internalName = parsed.thisClass().asInternalName();
      toProcess.put(internalName, parsed);
      if (isVisible(parsed.flags())) {
        classesToInclude.add(internalName);
      }
    }
    // recursively add all superclasses / interfaces / outer classes of visible classes to
    // classesToInclude:
    for (Set<String> a = classesToInclude; !a.isEmpty(); ) {
      classesToInclude.addAll(
          a =
              a.stream()
                  .map(toProcess::get)
                  .filter(Objects::nonNull)
                  .flatMap(ExtractJdkApis::getReferences)
                  .collect(Collectors.toSet()));
    }
    // remove all non-visible or not referenced classes:
    toProcess.keySet().removeIf(Predicate.not(classesToInclude::contains));
    // transformation of class files:
    System.out.println("Writing " + toProcess.size() + " visible classes...");
    for (var parsed : toProcess.values()) {
      String internalName = parsed.thisClass().asInternalName();
      System.out.println("Writing stub for class: " + internalName);
      ClassTransform ct =
          ClassTransform.dropping(
                  ce ->
                      switch (ce) {
                        case MethodModel e -> !isVisible(e.flags());
                        case FieldModel e -> !isVisible(e.flags());
                        case SourceFileAttribute _ -> true;
                        default -> false;
                      })
              .andThen(
                  (builder, ce) -> {
                    switch (ce) {
                      case ClassFileVersion _ -> builder.with(classFileVersion);
                      // the PreviewFeature annotation may refer to its own inner classes and
                      // therefore we must get rid of the inner class entry:
                      case InnerClassesAttribute a ->
                          builder.with(
                              InnerClassesAttribute.of(
                                  a.classes().stream()
                                      .filter(
                                          c ->
                                              !CD_PreviewFeature.equals(
                                                  c.outerClass()
                                                      .map(ClassEntry::asSymbol)
                                                      .orElse(null)))
                                      .toList()));
                      default -> builder.with(ce);
                    }
                  })
              .andThen(ExtractJdkApis::dropPreview)
              .andThen(
                  ClassTransform.transformingMethods(
                          MethodTransform.dropping(CodeModel.class::isInstance)
                              .andThen(ExtractJdkApis::dropPreview))
                      .andThen(ClassTransform.transformingFields(ExtractJdkApis::dropPreview)));
      out.putNextEntry(
          new ZipEntry(internalName.concat(".class")).setLastModifiedTime(FIXED_FILEDATE));
      out.write(cc.transformClass(parsed, ct));
      out.closeEntry();
    }
    // make sure that no classes are left over except those which are in java.base module
    classesToInclude.removeIf(toProcess.keySet()::contains);
    var missingClasses =
        classesToInclude.stream()
            .filter(
                internalName -> {
                  try {
                    return ClassDesc.ofInternalName(internalName)
                            .resolveConstantDesc(MethodHandles.publicLookup())
                            .getModule()
                        != Object.class.getModule();
                  } catch (ReflectiveOperationException _) {
                    return true;
                  }
                })
            .sorted()
            .toList();
    if (!missingClasses.isEmpty()) {
      throw new IllegalStateException(
          "Some referenced classes are not publicly available in java.base module: "
              + missingClasses);
    }
  }

  /**
   * returns all superclasses, interfaces, and outer classes of the parsed class as stream of
   * internal names
   */
  private static Stream<String> getReferences(ClassModel parsed) {
    var parents =
        Stream.concat(parsed.superclass().stream(), parsed.interfaces().stream())
            .map(ClassEntry::asInternalName)
            .collect(Collectors.toSet());
    var outerClasses =
        parsed.findAttributes(Attributes.innerClasses()).stream()
            .flatMap(a -> a.classes().stream())
            .filter(i -> parents.contains(i.innerClass().asInternalName()))
            .flatMap(i -> i.outerClass().stream())
            .map(ClassEntry::asInternalName);
    return Stream.concat(parents.stream(), outerClasses);
  }

  @SuppressWarnings("unchecked") // no idea how to get generics correct!?!
  private static <E extends ClassFileElement, B extends ClassFileBuilder<E, B>> void dropPreview(
      ClassFileBuilder<E, B> builder, E ele) {
    switch (ele) {
      case RuntimeInvisibleAnnotationsAttribute att ->
          builder.with(
              (E)
                  RuntimeInvisibleAnnotationsAttribute.of(
                      att.annotations().stream()
                          .filter(ann -> !CD_PreviewFeature.equals(ann.classSymbol()))
                          .toList()));
      default -> builder.with(ele);
    }
  }

  private static boolean isVisible(AccessFlags access) {
    return access.has(AccessFlag.PUBLIC) || access.has(AccessFlag.PROTECTED);
  }
}
