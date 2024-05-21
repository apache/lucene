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
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
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

import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

public final class ExtractJdkApis {
  
  private static final FileTime FIXED_FILEDATE = FileTime.from(Instant.parse("2022-01-01T00:00:00Z"));
  
  private static final String PATTERN_PANAMA_FOREIGN      = "java.base/java/{lang/foreign/*,nio/channels/FileChannel,util/Objects}";
  private static final String PATTERN_VECTOR_INCUBATOR    = "jdk.incubator.vector/jdk/incubator/vector/*";
  private static final String PATTERN_VECTOR_VM_INTERNALS = "java.base/jdk/internal/vm/vector/VectorSupport{,$Vector,$VectorMask,$VectorPayload,$VectorShuffle}";
  
  static final Map<Integer,List<String>> CLASSFILE_PATTERNS = Map.of(
      19, List.of(PATTERN_PANAMA_FOREIGN),
      20, List.of(PATTERN_PANAMA_FOREIGN, PATTERN_VECTOR_VM_INTERNALS, PATTERN_VECTOR_INCUBATOR),
      21, List.of(PATTERN_PANAMA_FOREIGN, PATTERN_VECTOR_VM_INTERNALS, PATTERN_VECTOR_INCUBATOR)
  );
  
  public static void main(String... args) throws IOException {
    if (args.length != 2) {
      throw new IllegalArgumentException("Need two parameters: java version, output file");
    }
    Integer jdk = Integer.valueOf(args[0]);
    if (jdk.intValue() != Runtime.version().feature()) {
      throw new IllegalStateException("Incorrect java version: " + Runtime.version().feature());
    }
    if (!CLASSFILE_PATTERNS.containsKey(jdk)) {
      throw new IllegalArgumentException("No support to extract stubs from java version: " + jdk);
    }
    var outputPath = Paths.get(args[1]);

    // create JRT filesystem and build a combined FileMatcher:
    var jrtPath = Paths.get(URI.create("jrt:/")).toRealPath();
    var patterns = CLASSFILE_PATTERNS.get(jdk).stream()
        .map(pattern -> jrtPath.getFileSystem().getPathMatcher("glob:" + pattern + ".class"))
        .toArray(PathMatcher[]::new);
    PathMatcher pattern = p -> Arrays.stream(patterns).anyMatch(matcher -> matcher.matches(p));
    
    // Collect all files to process:
    final List<Path> filesToExtract;
    try (var stream = Files.walk(jrtPath)) {
      filesToExtract = stream.filter(p -> pattern.matches(jrtPath.relativize(p))).collect(Collectors.toList());
    }
    
    // Process all class files:
    try (var out = new ZipOutputStream(Files.newOutputStream(outputPath))) {
      process(filesToExtract, out);
    }
  }

  private static void process(List<Path> filesToExtract, ZipOutputStream out) throws IOException {
    var classesToInclude = new HashSet<String>();
    var references = new HashMap<String, String[]>();
    var processed = new TreeMap<String, byte[]>();
    System.out.println("Transforming " + filesToExtract.size() + " class files...");
    for (Path p : filesToExtract) {
      try (var in = Files.newInputStream(p)) {
        var reader = new ClassReader(in);
        var cw = new ClassWriter(0);
        var cleaner = new Cleaner(cw, classesToInclude, references);
        reader.accept(cleaner, ClassReader.SKIP_CODE | ClassReader.SKIP_DEBUG | ClassReader.SKIP_FRAMES);
        processed.put(reader.getClassName(), cw.toByteArray());
      }
    }
    // recursively add all superclasses / interfaces of visible classes to classesToInclude:
    for (Set<String> a = classesToInclude; !a.isEmpty();) {
      a = a.stream().map(references::get).filter(Objects::nonNull).flatMap(Arrays::stream).collect(Collectors.toSet());
      classesToInclude.addAll(a);
    }
    // remove all non-visible or not referenced classes:
    processed.keySet().removeIf(Predicate.not(classesToInclude::contains));
    System.out.println("Writing " + processed.size() + " visible classes...");
    for (var cls : processed.entrySet()) {
      String cn = cls.getKey();
      System.out.println("Writing stub for class: " + cn);
      out.putNextEntry(new ZipEntry(cn.concat(".class")).setLastModifiedTime(FIXED_FILEDATE));
      out.write(cls.getValue());
      out.closeEntry();
    }
    classesToInclude.removeIf(processed.keySet()::contains);
    System.out.println("Referenced classes not included: " + classesToInclude);
  }
  
  static boolean isVisible(int access) {
    return (access & (Opcodes.ACC_PROTECTED | Opcodes.ACC_PUBLIC)) != 0;
  }
  
  static class Cleaner extends ClassVisitor {
    private static final String PREVIEW_ANN = "jdk/internal/javac/PreviewFeature";
    private static final String PREVIEW_ANN_DESCR = Type.getObjectType(PREVIEW_ANN).getDescriptor();
    
    private final Set<String> classesToInclude;
    private final Map<String, String[]> references;
    
    Cleaner(ClassWriter out, Set<String> classesToInclude, Map<String, String[]> references) {
      super(Opcodes.ASM9, out);
      this.classesToInclude = classesToInclude;
      this.references = references;
    }

    @Override
    public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
      super.visit(Opcodes.V11, access, name, signature, superName, interfaces);
      if (isVisible(access)) {
        classesToInclude.add(name);
      }
      references.put(name, Stream.concat(Stream.of(superName), Arrays.stream(interfaces)).toArray(String[]::new));
    }

    @Override
    public AnnotationVisitor visitAnnotation(String descriptor, boolean visible) {
      return Objects.equals(descriptor, PREVIEW_ANN_DESCR) ? null : super.visitAnnotation(descriptor, visible);
    }

    @Override
    public FieldVisitor visitField(int access, String name, String descriptor, String signature, Object value) {
      if (!isVisible(access)) {
        return null;
      }
      return new FieldVisitor(Opcodes.ASM9, super.visitField(access, name, descriptor, signature, value)) {
        @Override
        public AnnotationVisitor visitAnnotation(String descriptor, boolean visible) {
          return Objects.equals(descriptor, PREVIEW_ANN_DESCR) ? null : super.visitAnnotation(descriptor, visible);
        }
      };
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String descriptor, String signature, String[] exceptions) {
      if (!isVisible(access)) {
        return null;
      }
      return new MethodVisitor(Opcodes.ASM9, super.visitMethod(access, name, descriptor, signature, exceptions)) {
        @Override
        public AnnotationVisitor visitAnnotation(String descriptor, boolean visible) {
          return Objects.equals(descriptor, PREVIEW_ANN_DESCR) ? null : super.visitAnnotation(descriptor, visible);
        }
      };
    }

    @Override
    public void visitInnerClass(String name, String outerName, String innerName, int access) {
      if (!Objects.equals(outerName, PREVIEW_ANN)) {
        super.visitInnerClass(name, outerName, innerName, access);
      }
    }
    
    @Override
    public void visitPermittedSubclass(String c) {
    }

  }
  
}
