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
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.Objects;
import java.util.stream.Collectors;
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

public final class ExtractForeignAPI {
  
  private static final FileTime FIXED_FILEDATE = FileTime.from(Instant.parse("2022-01-01T00:00:00Z"));
  
  public static void main(String... args) throws IOException {
    if (args.length != 2) {
      throw new IllegalArgumentException("Need two parameters: java version, output file");
    }
    if (Integer.parseInt(args[0]) != Runtime.version().feature()) {
      throw new IllegalStateException("Incorrect java version: " + Runtime.version().feature());
    }
    var outputPath = Paths.get(args[1]);
    var javaBaseModule = Paths.get(URI.create("jrt:/")).resolve("java.base").toRealPath();
    var fileMatcher = javaBaseModule.getFileSystem().getPathMatcher("glob:java/{lang/foreign/*,nio/channels/FileChannel,util/Objects}.class");
    try (var out = new ZipOutputStream(Files.newOutputStream(outputPath)); var stream = Files.walk(javaBaseModule)) {
      var filesToExtract = stream.map(javaBaseModule::relativize).filter(fileMatcher::matches).sorted().collect(Collectors.toList());
      for (Path relative : filesToExtract) {
        System.out.println("Processing class file: " + relative);
        try (var in = Files.newInputStream(javaBaseModule.resolve(relative))) {
          final var reader = new ClassReader(in);
          final var cw = new ClassWriter(0);
          reader.accept(new Cleaner(cw), ClassReader.SKIP_CODE | ClassReader.SKIP_DEBUG | ClassReader.SKIP_FRAMES);
          out.putNextEntry(new ZipEntry(relative.toString()).setLastModifiedTime(FIXED_FILEDATE));
          out.write(cw.toByteArray());
          out.closeEntry();
        }
      }
    }
  }
  
  static class Cleaner extends ClassVisitor {
    private static final String PREVIEW_ANN = "jdk/internal/javac/PreviewFeature";
    private static final String PREVIEW_ANN_DESCR = Type.getObjectType(PREVIEW_ANN).getDescriptor();
    
    private boolean completelyHidden = false;
    
    Cleaner(ClassWriter out) {
      super(Opcodes.ASM9, out);
    }
    
    private boolean isHidden(int access) {
      return completelyHidden || (access & (Opcodes.ACC_PROTECTED | Opcodes.ACC_PUBLIC)) == 0;
    }

    @Override
    public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
      super.visit(Opcodes.V11, access, name, signature, superName, interfaces);
      completelyHidden = isHidden(access);
    }

    @Override
    public AnnotationVisitor visitAnnotation(String descriptor, boolean visible) {
      return Objects.equals(descriptor, PREVIEW_ANN_DESCR) ? null : super.visitAnnotation(descriptor, visible);
    }

    @Override
    public FieldVisitor visitField(int access, String name, String descriptor, String signature, Object value) {
      if (isHidden(access)) {
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
      if (isHidden(access)) {
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
    public void visitPermittedSubclass​(String c) {
    }

  }
  
}
