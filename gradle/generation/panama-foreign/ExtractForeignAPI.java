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
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.commons.ClassRemapper;
import org.objectweb.asm.commons.SimpleRemapper;

public final class ExtractForeignAPI {
  
  private static final FileTime FIXED_FILEDATE = FileTime.from(Instant.parse("2022-01-01T00:00:00Z"));
  
  public static void main(String... args) throws IOException {
    if (args.length != 1) {
      throw new IllegalArgumentException("Need one parameter with output file.");
    }
    var outputPath = Paths.get(args[0]);
    var javaBaseModule = Paths.get(URI.create("jrt:/")).resolve("java.base").toRealPath();
    var fileMatcher = javaBaseModule.getFileSystem().getPathMatcher("glob:java/{lang/foreign/*,nio/channels/FileChannel}.class");
    try (var out = new ZipOutputStream(Files.newOutputStream(outputPath)); var stream = Files.walk(javaBaseModule)) {
      var filesToExtract = stream.map(javaBaseModule::relativize).filter(fileMatcher::matches).sorted().collect(Collectors.toList());
      for (Path relative : filesToExtract) {
        System.out.println("Processing class file: " + relative);
        try (var in = Files.newInputStream(javaBaseModule.resolve(relative))) {
          final var reader = new ClassReader(in);
          final var cw = new ClassWriter(0);
          reader.accept(new Handler(cw), ClassReader.SKIP_CODE | ClassReader.SKIP_DEBUG);
          out.putNextEntry(new ZipEntry(reader.getClassName().concat(".class")).setLastModifiedTime(FIXED_FILEDATE));
          out.write(cw.toByteArray());
          out.closeEntry();
        }
      }
    }
  }
  
  static class Handler extends ClassVisitor {
    
    Handler(ClassWriter out) {
      super(Opcodes.ASM9, new ClassRemapper(out, new SimpleRemapper("jdk/internal/javac/PreviewFeature", "jdk/internal/javac/NoPreviewFeature")));
    }
    
    private static boolean isHidden(int access) {
      return (access & (Opcodes.ACC_PROTECTED | Opcodes.ACC_PUBLIC)) == 0;
    }

    @Override
    public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
      super.visit(Opcodes.V11, access, name, signature, superName, interfaces);
    }

    @Override
    public FieldVisitor visitField(int access, String name, String descriptor, String signature, Object value) {
      if (isHidden(access)) {
        return null;
      }
      return super.visitField(access, name, descriptor, signature, value);
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String descriptor, String signature, String[] exceptions) {
      if (isHidden(access)) {
        return null;
      }
      return super.visitMethod(access, name, descriptor, signature, exceptions);
    }

    @Override
    public void visitPermittedSubclass(String permittedSubclass) {
      // not compatible with old compilers
    }

    @Override
    public void visitSource(String source, String debug) {
      // info not needed
    }

  }
  
}
