///usr/bin/env jbang "$0" "$@" ; exit $?
//JAVA 17+
//DEPS com.github.javaparser:javaparser-core:3.28.0

import com.github.javaparser.JavaParser;
import com.github.javaparser.ParserConfiguration;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.body.MethodDeclaration;
import com.github.javaparser.ast.expr.AnnotationExpr;
import com.github.javaparser.printer.lexicalpreservation.LexicalPreservingPrinter;

import java.io.IOException;
import java.nio.file.*;
import java.util.List;
import java.util.stream.Stream;

/**
 * Removes redundant @Test annotations from methods whose names already start with "test".
 * Also removes the "import org.junit.Test" line if no other @Test usages remain in the file.
 * Uses LexicalPreservingPrinter so only the removed nodes are affected; no reformatting occurs.
 *
 * Usage:
 *   jbang RemoveRedundantTestAnnotations.java [root-directory]
 *
 * Docker:
 *   docker run --rm \
 *     -v jbang-cache:/root/.jbang/cache \
 *     -v "$(pwd)":/workdir \
 *     jbangdev/jbang \
 *     /workdir/dev-tools/scripts/RemoveRedundantTestAnnotations.java /workdir
 */
public class RemoveRedundantTestAnnotations {

    public static void main(String[] args) throws IOException {
        Path root = args.length > 0 ? Paths.get(args[0]) : Paths.get(".");

        var config = new ParserConfiguration()
            .setLanguageLevel(ParserConfiguration.LanguageLevel.BLEEDING_EDGE);
        var parser = new JavaParser(config);

        try (Stream<Path> paths = Files.walk(root)) {
            paths.filter(p -> p.toString().endsWith(".java"))
                 .sorted()
                 .forEach(file -> {
                     try {
                         var result = parser.parse(file);
                         if (!result.isSuccessful()) {
                             System.err.println("# parse error: " + file);
                             return;
                         }

                         CompilationUnit cu = result.getResult().get();
                         LexicalPreservingPrinter.setup(cu);

                         boolean modified = false;

                         for (MethodDeclaration m : cu.findAll(MethodDeclaration.class)) {
                             if (!m.getNameAsString().startsWith("test")) continue;

                             List<AnnotationExpr> toRemove = m.getAnnotations().stream()
                                 .filter(a -> a.getNameAsString().equals("Test"))
                                 .toList();

                             for (AnnotationExpr ann : toRemove) {
                                 m.getAnnotations().remove(ann);
                                 modified = true;
                             }
                         }

                         if (!modified) return;

                         // Remove the import if no @Test annotations remain anywhere in the file.
                         boolean hasOtherTestAnnotations = cu.findAll(AnnotationExpr.class).stream()
                             .anyMatch(a -> a.getNameAsString().equals("Test"));

                         if (!hasOtherTestAnnotations) {
                             cu.getImports().removeIf(imp ->
                                 imp.getNameAsString().equals("org.junit.Test"));
                         }

                         String rel = root.relativize(file).toString();
                         System.out.println(rel);
                         Files.writeString(file, LexicalPreservingPrinter.print(cu));

                     } catch (IOException e) {
                         System.err.println("# error: " + file + " — " + e.getMessage());
                     }
                 });
        }
    }
}
