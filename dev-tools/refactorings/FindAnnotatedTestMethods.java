///usr/bin/env jbang "$0" "$@" ; exit $?
//JAVA 17+
//DEPS com.github.javaparser:javaparser-core:3.28.0

import com.github.javaparser.JavaParser;
import com.github.javaparser.ParserConfiguration;
import com.github.javaparser.ast.body.MethodDeclaration;

import java.io.IOException;
import java.nio.file.*;
import java.util.stream.Stream;

/**
 * Finds all methods whose name starts with "test" and that carry a @Test annotation.
 *
 * Usage:
 *   jbang FindAnnotatedTestMethods.java [root-directory]
 *
 * Docker:
 *   docker run --rm \
 *     -v jbang-cache:/root/.jbang/cache \
 *     -v "$(pwd)":/workdir \
 *     jbangdev/jbang \
 *     jbang /workdir/dev-tools/scripts/FindAnnotatedTestMethods.java /workdir
 */
public class FindAnnotatedTestMethods {

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
                         parser.parse(file).getResult().ifPresent(cu ->
                             cu.findAll(MethodDeclaration.class).stream()
                               .filter(m -> m.getNameAsString().startsWith("test"))
                               .filter(m -> m.getAnnotations().stream()
                                   .anyMatch(a -> a.getNameAsString().equals("Test")))
                               .forEach(m -> {
                                   int line = m.getBegin().map(p -> p.line).orElse(-1);
                                   String rel = root.relativize(file).toString();
                                   String ann = m.getAnnotations().stream()
                                       .filter(a -> a.getNameAsString().equals("Test"))
                                       .findFirst()
                                       .map(Object::toString)
                                       .orElse("@Test");
                                   System.out.printf("%s:%d: %s %s%n",
                                       rel, line, ann, m.getSignature());
                               })
                         );
                     } catch (IOException e) {
                         System.err.println("# skip: " + file + " — " + e.getMessage());
                     }
                 });
        }
    }
}
