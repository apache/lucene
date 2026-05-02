///usr/bin/env jbang "$0" "$@" ; exit $?
//JAVA 17+
//DEPS com.github.javaparser:javaparser-core:3.28.0

import com.github.javaparser.JavaParser;
import com.github.javaparser.ParserConfiguration;
import com.github.javaparser.ast.body.ClassOrInterfaceDeclaration;
import com.github.javaparser.ast.body.MethodDeclaration;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.stream.*;

/**
 * Finds test classes (in src/test trees) that do not descend from LuceneTestCase.
 *
 * A "test class" is any non-abstract class in a src/test source tree that has at
 * least one method whose name starts with "test".
 *
 * The superclass graph is built from ALL .java files under the given root (both
 * src/main and src/test), so abstract base classes defined inside the project are
 * resolved transitively. Classes whose superclass cannot be resolved within the
 * project are flagged with "(unresolved)" — those are the ones most worth reviewing.
 *
 * Usage / Docker:
 *   docker run --rm \
 *     -v jbang-cache:/root/.jbang/cache \
 *     -v "$(pwd)":/workdir \
 *     jbangdev/jbang \
 *     /workdir/dev-tools/refactorings/FindTestClassesNotExtendingLuceneTestCase.java /workdir
 */
public class FindTestClassesNotExtendingLuceneTestCase {

    public static void main(String[] args) throws IOException {
        Path root = args.length > 0 ? Paths.get(args[0]) : Paths.get(".");

        var config = new ParserConfiguration()
            .setLanguageLevel(ParserConfiguration.LanguageLevel.BLEEDING_EDGE);
        var parser = new JavaParser(config);

        // simple class name -> simple superclass name (first declared, ignoring generics)
        Map<String, String> superOf = new HashMap<>();
        // simple class name -> relative file path, for test classes with test* methods
        Map<String, String> testClasses = new LinkedHashMap<>();

        try (Stream<Path> paths = Files.walk(root)) {
            paths.filter(p -> p.toString().endsWith(".java"))
                 .sorted()
                 .forEach(file -> {
                     try {
                         var result = parser.parse(file);
                         if (!result.getResult().isPresent()) return;

                         String rel = root.relativize(file).toString();
                         boolean isTestTree = rel.contains("/src/test/");

                         result.getResult().get()
                             .findAll(ClassOrInterfaceDeclaration.class).forEach(cls -> {
                                 if (cls.isInterface()) return;

                                 String name = cls.getNameAsString();

                                 // Record superclass (simple name, strip generics)
                                 cls.getExtendedTypes().stream().findFirst().ifPresent(t ->
                                     superOf.put(name, t.getNameAsString()));

                                 // A test class: non-abstract, in a test tree, has test* methods
                                 if (isTestTree && !cls.isAbstract()) {
                                     boolean hasTestMethods = cls.findAll(MethodDeclaration.class)
                                         .stream()
                                         .anyMatch(m -> m.getNameAsString().startsWith("test"));
                                     if (hasTestMethods) {
                                         testClasses.put(name, rel);
                                     }
                                 }
                             });
                     } catch (IOException e) {
                         System.err.println("# skip: " + file);
                     }
                 });
        }

        // Transitive closure: all classes that are descendants of LuceneTestCase
        Set<String> descendants = new HashSet<>();
        descendants.add("LuceneTestCase");
        boolean changed = true;
        while (changed) {
            changed = false;
            for (var entry : superOf.entrySet()) {
                if (descendants.contains(entry.getValue()) && descendants.add(entry.getKey())) {
                    changed = true;
                }
            }
        }

        // Report test classes outside the LuceneTestCase hierarchy
        long count = testClasses.entrySet().stream()
            .filter(e -> !descendants.contains(e.getKey()))
            .peek(e -> {
                String sup = superOf.containsKey(e.getKey())
                    ? superOf.get(e.getKey())
                    : "(no superclass)";
                boolean resolved = descendants.contains(sup) || superOf.containsKey(sup);
                String tag = resolved ? "" : "  *** unresolved superclass";
                System.out.printf("%s  [extends %s]%s%n", e.getValue(), sup, tag);
            })
            .count();

        System.err.printf("%n# %d test class(es) not descending from LuceneTestCase%n", count);
    }
}
