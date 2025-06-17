package org.apache.lucene.gradle.plugins.spotless;

import com.diffplug.spotless.FormatterFunc;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Wildcard import detector for spotless.
 *
 * @see "https://github.com/apache/lucene/issues/14553"
 */
public class WildcardImportDetector implements Serializable, FormatterFunc {
  private static final Pattern WILDCARD_IMPORT_PATTERN =
      Pattern.compile("(^import)(\\s+)(?:static\\s*)?([^*\\s]+\\.\\*;)", Pattern.MULTILINE);

  @Override
  public String apply(String input) throws Exception {
    Matcher matcher = WILDCARD_IMPORT_PATTERN.matcher(input);
    ArrayList<String> matches = new ArrayList<>();
    while (matcher.find()) {
      matches.add(matcher.group());
    }

    if (matches.isEmpty()) {
      return input;
    }

    String msg = "Replace with explicit imports (spotless can't fix it automatically):";
    if (matches.size() == 1) {
      throw new AssertionError(msg + matches.getFirst());
    } else {
      throw new AssertionError(
          msg
              + "\n"
              + matches.stream().map(match -> "  => " + match).collect(Collectors.joining("\n")));
    }
  }
}
