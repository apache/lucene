package org.apache.lucene.analysis.ja;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.IOException;
import java.util.Map;

/**
 * A {@link TokenFilter} that normalizes small letters (捨て仮名) in hiragana into normal letters.
 * For instance, "ちょっとまって" will be translated to "ちよつとまつて".
 *
 * <p>This filter is useful if you want to search against old style Japanese text such as patents,
 * legal, contract policies, etc.
 */
public final class JapaneseHiraganaUppercaseFilter extends TokenFilter {
    private static final Map<Character, Character> s2l;

    static {
        // supported characters are:
        // ぁ ぃ ぅ ぇ ぉ っ ゃ ゅ ょ ゎ ゕ ゖ
        s2l = Map.ofEntries(
                Map.entry('ぁ', 'あ'),
                Map.entry('ぃ', 'い'),
                Map.entry('ぅ', 'う'),
                Map.entry('ぇ', 'え'),
                Map.entry('ぉ', 'お'),
                Map.entry('っ', 'つ'),
                Map.entry('ゃ', 'や'),
                Map.entry('ゅ', 'ゆ'),
                Map.entry('ょ', 'よ'),
                Map.entry('ゎ', 'わ'),
                Map.entry('ゕ', 'か'),
                Map.entry('ゖ', 'け')
        );
    }

    private final CharTermAttribute termAttr = addAttribute(CharTermAttribute.class);

    public JapaneseHiraganaUppercaseFilter(TokenStream input) {
        super(input);
    }

    @Override
    public boolean incrementToken() throws IOException {
        if (input.incrementToken()) {
            String term = termAttr.toString();
            char[] src = term.toCharArray();
            char[] result = new char[src.length];
            for (int i = 0; i < src.length; i++) {
                Character c = s2l.get(src[i]);
                if (c != null) {
                    result[i] = c;
                } else {
                    result[i] = src[i];
                }
            }
            String resultTerm = String.copyValueOf(result);
            termAttr.setEmpty().append(resultTerm);
            return true;
        } else {
            return false;
        }
    }
}
