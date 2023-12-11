package org.apache.lucene.analysis.ja;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.IOException;
import java.util.Map;

public final class JapaneseKanaUppercaseFilter extends TokenFilter {
    private static final Map<Character, Character> s2l;

    static {
        // supported characters are:
        // ぁ ぃ ぅ ぇ ぉ っ ゃ ゅ ょ ゎ ゕ ゖ ァ ィ ゥ ェ ォ ヵ ㇰ ヶ ㇱ ㇲ ッ ㇳ ㇴ ㇵ ㇶ ㇷ ㇷ゚ ㇸ ㇹ ㇺ ャ ュ ョ ㇻ ㇼ ㇽ ㇾ ㇿ ヮ
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
                Map.entry('ゖ', 'け'),
                Map.entry('ァ', 'ア'),
                Map.entry('ィ', 'イ'),
                Map.entry('ゥ', 'ウ'),
                Map.entry('ェ', 'エ'),
                Map.entry('ォ', 'オ'),
                Map.entry('ヵ', 'カ'),
                Map.entry('ㇰ', 'ク'),
                Map.entry('ヶ', 'ケ'),
                Map.entry('ㇱ', 'シ'),
                Map.entry('ㇲ', 'ス'),
                Map.entry('ッ', 'ツ'),
                Map.entry('ㇳ', 'ト'),
                Map.entry('ㇴ', 'ヌ'),
                Map.entry('ㇵ', 'ハ'),
                Map.entry('ㇶ', 'ヒ'),
                Map.entry('ㇷ', 'フ'),
                Map.entry('ㇸ', 'ヘ'),
                Map.entry('ㇹ', 'ホ'),
                Map.entry('ㇺ', 'ム'),
                Map.entry('ャ', 'ヤ'),
                Map.entry('ュ', 'ユ'),
                Map.entry('ョ', 'ヨ'),
                Map.entry('ㇻ', 'ラ'),
                Map.entry('ㇼ', 'リ'),
                Map.entry('ㇽ', 'ル'),
                Map.entry('ㇾ', 'レ'),
                Map.entry('ㇿ', 'ロ'),
                Map.entry('ヮ', 'ワ')
        );
    }

    private final CharTermAttribute termAttr = addAttribute(CharTermAttribute.class);

    public JapaneseKanaUppercaseFilter(TokenStream input) {
        super(input);
    }

    @Override
    public boolean incrementToken() throws IOException {
        if (input.incrementToken()) {
            String term = termAttr.toString();
            // Small letter "ㇷ゚" is not single character, so it should be converted to "プ" as String
            term = term.replace("ㇷ゚", "プ");
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
