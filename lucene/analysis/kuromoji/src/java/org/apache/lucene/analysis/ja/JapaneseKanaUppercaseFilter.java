package org.apache.lucene.analysis.ja;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public final class JapaneseKanaUppercaseFilter extends TokenFilter {
    private final CharTermAttribute termAttr = addAttribute(CharTermAttribute.class);

    private Map<Character, Character> s2l;

    public JapaneseKanaUppercaseFilter(TokenStream input) {
        super(input);
        s2l = new HashMap<Character, Character>();
        s2l.put('ぁ', 'あ');
        s2l.put('ぃ', 'い');
        s2l.put('ぅ', 'う');
        s2l.put('ぇ', 'え');
        s2l.put('ぉ', 'お');
        s2l.put('ゃ', 'や');
        s2l.put('ゅ', 'ゆ');
        s2l.put('ょ', 'よ');
        s2l.put('っ', 'つ');
        s2l.put('ゎ', 'わ');

        s2l.put('ァ', 'ア');
        s2l.put('ィ', 'イ');
        s2l.put('ゥ', 'ウ');
        s2l.put('ェ', 'エ');
        s2l.put('ォ', 'オ');
        s2l.put('ャ', 'ヤ');
        s2l.put('ュ', 'ユ');
        s2l.put('ョ', 'ヨ');
        s2l.put('ッ', 'ツ');
        s2l.put('ヮ', 'ワ');
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
