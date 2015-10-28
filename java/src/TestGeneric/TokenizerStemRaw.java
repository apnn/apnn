package TestGeneric;

import io.github.htools.extract.DefaultTokenizer;
import io.github.htools.lib.Log;

/**
 * This is the default tokenizer used, it replaces all non alphanumeric
 * characters with whitespace, converts HTML special characters (e.g. &gl;,
 * &#65;) to ASCII, when possible converts diacritics to their ACSII equivalent
 * (e.g. e with diacritic becomes e), removes remaining non-ASCII characters,
 * lowercases, and creates tokens of words separated by whitespace. The
 * tokenizer works in situ, therefore the remaining byte array used as input
 * will reflect the same changes, having \0 bytes for all removed bytes.
 *
 * This version of the tokenizer also removes stopwords in the
 * StopWordsMultiLang set.
 *
 * @author Jeroen
 */
public class TokenizerStemRaw extends Tokenizer {

    public static Log log = new Log(TokenizerStemRaw.class);

    public TokenizerStemRaw() {
        super();
        this.stemWordsByteArray();
    }

}
