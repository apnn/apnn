package TestGeneric;

import io.github.htools.extract.modules.ConvertHtmlASCIICodes;
import io.github.htools.extract.modules.ConvertHtmlSpecialCodes;
import io.github.htools.extract.modules.ConvertToLowercase;
import io.github.htools.extract.modules.ConvertUnicodeDiacritics;
import io.github.htools.extract.modules.ConvertWhitespace;
import io.github.htools.extract.modules.RemoveDanglingS;
import io.github.htools.extract.modules.RemoveHtmlSpecialCodes;
import io.github.htools.extract.modules.RemoveQuote;
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
public class ContentExtractorPAN extends ContentExtractor {

    public static Log log = new Log(ContentExtractorPAN.class);

    public ContentExtractorPAN() {
        super();
    }
    
    protected void build() {
        this.addPreProcessor(ConvertHtmlASCIICodes.class);
        this.addPreProcessor(ConvertHtmlSpecialCodes.class);
        this.addPreProcessor(ConvertUnicodeDiacritics.class);
        this.addPreProcessor(ConvertToLowercase.class);
        this.addPreProcessor(RemoveHtmlSpecialCodes.class);
        this.addPreProcessor(RemoveDanglingS.class);
        this.addPreProcessor(RemoveQuote.class);
        this.addPreProcessor(ConvertWhitespace.class);
        //this.addPreProcessor(RemoveShortWords.class);
        //this.addPreProcessor(RemoveNumbers.class);
        //this.addPreProcessor(new RemoveNonASCII(this, true));
    }    
}
