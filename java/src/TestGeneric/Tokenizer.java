package TestGeneric;

import io.github.htools.extract.AbstractTokenizer;
import io.github.htools.extract.DefaultTokenizer;
import io.github.htools.extract.modules.ConvertHtmlASCIICodes;
import io.github.htools.extract.modules.ConvertHtmlSpecialCodes;
import io.github.htools.extract.modules.ConvertToLowercase;
import io.github.htools.extract.modules.ConvertUnicodeDiacritics;
import io.github.htools.extract.modules.RemoveFilteredWords;
import io.github.htools.extract.modules.RemoveHtmlSpecialCodes;
import io.github.htools.extract.modules.RemoveNonASCII;
import io.github.htools.extract.modules.StemTokens;
import io.github.htools.extract.modules.TokenWord;
import io.github.htools.lib.Log;
import io.github.htools.words.StopWordsSmart;
import java.util.HashSet;

/**
 * This is the default tokenizer used, it replaces all non alphanumeric
 * characters with whitespace, converts HTML special characters (e.g. &gl;,
 * &#65;) to ASCII, when possible converts diacritics to their ACSII equivalent
 * (e.g. e with diacritic becomes e), removes remaining non-ASCII characters,
 * lowercases, and creates tokens of words separated by whitespace. The
 * tokenizer works in situ, therefore the remaining byte array used as input
 * will reflect the same changes, having \0 bytes for all removed bytes.
 *
 * @author Jeroen
 */
public class Tokenizer extends AbstractTokenizer {

    public static final Log log = new Log(DefaultTokenizer.class);
    static HashSet<String> unstemmedFilterSet = StopWordsSmart.getUnstemmedFilterSet();

    public Tokenizer() {
        super();
    }

    @Override
    public Class getTokenMarker() {
        return TokenWord.class;
    }

    @Override
    protected void buildPreProcess() {
        this.addPreProcessor(ConvertHtmlASCIICodes.class);
        this.addPreProcessor(ConvertHtmlSpecialCodes.class);
        this.addPreProcessor(ConvertUnicodeDiacritics.class);
        this.addPreProcessor(new RemoveNonASCII(this, true));
        this.addPreProcessor(ConvertToLowercase.class);
    }

    @Override
    protected void buildProcess() {
        this.addProcess(RemoveHtmlSpecialCodes.class);
    }
    
    public AbstractTokenizer removeStopWords() {
        addEndPipeline(new RemoveFilteredWords(this, unstemmedFilterSet));
        return this;
    }
    
    /**
     * add Porter2 stemmer
     * @return 
     */
    public AbstractTokenizer stemWords() {
        this.getTokenMarker().asSubclass(StemTokens.class);
        return this;
    }
    
    /**
     * @param term
     * @return true if the term is in the stop word list
     */
    public static boolean isStopword(String term) {
        return unstemmedFilterSet.contains(term);
    }
}
