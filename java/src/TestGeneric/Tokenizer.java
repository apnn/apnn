package TestGeneric;

import static TestGeneric.Document.log;
import io.github.htools.extract.AbstractTokenizer;
import io.github.htools.extract.DefaultTokenizer;
import io.github.htools.extract.modules.ConvertHtmlASCIICodes;
import io.github.htools.extract.modules.ConvertHtmlSpecialCodes;
import io.github.htools.extract.modules.ConvertToLowercase;
import io.github.htools.extract.modules.ConvertUnicodeDiacritics;
import io.github.htools.extract.modules.RemoveFilteredWords;
import io.github.htools.extract.modules.RemoveHtmlSpecialCodes;
import io.github.htools.extract.modules.RemoveNonASCII;
import io.github.htools.extract.modules.RemoveNonAlphanumeric;
import io.github.htools.extract.modules.StemByteArray;
import io.github.htools.extract.modules.StemTokens;
import io.github.htools.extract.modules.TokenWord;
import io.github.htools.lib.ClassTools;
import io.github.htools.lib.Log;
import io.github.htools.words.StopWordsSmart;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
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
    static Tokenizer singleton;
    HashSet<String> stopwords = getUnstemmedStopwords();
    RemoveFilteredWords stopwordRemover = new RemoveFilteredWords(null, stopwords);
    StemTokens tokenStemmer = new StemTokens(null, "tokenize");

    public Tokenizer() {
        super();
    }

    public static Tokenizer get(Class<? extends Tokenizer> clazz) {
        if (singleton == null) {
            try {
                Constructor<Tokenizer> cons
                        = ClassTools.getAssignableConstructor(clazz, Tokenizer.class);
                singleton = ClassTools.construct(cons);
            } catch (ClassNotFoundException ex) {
                log.fatalexception(ex, "getTokenzizer( %s )", clazz.getCanonicalName());
            }
        }
        return singleton;
    }

    protected HashSet<String> getStemmedStopwords() {
        return StopWordsSmart.getStemmedFilterSet();
    }

    protected HashSet<String> getUnstemmedStopwords() {
        return StopWordsSmart.getUnstemmedFilterSet();
    }

    @Override
    public Class getTokenMarker() {
        return TokenWord.class;
    }

    protected RemoveFilteredWords getStopwordRemover() {
        if (stopwordRemover == null) {
            stopwordRemover = new RemoveFilteredWords(null, stopwords);
        }
        return stopwordRemover;
    }

    @Override
    protected void buildPreProcess() {
        this.addPreProcessor(ConvertHtmlASCIICodes.class);
        this.addPreProcessor(ConvertHtmlSpecialCodes.class);
        this.addPreProcessor(ConvertUnicodeDiacritics.class);
        this.addPreProcessor(new RemoveNonASCII(this, true));
        this.addPreProcessor(ConvertToLowercase.class);
        this.addPreProcessor(RemoveHtmlSpecialCodes.class);
        this.addPreProcessor(RemoveNonAlphanumeric.class);
    }

    @Override
    protected void buildProcess() {
    }

    public void removeStopWords() {
        addEndPipeline(getStopwordRemover());
    }

    /**
     * add Porter2 stemmer
     *
     * @return
     */
    public void stemWords() {
        stopwords = getStemmedStopwords();
        this.addEndPipeline(tokenStemmer);
    }

    public void stemWordsByteArray() {
        stopwords = getStemmedStopwords();
        this.addPreProcessor(StemByteArray.class);
    }

    /**
     * @param term
     * @return true if the term is in the stop word list
     */
    public boolean isStopword(String term) {
        return stopwords.contains(term);
    }

    public ArrayList<String> removeStopwords(ArrayList<String> terms) {
        return stopwordRemover.process(terms);
    }

    public ArrayList<String> stemWords(ArrayList<String> terms) {
        return stopwordRemover.process(terms);
    }
}
