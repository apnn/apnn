package TestGeneric;

import io.github.htools.extract.AbstractTokenizer;
import io.github.htools.extract.Content;
import io.github.htools.extract.DefaultTokenizer;
import io.github.htools.extract.Extractor;
import io.github.htools.extract.modules.ConvertHtmlASCIICodes;
import io.github.htools.extract.modules.ConvertHtmlSpecialCodes;
import io.github.htools.extract.modules.ConvertToLowercase;
import io.github.htools.extract.modules.ConvertUnicodeDiacritics;
import io.github.htools.extract.modules.ConvertWhitespace;
import io.github.htools.extract.modules.ExtractorProcessor;
import io.github.htools.extract.modules.MarkHL;
import io.github.htools.extract.modules.MarkHead;
import io.github.htools.extract.modules.MarkHeadline;
import io.github.htools.extract.modules.MarkITAG56;
import io.github.htools.extract.modules.MarkTTL;
import io.github.htools.extract.modules.MarkText;
import io.github.htools.extract.modules.MarkTitle;
import io.github.htools.extract.modules.RemoveDanglingS;
import io.github.htools.extract.modules.RemoveFilteredWords;
import io.github.htools.extract.modules.RemoveHtmlComment;
import io.github.htools.extract.modules.RemoveHtmlSpecialCodes;
import io.github.htools.extract.modules.RemoveHtmlTags;
import io.github.htools.extract.modules.RemoveNonAlphanumericQuote;
import io.github.htools.extract.modules.RemoveSection;
import io.github.htools.extract.modules.RemoveSingleQuotes;
import io.github.htools.extract.modules.StemByteArray;
import io.github.htools.extract.modules.StemTokens;
import io.github.htools.extract.modules.TokenWordQuote;
import io.github.htools.extract.modules.TokenizerRegex;
import io.github.htools.lib.ClassTools;
import io.github.htools.lib.Log;
import io.github.htools.search.ByteSearch;
import io.github.htools.search.ByteSearchPosition;
import io.github.htools.search.ByteSearchSection;
import io.github.htools.words.StopWordsMultiLang;
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
    HashSet<String> stopwords;
    RemoveFilteredWords stopwordRemover;
    StemTokens tokenStemmer = new StemTokens(null, "tokenize");

    public Tokenizer() {
        super();
        HashSet<String> sw = getUnstemmedStopwords();
        stopwords = new HashSet();
        for (String word : sw) {
            ArrayList<String> tokenize = tokenize(word);
            if (tokenize.size() == 1) {
                stopwords.add(tokenize.get(0));
            } else {
                for (String w : tokenize) {
                    if (w.length() > 0) {
                        stopwords.add(w);
                        log.info("partial stopword %s", w);
                    }
                }
            }
        }
//        log.info("stopwords %d %s", stopwords.size(), stopwords);
//            for (ExtractorProcessor p : this.preprocess) {
//                log.info("%s", p.getClass().getCanonicalName());
//            }
        if (stopwords.size() != 1745) {
            log.crash();
        }
        stopwordRemover = new RemoveFilteredWords(null, stopwords);
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
        return StopWordsMultiLang.getStemmedFilterSet();
    }

    protected HashSet<String> getUnstemmedStopwords() {
        return StopWordsMultiLang.getUnstemmedFilterSet();
    }

    @Override
    public Class getTokenMarker() {
        return TokenWordQuote.class;
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
        this.addPreProcessor(ConvertToLowercase.class);
        this.addPreProcessor(RemoveHtmlSpecialCodes.class);
        this.addPreProcessor(RemoveNonAlphanumericQuote.class);
        this.addPreProcessor(RemoveSingleQuotes.class);
        this.addPreProcessor(RemoveDanglingS.class);
        //this.addPreProcessor(new RemoveNonASCII(this, true));
    }

    @Override
    protected void buildProcess() {
        this.addProcess("tokenize", ConvertWhitespace.class);
    }

    public void removeStopWords() {
        addEndPipeline(getStopwordRemover());
    }
    
    public void removeTrecMetadata() {
        this.addSectionMarker(MarkTitle.class, "all", "titlesection");
        this.addSectionMarker(MarkHeadline.class, "all", "titlesection");
        this.addSectionMarker(MarkTTL.class, "all", "titlesection");
        this.addSectionMarker(MarkITAG56.class, "all", "titlesection");
        this.addSectionMarker(MarkHL.class, "all", "titlesection");
        this.addSectionMarker(MarkHead.class, "all", "titlesection");
        this.addSectionMarker(MarkText.class, "all", "text");
        this.insertPipeline(RemoveTrecMetadata.class, TokenizerRegex.class);
        this.addPreProcessor(RemoveHtmlComment.class);
        this.addPreProcessor(RemoveHtmlTags.class);
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
    
    public static class RemoveTrecMetadata extends ExtractorProcessor {

        public static Log log = new Log(RemoveSection.class);
        ByteSearch text2 = ByteSearch.create("\\[text\\]");

        public RemoveTrecMetadata(Extractor extractor, String process) {
            super(extractor, process);
        }

        @Override
        public void process(Content entity, ByteSearchSection section, String attribute) {
            ArrayList<ByteSearchSection> textSection = entity.getSectionPos("text");
            if (textSection.size() > 0) {
                ByteSearchSection textPos = textSection.get(0);
                ByteSearchPosition text2Pos = text2.findPos(textPos);
                if (text2Pos.found()) {
                    textPos = new ByteSearchSection(textPos.haystack, textPos.start, text2Pos.end,
                    textPos.innerend, textPos.end);
                }
                ArrayList<ByteSearchSection> sectionPos = entity.getSectionPos("titlesection");
                if (sectionPos.size() > 0 && sectionPos.get(0).innerend < textPos.innerstart) {
                    ByteSearchSection tsection = sectionPos.get(0);
                    for (int i = section.start; i < tsection.innerstart; i++) {
                        section.haystack[i] = 0;
                    }
                    for (int i = tsection.innerend; i < textPos.innerstart; i++) {
                        section.haystack[i] = 32;
                    }
                    for (int i = textPos.innerend; i < section.end; i++) {
                        section.haystack[i] = 0;
                    }
                } else {
                    for (int i = section.start; i < textPos.innerstart; i++) {
                        section.haystack[i] = 0;
                    }
                    for (int i = textPos.innerend; i < section.end; i++) {
                        section.haystack[i] = 0;
                    }
                }
            }
        }
    }    
}
