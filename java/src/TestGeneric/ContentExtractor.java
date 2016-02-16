package TestGeneric;

import io.github.htools.collection.SelectionInterval;
import io.github.htools.extract.Content;
import io.github.htools.extract.DefaultTokenizer;
import io.github.htools.extract.Extractor;
import io.github.htools.extract.modules.ExtractorProcessor;
import io.github.htools.extract.modules.RemoveSection;
import io.github.htools.lib.ArrayTools;
import io.github.htools.lib.ByteTools;
import io.github.htools.lib.ClassTools;
import io.github.htools.lib.Log;
import io.github.htools.search.ByteSearch;
import io.github.htools.search.ByteSearchPosition;
import io.github.htools.search.ByteSearchSection;
import io.github.htools.type.Pair;
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
public abstract class ContentExtractor extends Extractor {

    public static final Log log = new Log(DefaultTokenizer.class);
    public ByteSearch wordsplitter = getWordSplitter();
    static ContentExtractor singleton;
    protected HashSet<String> stopwords;

    public ContentExtractor() {
        super();
        build();
//        log.info("stopwords %d %s", stopwords.size(), stopwords);
//            for (ExtractorProcessor p : this.preprocess) {
//                log.info("%s", p.getClass().getCanonicalName());
//            }
    }

    protected abstract void build();
    
    public ByteSearch getWordSplitter() {
        return ByteSearch.create("\\w+");
    }
    
    public byte[] extractContent(byte[] content) {
        Content process = process(content);
        return ByteTools.toFullTrimmed(process.getContent());
    }

    public byte[] extractContent(String content) {
        Content process = process(content);
        return ByteTools.toFullTrimmed(process.getContent());
    }

    public ArrayList<String> getTokens(byte[] content) {
        return wordsplitter.extractAll(content);
    }

    public ArrayList<ByteSearchPosition> getTokenPositions(Document doc) {
        byte[] copy = ArrayTools.clone(doc.getOriginalContent());
        Content process = process(copy);
        return wordsplitter.findAllPos(copy);
    }

    public static ContentExtractor get(Class<? extends ContentExtractor> clazz) {
        if (singleton == null) {
            try {
                Constructor<ContentExtractor> cons
                        = ClassTools.getAssignableConstructor(clazz, ContentExtractor.class);
                singleton = ClassTools.construct(cons);
            } catch (ClassNotFoundException ex) {
                log.fatalexception(ex, "getTokenzizer( %s )", clazz.getCanonicalName());
            }
        }
        return singleton;
    }

    protected HashSet<String> getStopwords() {
        if (stopwords == null) {
            HashSet<String> sw = StopWordsMultiLang.getUnstemmedFilterSet();
            stopwords = new HashSet();
            for (String word : sw) {
                Content process = process(word);
                ArrayList<String> words = this.getTokens(process.getContent());
                for (String tw : words) {
                    if (tw.length() > 0) {
                        stopwords.add(tw);
                    }
                }
            }
            log.info("stopwords %s", stopwords);
        }
        return stopwords;
    }

    /**
     * @param term
     * @return true if the term is in the stop word list
     */
    public boolean isStopword(String term) {
        return getStopwords().contains(term);
    }

    public static class RemoveTrecMetadata extends ExtractorProcessor {

        public static Log log = new Log(RemoveSection.class);
        ByteSearch text2 = ByteSearch.create("\\[text\\]");

        public RemoveTrecMetadata(Extractor extractor, String process) {
            super(extractor, process);
        }

        @Override
        public void process(Content entity, ByteSearchSection section, String attribute) {
            log.info("process text section %s", section.toString());
            SelectionInterval<Integer> selected = new SelectionInterval();
            if (entity.getSectionPos("text2").size() > 0) {
                for (ByteSearchSection s : entity.getSectionPos("text2")) {
                    selected.addInterval(s.innerstart, s.innerend);
                }
            } else {
                for (ByteSearchSection s : entity.getSectionPos("text")) {
                    ByteSearchPosition text2Pos = text2.findPos(s);
                    if (text2Pos.found()) {
                        s = new ByteSearchSection(s.haystack, s.start, text2Pos.end,
                                s.innerend, s.end);
                    }
                    selected.addInterval(s.innerstart, s.innerend);
                }
            }
            for (ByteSearchSection s : entity.getSectionPos("titlesection")) {
                selected.addInterval(s.innerstart, s.innerend);
            }
            for (Pair<Integer, Integer> interval : selected.missed(section.start, section.end)) {
                log.info("remove %d %d %s", interval.getKey(), interval.getValue(),
                        ByteTools.toString(section.haystack, interval.getKey(), interval.getValue()));
                if (interval.getKey() < interval.getValue());
                section.haystack[interval.getKey()] = 32;
                for (int p = interval.getKey() + 1; p < interval.getValue(); p++) {
                    section.haystack[p] = 0;
                }
            }
        }
    }

    public static class RemoveNYTMetadata extends ExtractorProcessor {

        public static Log log = new Log(RemoveSection.class);

        public RemoveNYTMetadata(Extractor extractor, String process) {
            super(extractor, process);
        }

        @Override
        public void process(Content entity, ByteSearchSection section, String attribute) {
            ArrayList<ByteSearchSection> textSection = entity.getSectionPos("text");
            //log.info("process textsection %s", textSection);
            if (textSection.size() > 0) {
                ByteSearchSection textPos = textSection.get(0);
                ArrayList<ByteSearchSection> titleSection = entity.getSectionPos("titlesection");
                //log.info("process titlesection %s", titleSection);
                if (titleSection.size() > 0 && titleSection.get(0).innerend < textPos.innerstart) {
                    ByteSearchSection tsection = titleSection.get(0);
                    //log.info("text remove %d %d %d %d %d %d", section.start, tsection.innerstart, tsection.innerend, textPos.innerstart, textPos.innerend, section.end);
                    for (int i = section.start; i < tsection.innerstart; i++) {
                        section.haystack[i] = 0;
                    }
                    for (int i = tsection.innerend; i < textPos.innerstart; i++) {
                        section.haystack[i] = 32;
                    }
                    for (int i = textPos.innerend; i < section.end; i++) {
                        section.haystack[i] = 0;
                    }
                    //log.info("%s", ByteTools.toString(section.haystack, textPos.innerstart, textPos.innerend));
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
