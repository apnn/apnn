package TestGeneric;

import io.github.htools.extract.Content;
import io.github.htools.extract.modules.*;
import io.github.htools.lib.ByteTools;
import io.github.htools.lib.Log;
import io.github.htools.words.StopWordsContractions;
import io.github.htools.words.StopWordsSmart;
import io.github.htools.words.englishStemmer;

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
 * This version of the tokenizer also removes stopwords in the
 * StopWordsMultiLang set.
 *
 * @author Jeroen
 */
public class ContentExtractorNYT extends ContentExtractor {

    public static Log log = new Log(ContentExtractorNYT.class);
    public Content lastDocument;

    public ContentExtractorNYT() {
        super();
    }
    
    @Override
    public byte[] extractContent(byte[] content) {
        Content process = process(content);
        lastDocument = process;
        return ByteTools.toFullTrimmed(process.getContent());
    }
    
    protected HashSet<String> getStopwords() {
        if (stopwords == null) {
            HashSet<String> sw = StopWordsSmart.getUnstemmedFilterSet();
            sw.addAll(StopWordsContractions.getUnstemmedFilterSet());
            stopwords = new HashSet();
            for (String word : sw) {
                Content process = process(word);
                ArrayList<String> words = this.getTokens(process.getContent());
                for (String tw : words) {
                    if (tw.length() > 0) {
                        tw = englishStemmer.get().stem(tw);
                        stopwords.add(tw);
                    }
                }
            }
        }
        return stopwords;
    }
    
    public void build() {
        this.addPreProcessor(ConvertHtmlASCIICodes.class);
        this.addPreProcessor(ConvertHtmlSpecialCodes.class);
        this.addPreProcessor(ConvertUnicodeDiacritics.class);
        this.addPreProcessor(ConvertDotsAbbreviations.class);
        //this.addPreProcessor(ConvertToLowercase.class);
        this.addPreProcessor(RemoveHtmlSpecialCodes.class);
        this.addPreProcessor(RemoveDanglingS.class);
        this.addPreProcessor(RemoveQuote.class);
        this.addPreProcessor(ConvertWhitespace.class);
        //this.addPreProcessor(new RemoveNonASCII(this, true));
        this.addSectionMarker(MarkTitle.class, "all", "titlesection");
        this.addSectionMarker(MarkNYTText.class, "all", "text");
        this.addSectionMarker(MarkDescriptor.class, "all", "descriptor");
        this.addSectionProcess("descriptor", "storelabel", "label");
        this.addProcess("storelabel", StoreLiteralSection.class);
        this.addSectionProcess("all", "extract", "result");
        this.addProcess("extract", RemoveNYTMetadata.class);
        this.addPreProcessor(RemoveHtmlComment.class);
        this.addProcess("extract", RemoveHtmlTags.class);
    }    
}
