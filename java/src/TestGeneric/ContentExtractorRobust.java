package TestGeneric;

import io.github.htools.extract.Content;
import io.github.htools.extract.DefaultTokenizer;
import io.github.htools.extract.modules.ConvertDotsAbbreviations;
import io.github.htools.extract.modules.ConvertHtmlASCIICodes;
import io.github.htools.extract.modules.ConvertHtmlSpecialCodes;
import io.github.htools.extract.modules.ConvertUnicodeDiacritics;
import io.github.htools.extract.modules.ConvertWhitespace;
import io.github.htools.extract.modules.MarkFRText;
import io.github.htools.extract.modules.MarkFRTitle;
import io.github.htools.extract.modules.MarkGraphic;
import io.github.htools.extract.modules.MarkH3;
import io.github.htools.extract.modules.MarkHL;
import io.github.htools.extract.modules.MarkHeadline;
import io.github.htools.extract.modules.MarkText;
import io.github.htools.extract.modules.MarkTitle;
import io.github.htools.extract.modules.RemoveDanglingS;
import io.github.htools.extract.modules.RemoveHtmlComment;
import io.github.htools.extract.modules.RemoveHtmlSpecialCodes;
import io.github.htools.extract.modules.RemoveHtmlTags;
import io.github.htools.extract.modules.RemoveQuote;
import io.github.htools.extract.modules.RemoveSingleQuotes;
import io.github.htools.lib.Log;
import io.github.htools.words.StopWordsContractions;
import io.github.htools.words.StopWordsMultiLang;
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
public class ContentExtractorRobust extends ContentExtractor {

    public static Log log = new Log(ContentExtractorRobust.class);

    public ContentExtractorRobust() {
        super();
    }

    public ArrayList<String> getTokens(byte[] content) {
        ArrayList<String> extractedTerms = wordsplitter.extractAll(content);
        ArrayList<String> terms = new ArrayList(extractedTerms.size());
        for (String term : extractedTerms) {
            term = term.toLowerCase();
            term = englishStemmer.get().stem(term);
            terms.add(term);
        }
        return terms;
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
    
    protected void build() {
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
        this.addSectionMarker(MarkHeadline.class, "all", "titlesection");
        this.addSectionMarker(MarkH3.class, "all", "titlesection");
        this.addSectionMarker(MarkFRTitle.class, "all", "titlesection");
        this.addSectionMarker(MarkHL.class, "all", "titlesection");
        this.addSectionMarker(MarkText.class, "all", "text");
        this.addSectionMarker(MarkGraphic.class, "all", "text");
        this.addSectionMarker(MarkFRText.class, "all", "text2");
        this.addSectionProcess("all", "extract", "result");
        this.addProcess("extract", RemoveHtmlComment.class);
        this.addProcess("extract", RemoveTrecMetadata.class);
        this.addProcess("extract", RemoveHtmlTags.class);
    }
    
}
